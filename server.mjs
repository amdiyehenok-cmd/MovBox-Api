// movbox-stream — pure-HTTP stream + caption + MP4 + search proxy.
// No Chrome, no Puppeteer. ~50MB RSS.
//
// Endpoints (port 4000):
//   GET  /                          health + service info
//   GET  /health                     readiness probe
//   GET  /stream?detailPath=&season=&episode=&subjectId=
//                                  MP4 sources + captions (JSON)
//   GET  /mp4?url=<encoded>         stream the MP4 binary with browser
//                                  headers so the device can download it
//   POST /search                    proxy to h5-api /subject/search with
//                                  proper auth so the device can search
//                                  the WHOLE boxmovies database
//   GET  /subtitle?url=<encoded>    serve the .srt/.vtt caption with
//                                  the right browser headers
//
// Auth: the upstream h5-api requires a Bearer token from boxmovies.org's
// mb_token cookie. The device can't get that, so this relay injects it
// when proxying to the upstream. The token is captured once from a real
// browser session and embedded here. When it expires, the device will
// get a 401 — easy to detect and rotate.

import http from 'node:http';
import https from 'node:https';
import { URL } from 'node:url';

const H5 = 'https://h5-api.aoneroom.com/wefeed-h5api-bff';
const HOST = 'https://boxmovies.org';
const PORT = Number(process.env.PORT || 4000);
const HOSTNAME = process.env.HOST || '0.0.0.0';
const PUBLIC_HOST = process.env.PUBLIC_HOST || '';

// ── Upstream connection pool ───────────────────────────────────────────
// Every media byte comes from one CDN host.
//
// keepAlive is deliberately OFF. With a pool on, a socket the CDN has
// already closed can be handed back out, and the request then hangs
// forever — no response, no 'error' event, nothing to time out on. That
// is exactly what happened in production: /health and /stream stayed
// healthy while every /mp4 request sat there with zero bytes buffered,
// which presented to the user as a player stuck on "Buffering…" forever.
//
// Reuse only saved a ~200ms TLS handshake. The latency that actually
// matters is the 3MB moov, and that is solved by the head cache below.
// Reliability wins that trade comfortably.
const upstreamAgent = new https.Agent({
  keepAlive: false,
  maxSockets: 24,
});

/**
 * How long to wait for the upstream to send RESPONSE HEADERS before
 * giving up. Scoped to the header phase only — it is cleared the moment
 * headers arrive, so a 250MB body can stream for as long as it needs.
 *
 * Without this, "upstream is silent" is indistinguishable from "upstream
 * is slow", and the client waits forever.
 */
const UPSTREAM_HEADER_TIMEOUT_MS = 15000;

// ── Generic byte-budgeted LRU ──────────────────────────────────────────
// Bounded by bytes rather than entry count so one big buffer can't blow
// the container's memory budget. Deleting an entry calls its onEvict
// hook so the caller can keep a running byte total.
class ByteLRU {
  constructor(maxBytes, onEvict) {
    this.maxBytes = maxBytes;
    this.bytes = 0;
    this.onEvict = onEvict || null;
    this.m = new Map();   // key → { size, value }; Map preserves insertion order
  }
  get(k) {
    const e = this.m.get(k);
    if (!e) return null;
    // Refresh recency: re-insert moves the key to the end.
    this.m.delete(k);
    this.m.set(k, e);
    return e.value;
  }
  has(k) { return this.m.has(k); }
  set(k, value, size) {
    const prev = this.m.get(k);
    if (prev) { this.bytes -= prev.size; this.m.delete(k); }
    this.m.set(k, { size, value });
    this.bytes += size;
    while (this.bytes > this.maxBytes && this.m.size > 0) {
      const oldest = this.m.keys().next().value;
      const e = this.m.get(oldest);
      this.m.delete(oldest);
      this.bytes -= e.size;
      if (this.onEvict) this.onEvict(oldest, e.value, e.size);
    }
  }
  get size() { return this.m.size; }
  get totalBytes() { return this.bytes; }
}
// Metadata cache budget. Subtitle bodies are the only large entries here
// (tens of KB); /stream and /search payloads are a few KB. 32MB is far
// more than this keyspace needs and keeps the container lean.
//
// The old implementation was a FIFO keyed on insertion order with a hard
// 500-entry cap, so a burst of /search queries evicted the /stream
// metadata we cared about most — /health reported cache_size pinned at
// exactly 500/500, which is the tell that eviction was thrashing and
// every "cache hit rate" was really a miss.
const CACHE_BUDGET_BYTES = 32 * 1024 * 1024;

// key → { ts, ttl, data }. Recency is handled by ByteLRU (get()
// re-inserts), so a hot /stream entry survives a flood of searches.
const cache = new ByteLRU(CACHE_BUDGET_BYTES);
function cacheDelete(k) {
  const e = cache.m.get(k);
  if (!e) return;
  cache.m.delete(k);
}
function cacheGet(k) {
  const e = cache.get(k);
  if (!e) return null;
  if (Date.now() - e.ts > e.ttl) { cacheDelete(k); return null; }
  return e.data;
}
function cacheSet(k, d, ttl) {
  const size = typeof d === 'string'
    ? d.length
    : (d && d.buf && d.buf.length) || JSON.stringify(d ?? null).length * 2;
  cache.set(k, { ts: Date.now(), ttl, data: d }, size);
}

// ── Media head cache ───────────────────────────────────────────────────
// A progressive MP4 cannot decode a single frame until the browser has
// downloaded the whole `moov` atom. Ours are ~3MB. Measured end to end
// that moov download is the single largest contributor to time-to-first-
// frame, and because every viewer of the same title asks for the same
// byte range first, it is trivially cacheable.
//
// Key is the upstream PATH, not the full URL: the CDN rotates `sign` and
// `t` on every request but the bytes behind a path never change.
const HEAD_CACHE_BYTES = 4 * 1024 * 1024;      // covers our ~3MB moov
const HEAD_CACHE_BUDGET = 128 * 1024 * 1024;   // ~32 titles resident
const headCache = new ByteLRU(HEAD_CACHE_BUDGET);

function headCacheKey(upstreamUrl) {
  try { return new URL(upstreamUrl).pathname; } catch { return upstreamUrl; }
}
// Per-endpoint TTLs. /stream metadata changes rarely (only when boxmovies
// rotates their CDN URLs or a source goes down), /search results barely
// change at all in a 1hr window, and /subtitle bodies are immutable once
// published. Long TTLs = the relay answers from memory and the upstream
// h5-api never sees the request. This is the cheapest, cleanest way to
// keep the upstream cost at zero for repeats.
const CACHE_TTL = {
  stream:   30 * 60 * 1000,   // 30 min — movie play metadata + captions
  search:   60 * 60 * 1000,   // 1 hr — search results
  subtitle: 60 * 60 * 1000,   // 1 hr — srt/vtt bytes (immutable)
};
// Compose the cache key for a /stream call from the parts that uniquely
// identify a play: detailPath + subjectId + season + episode. We use the
// same shape as the old single-TTL cache so existing entries don't get
// duplicated after a redeploy.
function keyOf(p) {
  return [p.detailPath, p.subjectId, p.season || 0, p.episode || 0].join('|');
}

// (Per-IP rate limiter removed — the upstream boxmovies.org free quota
// (3 plays/day per anonymous session) is the real constraint; capping
// per-IP traffic on the relay just hides issues without adding value
// at the scale we're at.)
function clientIp(req) {
  // DO / Render / etc. forward the original client IP in x-forwarded-for.
  // Take the leftmost entry (the real client, per RFC).
  const xff = req.headers['x-forwarded-for'];
  if (typeof xff === 'string' && xff) return xff.split(',')[0].trim();
  return req.socket?.remoteAddress || 'unknown';
}

// ── Per-IP Bearer-token pool ────────────────────────────────────────────
// boxmovies.org hands out anonymous sessions with a per-uid daily free
// quota (999 free plays per fresh session). With one shared token, the
// first user to burn their quota locks out everyone else. So the relay
// keeps a Map<ip, { token, mintedAt, lastUsed, uid }> — every device-IP
// gets its own fresh mint, and each device has its own 999-play bucket.
//
// Memory: 300-byte JWT per IP × N devices. We cap the pool with a TTL
// (24h idle) and periodic LRU-style eviction. For 10k devices that's
// ~3MB total — well under the 512MB App Platform free tier.
//
// Minting: GET /wefeed-h5api-bff/app/get-latest-app-pkgs?app_name=moviebox
//   → response sets a `token` cookie that IS the JWT we send as Bearer.

const TOKEN_TTL_MS = 24 * 60 * 60 * 1000;   // 24h idle → evict
const tokenStore = new Map();   // ip → { token, uid, mintedAt, lastUsed }
let tokenMintedTotal = 0;

async function mintOne() {
  const url = 'https://h5-api.aoneroom.com/wefeed-h5api-bff/app/get-latest-app-pkgs?app_name=moviebox';
  const r = await fetch(url, {
    headers: {
      Origin: 'https://boxmovies.org',
      Referer: 'https://boxmovies.org/',
      'User-Agent': 'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36',
      'Accept': 'application/json, text/plain, */*',
    },
    redirect: 'follow',
  });
  if (!r.ok) throw new Error(`mint failed: ${r.status}`);
  const setCookie = r.headers.get('set-cookie') || '';
  const m = setCookie.match(/(?:^|,\s*)token=([^;]+)/i);
  if (!m) throw new Error('no token cookie in mint response');
  const newJwt = m[1];
  let uid = 0;
  try {
    const parts = newJwt.split('.');
    if (parts.length === 3) {
      const payload = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));
      uid = payload.uid || 0;
    }
  } catch (_) { /* ignore */ }
  return { token: newJwt, uid };
}

async function getOrMintForIp(ip) {
  const now = Date.now();
  const e = tokenStore.get(ip);
  if (e && now - e.lastUsed < TOKEN_TTL_MS) {
    e.lastUsed = now;
    return e.token;
  }
  const { token, uid } = await mintOne();
  tokenStore.set(ip, { token, uid, mintedAt: now, lastUsed: now });
  tokenMintedTotal += 1;
  console.log(`[bearer] minted for ip=${ip} uid=${uid} (total mints: ${tokenMintedTotal}, pool size: ${tokenStore.size})`);
  return token;
}

async function refreshForIp(ip) {
  const { token, uid } = await mintOne();
  tokenStore.set(ip, { token, uid, mintedAt: Date.now(), lastUsed: Date.now() });
  tokenMintedTotal += 1;
  console.log(`[bearer] refreshed for ip=${ip} uid=${uid} (total mints: ${tokenMintedTotal})`);
  return token;
}

// Periodic cleanup: drop entries idle for >24h so the pool doesn't grow
// without bound across days of operation.
setInterval(() => {
  const now = Date.now();
  let evicted = 0;
  for (const [ip, e] of tokenStore.entries()) {
    if (now - e.lastUsed > TOKEN_TTL_MS) { tokenStore.delete(ip); evicted += 1; }
  }
  if (evicted) console.log(`[bearer] evicted ${evicted} idle tokens, pool size now ${tokenStore.size}`);
}, 60 * 60 * 1000).unref();

// ── Per-IP daily-play cap + ad reward ───────────────────────────────────
// Soft cap we impose on top of boxmovies' quota. Each IP gets N free
// /stream plays per 24h before the response flips to `adRequired: true`.
// The app then shows a rewarded ad and calls POST /reward, which
// resets the counter and mints a fresh upstream token in one shot.
//
// Tunable via env: FREE_PLAYS_PER_DAY (default 999 = effectively
// disabled). The cap exists to force users to watch a rewarded ad
// every N plays. While Start.io's ad inventory is still ramping
// up for this app, we set the default to 999 so the cap never
// blocks the user. Once Start.io has real inventory and we want
// to push monetization, set this to 50 in the env.

const FREE_PLAYS_PER_DAY = Number(process.env.FREE_PLAYS_PER_DAY || 999);
const REWARD_COOLDOWN_MS = 60 * 1000;   // 1 reward per IP per minute max
const ipPlays = new Map();   // ip → { count, dayStart }
const ipLastReward = new Map();  // ip → timestamp

function getOrCreatePlayRecord(ip) {
  const now = Date.now();
  let e = ipPlays.get(ip);
  if (!e || now - e.dayStart > 24 * 60 * 60 * 1000) {
    e = { count: 0, dayStart: now };
    ipPlays.set(ip, e);
  }
  return e;
}

const USER_AGENT='Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36';

// Headers to use when proxying to the upstream h5-api. The Bearer comes
// from the per-IP token pool (see getOrMintForIp). These are the headers
// the boxmovies.org site itself sends, so the upstream treats the
// request as if it's coming from the live website.
function upstreamHeaders(token, extra = {}) {
  return {
    Origin: HOST,
    Referer: `${HOST}/`,
    'User-Agent': USER_AGENT,
    'x-client-info': '{"timezone":"Africa/Nairobi"}',
    'x-request-lang': 'en',
    'Accept': 'application/json, text/plain, */*',
    Authorization: `Bearer ${token}`,
    ...extra,
  };
}

async function fetchPlay(req, d, p, se, ep) {
  const ip = clientIp(req);
  const url = `${H5}/subject/play?subjectId=${p}&se=${se||0}&ep=${ep||0}&detailPath=${d}`;
  const referer = `${HOST}/movies/${d}?id=${p}&type=/tv/detail&se=${se||0}&ep=${ep||0}&lang=en`;
  let token = await getOrMintForIp(ip);
  let r = await fetch(url, { headers: upstreamHeaders(token, { Referer: referer }) });
  // 401/403 → this IP's token is dead. Mint a fresh one and retry once.
  if (r.status === 401 || r.status === 403) {
    console.log(`[bearer] ip=${ip} got ${r.status}, refreshing token...`);
    try { token = await refreshForIp(ip); } catch (_) { /* fall through */ }
    r = await fetch(url, { headers: upstreamHeaders(token, { Referer: referer }) });
  }
  if (!r.ok) throw new Error(`h5-api ${r.status}`);
  return await r.json();
}
async function fetchCaption(req, fmt, id, sid, dp) {
  const ip = clientIp(req);
  const token = await getOrMintForIp(ip);
  const url = `${H5}/subject/caption?format=${fmt}&id=${id}&subjectId=${sid}&detailPath=${dp}`;
  const r = await fetch(url, { headers: upstreamHeaders(token, { Referer: `${HOST}/movies/${dp}` }) });
  if (!r.ok) return null;
  try { return await r.json(); } catch { return null; }
}
function pickBestStream(s){let b=null;for(const x of s||[]){if(!x?.url||!x?.id)continue;const r=Number(x.resolutions)||x.resolution||0;if(!b||r>(b._res||0))b={...x,_res:r};}return b;}

// Build the absolute base URL the device will use to call back into the
// relay for /mp4 and /subtitle. PUBLIC_HOST is preferred (set it in
// production to your real public URL, e.g. https://movbox-api.onrender.com)
// so device-facing URLs are stable even behind a proxy that rewrites the
// host header. When PUBLIC_HOST is empty, fall back to the protocol
// DO/Render/etc. forward in `x-forwarded-proto` and the Host header.
// This matters: if we hardcoded http:// but the platform only serves
// https://, every /mp4 and /subtitle request would 301 to https and
// ExoPlayer would throw "Source error" before even reaching the stream.
function baseUrl(req) {
  if (PUBLIC_HOST) return PUBLIC_HOST.replace(/\/+$/, '');
  const proto = (req.headers['x-forwarded-proto'] || 'https').toString();
  return `${proto}://${req.headers.host}`;
}
function cleanCaption(c, base){
  const proxied=c.url?`${base}/subtitle?url=${encodeURIComponent(c.url)}`:c.url;
  const ext=c.url?c.url.split('?')[0].split('.').pop()?.toLowerCase():'';
  const mime=ext==='vtt'?'text/vtt':ext==='srt'?'application/x-subrip':'application/x-subrip';
  return {id:c.id,url:proxied,mimeType:mime,languageCode:c.lan||'en',language:c.lanName||c.lan||'Unknown',delay:c.delay||0};
}
async function handleSubtitle(req,res,url){
  // Cache the subtitle bytes — they never change. Same URL across all
  // users = 1 upstream fetch per hour, no matter how many people are
  // watching.
  const cached = cacheGet(`sub:${url}`);
  if (cached) {
    res.writeHead(200, {'content-type': cached.ct, 'content-length': String(cached.buf.length), 'access-control-allow-origin':'*', 'x-cache':'hit'});
    res.end(cached.buf); return;
  }
  const upstream=await fetch(url,{headers:upstreamHeaders(),redirect:'follow'});
  if(!upstream.ok){res.writeHead(upstream.status,{'content-type':'application/json'});res.end(JSON.stringify({ok:false,error:`upstream ${upstream.status}`}));return;}
  const ct=upstream.headers.get('content-type')||'application/x-subrip';
  const buf=Buffer.from(await upstream.arrayBuffer());
  cacheSet(`sub:${url}`, { ct, buf }, CACHE_TTL.subtitle);
  res.writeHead(200,{'content-type':ct,'content-length':String(buf.length),'access-control-allow-origin':'*','x-cache':'miss'});
  res.end(buf);
}

async function handleStream(req,res,p){
  if(!p.detailPath||!p.subjectId){res.writeHead(400,{'content-type':'application/json'});res.end(JSON.stringify({ok:false,error:'detailPath and subjectId required'}));return;}
  const ip = clientIp(req);
  // Soft per-IP daily-play cap. When the IP is over the cap, we tell the
  // app "ad required" and let it show a rewarded ad. After the ad, the
  // app calls POST /reward which resets the counter + mints a fresh
  // upstream token. This is the monetization layer: every time a user
  // burns through their free plays, they see an ad to keep going.
  const rec = getOrCreatePlayRecord(ip);
  if (rec.count >= FREE_PLAYS_PER_DAY) {
    res.writeHead(200, {'content-type':'application/json'});
    res.end(JSON.stringify({
      ok: false,
      adRequired: true,
      reason: 'daily_play_cap',
      playsToday: rec.count,
      cap: FREE_PLAYS_PER_DAY,
      message: 'Watch a short ad to continue.',
    }));
    return;
  }
  rec.count += 1;
  // Build the base URL for proxied /mp4 and /subtitle entries. Must come
  // BEFORE the cleanCaption call below since captions use it too.
  const base = baseUrl(req);
  const k=keyOf(p);let cached=cacheGet(k);
  if(cached){res.writeHead(200,{'content-type':'application/json','x-cache':'hit'});res.end(JSON.stringify(cached));return;}
  let play=await fetchPlay(req, p.detailPath, p.subjectId, p.season||0, p.episode||0);
  let data=(play&&play.data)||{};
  let streams=data.streams||[];
  // If the upstream's quota is on its last legs and the IP's per-token
  // refresh didn't help, fall through with empty streams. The ad path
  // above (next call from the same IP) will catch it.
  let captions=[];
  const best=pickBestStream(streams);
  if(best?.id){const cap=await fetchCaption(req, 'MP4', best.id, p.subjectId, p.detailPath);const capArr=cap?.data?.captions||[];captions=capArr.map(c=>cleanCaption(c, base)).filter(c=>c.url);}
  // Build a proxied MP4 URL for every stream so the device can download
  // it via the relay. Without this, the device tries to hit the upstream
  // CDN directly and gets rate-limited.
  const proxiedStreams=streams.map(s=>{
    if(!s.url) return s;
    return {...s,url:`${base}/mp4?url=${encodeURIComponent(s.url)}`};
  });
  const out={
    ok: streams.length > 0,
    source: 'movbox-stream',
    code: play?.code ?? 0,
    message: play?.message ?? 'ok',
    playsRemaining: FREE_PLAYS_PER_DAY - rec.count,
    cap: FREE_PLAYS_PER_DAY,
    data: {
      streams: proxiedStreams,
      freeNum: data.freeNum ?? 0,
      limited: !!data.limited,
      hasResource: streams.length > 0,
      vipLocked: !!data.vipLocked,
      codecPriority: data.codecPriority || [],
      captions,
    },
  };
  // Only cache "happy" responses. If the upstream returned an empty
  // stream list (free daily quota burned, VIP-locked content, or a
  // missing subject), don't pin that to disk for 30 min — the user
  // would get a stale "no streams" until the cache expires, even after
  // the upstream limit resets. The next call re-checks the upstream
  // for free.
  const worthCaching = streams.length > 0 && !out.data.limited && !out.data.vipLocked;
  if (worthCaching) cacheSet(k, out, CACHE_TTL.stream);
  res.writeHead(200,{'content-type':'application/json','x-cache': worthCaching ? 'miss' : 'bypass'});
  res.end(JSON.stringify(out));
}

/**
 * POST /reward
 *
 * App calls this after the user finishes a rewarded ad. Resets the
 * per-IP daily-play counter AND mints a fresh upstream token in one
 * shot, so the next /stream call from this IP gets a fresh 999-play
 * bucket from boxmovies.
 *
 * Cooldown: 1 reward per IP per minute. Stops the user from spamming
 * the button to clear the counter faster than the ad takes to play.
 *
 * Body shape: none (the IP is taken from the request). Returns
 *   200 { ok: true, playsReset: true, cap: N }
 *   429 { ok: false, error: 'reward_cooldown', waitSeconds: N }
 */
async function handleReward(req, res) {
  if (req.method !== 'POST') {
    res.writeHead(405, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'POST required' }));
    return;
  }
  const ip = clientIp(req);
  const now = Date.now();
  const last = ipLastReward.get(ip) || 0;
  const wait = Math.ceil((REWARD_COOLDOWN_MS - (now - last)) / 1000);
  if (now - last < REWARD_COOLDOWN_MS) {
    res.writeHead(429, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'reward_cooldown', waitSeconds: Math.max(1, wait) }));
    return;
  }
  // Reset the per-IP counter so the user gets another full cap of plays.
  ipPlays.set(ip, { count: 0, dayStart: now });
  ipLastReward.set(ip, now);
  // Force a fresh upstream token so the next /stream hits a brand-new
  // boxmovies session (999 fresh plays, not the previous token's last
  // few).
  let refreshed = false;
  try {
    await refreshForIp(ip);
    refreshed = true;
  } catch (e) {
    console.log(`[reward] token refresh failed for ${ip}: ${e.message}`);
  }
  console.log(`[reward] ip=${ip} plays reset, token ${refreshed ? 'refreshed' : 'kept (refresh failed)'}`);
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ ok: true, playsReset: true, tokenRefreshed: refreshed, cap: FREE_PLAYS_PER_DAY }));
}

/**
 * /mp4?url=<encoded>
 *
 * Streams the upstream MP4 back to the client with the Referer/Origin
 * headers the upstream CDN insists on. The client can't send those on a
 * cross-origin <video> request, so it has to come through us.
 *
 * Two things make this fast:
 *
 * 1. Native streaming instead of undici. The previous implementation
 *    used `fetch()` + `body.getReader()` and awaited once per ~64KB
 *    chunk. undici's fetch body reader is a JS-level async generator
 *    with no C++ fast path, and measured end to end it capped the whole
 *    relay at 0.02-0.18 MB/s while the identical upstream request from
 *    the same machine ran at 1.27 MB/s — a ~64x loss that had nothing
 *    to do with the network. `up.pipe(res)` is the real fast path.
 *
 * 2. A head cache. Every viewer's first request is for the same leading
 *    bytes, which is where the ~3MB `moov` index lives. Serving that
 *    from memory turns the single largest contributor to
 *    time-to-first-frame into a memcpy.
 *
 * Range requests are passed through untouched so seeking and
 * DownloadManager resume keep working.
 */

// Parse a single-range "bytes=start-end" header. Returns null for
// multi-range or malformed input, which we treat as "no usable range".
function parseRange(hdr) {
  if (!hdr) return null;
  const m = /^bytes=(\d*)-(\d*)$/.exec(String(hdr).trim());
  if (!m) return null;
  const [, a, b] = m;
  if (a === '' && b === '') return null;
  return { start: a === '' ? 0 : Number(a), end: b === '' ? null : Number(b) };
}

// Pull the total file size out of a Content-Range header.
function totalFromContentRange(cr) {
  if (!cr) return null;
  const m = /\/(\d+)\s*$/.exec(String(cr));
  return m ? Number(m[1]) : null;
}

// Collect an entire upstream response into a Buffer, with a hard cap so
// a malformed/huge response can never exhaust the container.
function readAll(res, limitBytes) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let got = 0;
    res.on('data', (c) => {
      got += c.length;
      if (got > limitBytes) { res.destroy(); reject(new Error('head too large')); return; }
      chunks.push(c);
    });
    res.on('end', () => resolve(Buffer.concat(chunks, got)));
    res.on('error', reject);
  });
}

async function handleMp4(req, res, url) {
  if (!url) {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'url required' }));
    return;
  }

  const hdrs = {
    Origin: HOST,
    Referer: `${HOST}/`,
    'User-Agent': USER_AGENT,
    'Accept-Encoding': 'identity',   // never let the CDN gzip media
  };
  const rangeHeader = req.headers['range'];
  if (rangeHeader) hdrs['Range'] = rangeHeader;

  const parsed = new URL(url);
  const mod = parsed.protocol === 'http:' ? http : https;
  const key = headCacheKey(url);

  // ── Head-cache fast path ───────────────────────────────────────────
  // Only for explicit bounded ranges starting at byte 0 — that is the
  // shape a <video> element issues while it fetches the index.
  const r = parseRange(rangeHeader);
  if (r && r.start === 0 && r.end !== null && r.end < HEAD_CACHE_BYTES) {
    const hit = headCache.get(key);
    if (hit && hit.buf.length >= r.end + 1) {
      const slice = hit.buf.subarray(0, r.end + 1);
      const cr = `bytes 0-${r.end}/${hit.total || hit.buf.length}`;
      res.writeHead(206, {
        'content-type': 'video/mp4',
        'content-length': String(slice.length),
        'content-range': cr,
        'accept-ranges': 'bytes',
        'access-control-allow-origin': '*',
        'x-cache': 'hit',
      });
      res.end(slice);
      return;
    }
  }

  let settled = false;
  let upReq = null;
  let retried = false;
  const onClientGone = () => {
    if (settled) return;
    settled = true;
    try { upReq && upReq.destroy(); } catch (_) {}
    try { res.end(); } catch (_) {}
  };

  const reqOptions = {
    protocol: parsed.protocol,
    hostname: parsed.hostname,
    port: parsed.port || (parsed.protocol === 'http:' ? 80 : 443),
    path: parsed.pathname + parsed.search,
    method: 'GET',
    headers: hdrs,
    agent: upstreamAgent,
    // The CDN must not hand us a redirect for media; a redirect here
    // would silently drop our Origin/Referer and 429 the follow-up.
    // undici's fetch(redirect:'follow') handled this; native does not.
    // We surface it as an error instead of guessing.
  };

  function failClient(e) {
    if (settled) return;
    settled = true;
    if (res.headersSent) { try { res.end(); } catch (_) {} return; }
    res.writeHead(502, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: e && e.message ? e.message : 'upstream failed' }));
  }

  function openUpstream() {
    upReq = mod.request(reqOptions, (up) => {
      // Headers are in — stop guarding and let the body stream for as
      // long as it needs. A 250MB file must never be cut off.
      try { upReq.setTimeout(0); } catch (_) {}

      if (up.statusCode >= 300 && up.statusCode < 400 && up.headers.location) {
        up.resume();
        failClient(new Error(`upstream redirect ${up.statusCode}`));
        return;
      }
      if (up.statusCode !== 206 && up.statusCode !== 200) {
        up.resume();
        failClient(new Error(`upstream ${up.statusCode}`));
        return;
      }

      const ct = up.headers['content-type'] || 'video/mp4';
      const total = totalFromContentRange(up.headers['content-range']);
      const baseHeaders = {
        'content-type': ct,
        'accept-ranges': up.headers['accept-ranges'] || 'bytes',
        'access-control-allow-origin': '*',
      };

      // ── Buffered head path ─────────────────────────────────────────
      // Buffer the whole leading range so we can both answer instantly
      // and leave it in the cache for the next viewer.
      if (r && r.start === 0 && r.end !== null && r.end < HEAD_CACHE_BYTES) {
        settled = true;
        (async () => {
          try {
            const buf = await readAll(up, HEAD_CACHE_BYTES);
            headCache.set(key, { buf, total }, buf.length);
            if (res.writableEnded) return;
            const h = { ...baseHeaders, 'content-length': String(buf.length), 'x-cache': 'miss' };
            if (up.headers['content-range']) h['content-range'] = up.headers['content-range'];
            res.writeHead(up.statusCode, h);
            res.end(buf);
          } catch (e) {
            try { if (!res.writableEnded) res.end(); } catch (_) {}
          }
        })();
        return;
      }

      // ── Stream path ────────────────────────────────────────────────
      const outHeaders = { ...baseHeaders, 'x-cache': 'miss' };
      if (up.headers['content-length']) outHeaders['content-length'] = up.headers['content-length'];
      if (up.headers['content-range']) outHeaders['content-range'] = up.headers['content-range'];
      settled = true;
      // Must write the status+headers BEFORE piping. pipe() on its own
      // would trigger an implicit 200 with no content-type / content-range
      // / accept-ranges, which breaks seeking.
      res.writeHead(up.statusCode, outHeaders);
      up.pipe(res);
      up.on('error', () => { try { res.end(); } catch (_) {} });
    });

    // ── Header-phase guard ────────────────────────────────────────────
    // The only timeout in the whole request. It is cleared the instant
    // response headers arrive, so it can only ever fire while we are
    // waiting for the upstream to say something. Without it, a socket
    // that never errors and never answers hangs the request forever —
    // which is exactly how the player ended up buffering indefinitely.
    upReq.setTimeout(UPSTREAM_HEADER_TIMEOUT_MS, () => {
      upReq.destroy(new Error(`upstream did not respond within ${UPSTREAM_HEADER_TIMEOUT_MS}ms`));
    });

    upReq.on('error', (e) => {
      if (settled) return;
      // One retry on a fresh socket. A reused keep-alive socket that the
      // CDN had already closed fails exactly like this: no response,
      // ECONNRESET or hangup. Retrying once on a clean socket is cheap;
      // looping would just mask a genuinely dead origin.
      if (!retried) {
        retried = true;
        console.warn(`[mp4] ${e.message} — retrying on a fresh socket (${parsed.hostname})`);
        openUpstream();
        return;
      }
      failClient(e);
    });

    upReq.end();
  }

  // The browser aborts range requests constantly while seeking. Tear the
  // upstream socket down with it so we aren't paying for bytes nobody
  // will read.
  res.on('close', onClientGone);
  req.on('aborted', onClientGone);

  openUpstream();
}

/**
 * POST /search
 *
 * Proxy to upstream h5-api /subject/search with the Bearer auth
 * header injected. Body shape (JSON):
 *   {
 *     "keyword":     "avatar",
 *     "page":        "1",         // upstream wants a STRING
 *     "perPage":     28,
 *     "subjectType": 0           // 0=all, 1=movie, 2=series, …
 *   }
 *
 * Response is the upstream envelope unchanged:
 *   { code, message, data: { pager, items: [...] } }
 *
 * Why a proxy: the upstream h5-api requires a Bearer token that
 * boxmovies.org mints from the `mb_token` cookie. The device can't
 * get that cookie without loading boxmovies.org in a WebView, which
 * is exactly the heavy lift the user wanted to avoid. This proxy
 * lets the device do a real search of the whole boxmovies database
 * (e.g. 107 results for "avatar") with one POST.
 */
async function handleSearch(req, res) {
  if (req.method !== 'POST') {
    res.writeHead(405, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'POST required' }));
    return;
  }
  // Read JSON body (upstream requires <200KB so one read is fine)
  let raw = '';
  for await (const chunk of req) {
    raw += chunk;
    if (raw.length > 200_000) {
      res.writeHead(413, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'body too large' }));
      return;
    }
  }
  let body;
  try {
    body = JSON.parse(raw);
  } catch (e) {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'invalid JSON body' }));
    return;
  }
  const keyword = String(body.keyword || '').trim();
  if (!keyword) {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'keyword required' }));
    return;
  }
  const page = String(body.page ?? '1');
  const perPage = Number(body.perPage ?? 28);
  const subjectType = Number(body.subjectType ?? 0);

  // Cache by query+page+type so the device doesn't hammer the upstream
  const cacheKey = `search:${keyword.toLowerCase()}:${page}:${perPage}:${subjectType}`;
  const cached = cacheGet(cacheKey);
  if (cached) {
    res.writeHead(200, { 'content-type': 'application/json', 'x-cache': 'hit' });
    res.end(JSON.stringify(cached));
    return;
  }

  // /search uses a per-IP token too, even though it's the relay's job
  // to mint one. Without Bearer, h5-api returns 400.
  const token = await getOrMintForIp(clientIp(req));
  // The upstream search endpoint takes a JSON body with these exact
  // field names. We pass through the user's keyword + pagination +
  // subjectType filter unchanged.
  const upstream = await fetch(`${H5}/subject/search`, {
    method: 'POST',
    headers: upstreamHeaders(token, { 'Content-Type': 'application/json' }),
    body: JSON.stringify({ keyword, page, perPage, subjectType }),
  });
  if (!upstream.ok) {
    res.writeHead(upstream.status, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: `upstream ${upstream.status}` }));
    return;
  }
  const data = await upstream.json();
  cacheSet(cacheKey, data, CACHE_TTL.search);
  res.writeHead(200, { 'content-type': 'application/json', 'x-cache': 'miss' });
  res.end(JSON.stringify(data));
}

// ── Server tuning ─────────────────────────────────────────────────────
// Node defaults are hostile to a long-lived media proxy:
//   requestTimeout  = 300s  -> a 247MB file at 1MB/s needs ~247s and
//                              gets killed mid-stream near the end.
//   headersTimeout  = 60s   -> fine, we don't accept slow clients.
//   keepAliveTimeout= 5s    -> our upstream socket pool wants more.
//   timeout         = 0     -> already unlimited, keep it.
// (Applied after createServer below — `server` is a const and this file
// is an ES module, so touching it earlier is a TDZ ReferenceError.)

const server=http.createServer(async(req,res)=>{
  try{
    const u=new URL(req.url,`http://${req.headers.host}`);
    if(u.pathname==='/health'){const m=process.memoryUsage();res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify({ok:true,ram_mb:Math.round(m.rss/1024/1024*10)/10,cache_size:cache.size,cache_mb:Math.round(cache.totalBytes/1048576*10)/10,head_cache_size:headCache.size,head_cache_mb:Math.round(headCache.totalBytes/1048576*10)/10,token_pool_size:tokenStore.size,ip_plays_tracked:ipPlays.size,free_plays_per_day:FREE_PLAYS_PER_DAY,uptime_s:Math.round(process.uptime())}));return;}
    if(u.pathname==='/stream'){const p={detailPath:u.searchParams.get('detailPath'),subjectId:u.searchParams.get('subjectId'),season:u.searchParams.get('season')||'0',episode:u.searchParams.get('episode')||'0'};await handleStream(req,res,p);return;}
    if(u.pathname==='/mp4'){await handleMp4(req, res, u.searchParams.get('url'));return;}
    if(u.pathname==='/search'){await handleSearch(req,res);return;}
    if(u.pathname==='/subtitle'){const url=u.searchParams.get('url');if(!url){res.writeHead(400,{'content-type':'application/json'});res.end(JSON.stringify({ok:false,error:'url required'}));return;}await handleSubtitle(req,res,url);return;}
    if(u.pathname==='/reward'){await handleReward(req,res);return;}
    res.writeHead(404,{'content-type':'application/json'});res.end(JSON.stringify({ok:false,error:'not found'}));
  }catch(e){res.writeHead(500,{'content-type':'application/json'});res.end(JSON.stringify({ok:false,error:e.message}));}
});
server.requestTimeout = 0;          // no wall-clock cap on a media stream
server.timeout = 0;
server.headersTimeout = 60_000;
server.keepAliveTimeout = 72_000;
server.on('connection', (s) => s.setNoDelay(true));
server.listen(PORT,HOSTNAME,()=>{console.log(`movbox-stream listening on http://${HOSTNAME}:${PORT}`);});
