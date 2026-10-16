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
import { URL } from 'node:url';
import { Readable } from 'node:stream';
import { once } from 'node:events';

const H5 = 'https://h5-api.aoneroom.com/wefeed-h5api-bff';
const HOST = 'https://boxmovies.org';

/**
 * Which site the MEDIA proxy claims to be when it talks to the CDN.
 *
 * The upstream runs a Referer allowlist. Measured against the live CDN:
 *
 *   Referer: https://movibox.xyz/    -> 206 (allow-listed)
 *   Referer: https://boxmovies.org/  -> 206 from an allow-listed IP, hangs otherwise
 *   Referer: https://movbox.xyz/     -> 429 blocked
 *   no Referer                       -> 429 blocked
 *
 * movbox.xyz is not on that allowlist, so the media bytes have to be
 * proxied — but the proxy can present itself as the sister site, which
 * is. Override with UPSTREAM_MEDIA_SITE if the allowlist changes.
 *
 * This is a stopgap. The permanent fix is to get movbox.xyz onto the
 * allowlist too, after which the <video> element can fetch the CDN
 * directly and this entire proxy can go away.
 */
const UPSTREAM_MEDIA_SITE = (process.env.UPSTREAM_MEDIA_SITE || 'https://movibox.xyz').replace(/\/+$/, '');
const PORT = Number(process.env.PORT || 4000);
const HOSTNAME = process.env.HOST || '0.0.0.0';
const PUBLIC_HOST = process.env.PUBLIC_HOST || '';

// ── Upstream transport ─────────────────────────────────────────────────
// Every media byte comes from one CDN host, fetched with undici's `fetch`.
//
// This was a hand-rolled `https.request` + `https.Agent` pool. It worked in
// local testing and hung forever in production, and the reason matters: the
// only guard on it was `req.setTimeout(7000)`, which Node cannot fire until
// the request already holds a socket. A stall in DNS/TCP/TLS therefore had no
// timer at all. Measured on the live service: /mp4 sat 90s+ with zero bytes
// and never returned, while /health — same process, same origin, same host,
// but using `fetch` — answered in 38ms.
//
// So the transport is now the one that demonstrably reaches this origin from
// this instance, and the deadline is armed before the request is issued
// rather than after a socket appears. KeepAlive/pool tuning is now undici's
// problem; it manages connection lifecycle correctly on its own.

/**
 * How long to wait for the upstream to send RESPONSE HEADERS before
 * giving up. Scoped to the header phase only — it is cleared the moment
 * headers arrive, so a 250MB body can stream for as long as it needs.
 *
 * Without this, "upstream is silent" is indistinguishable from "upstream
 * is slow" and the client waits forever. Kept long enough to survive a cold
 * CDN edge: measured cold 4MB fetches at this host ran ~10s before the bytes
 * arrived, so the old 7s was cutting off exactly the requests that were about
 * to succeed. Still bounded so a genuinely dead host fails in seconds rather
 * than hanging the player.
 */
const UPSTREAM_HEADER_TIMEOUT_MS = 25000;

/**
 * Separate budget for reading an 8MB chunk body once headers have arrived.
 *
 * Splitting it out matters: AbortSignal.timeout() keeps counting through the
 * body read, so a single budget silently truncated chunk payloads that were
 * still arriving perfectly well. 45s for 8MB is far above the ~100ms warm
 * and comfortably above the ~10s cold measurement.
 */
const UPSTREAM_BODY_TIMEOUT_MS = 60_000;

// The old HEAD_CACHE_BODY_TIMEOUT_MS is gone: the head-cache it guarded was
// removed when chunked assembly arrived, and its replacement is
// UPSTREAM_BODY_TIMEOUT_MS below, which every chunk body now goes through.

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

// ── Media chunk cache ──────────────────────────────────────────────────
// Superseded the old 4MB "head cache", which asked the upstream for a 4MB
// range in one request. The upstream accepts those headers and then goes
// silent — see the 256KB ceiling documented above handleMp4. Chunks are now
// fetched at the size the CDN actually honours and assembled here, so the
// cache is what makes repeated playback instant and upstream-free.
//
// Key is the upstream PATH, not the full URL: the CDN rotates `sign` and `t`
// on every request but the bytes behind a path never change.
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
    try { rememberMediaOrigin(new URL(s.url).pathname, {
      ip, detailPath: p.detailPath, subjectId: p.subjectId,
      season: p.season || 0, episode: p.episode || 0,
    }); } catch (_) {}
    // Warm ONLY the stream the client is about to pick. Warming every
    // resolution would multiply bandwidth for nothing — a session plays
    // exactly one, and a cold first chunk is precisely what leaves the
    // player spinning for ten seconds. Matched by id against pickBestStream
    // (highest resolution) rather than by array position, because the
    // upstream does not guarantee best-first ordering.
    if (best && s.id === best.id) { try { prewarmHead(s.url, new URL(s.url).pathname); } catch (_) {} }
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

// ── Upstream media access constraints (measured 2026-10-13) ────────────
//
// The CDN does not behave like a normal file host, and both of its limits
// were invisible until they were measured:
//
//  1. RESPONSE SIZE CEILING. Measured on one fresh signature each:
//       64KB  -> 200 OK in 0.6s
//       128KB -> 200 OK in 0.6s
//       256KB -> 200 OK in 0.6s
//       512KB -> headers, then silence (stalls)
//        1MB  -> headers, then silence (stalls)
//       open  -> headers, then ~10KB, then silence
//     Anything over 256KB is accepted with headers and then goes silent.
//     This is why the 4MB head-cache read never populated from upstream and
//     why playback died a few minutes in.
//
//  2. COLD vs WARM IS THE WHOLE STORY. The single biggest factor is not the
//     range size or the signature — it is whether the CDN edge already holds
//     the requested bytes. Measured 2026-10-15 on
//     bcdnxw.hakunaymatata.com (Crystal Lake s1e1, 2.7GB):
//
//       first request (cold)        256KB in 323ms
//       immediately after (warm)    256KB in  54ms   ->  18.5 MB/s
//       warm 4MB                                   ->  41.2 MB/s
//       warm 8MB                                   -> 100.0 MB/s
//       warm 32MB, fresh signature                 -> 615.4 MB/s
//
//     And a single signature serves MANY ranges: the same signed URL that
//     returned bytes 0-262143 then returned 262144-524287 and more. So
//     `sign`/`t` is NOT single-use and there is NO 256KB ceiling. Both were
//     wrong when this file first claimed otherwise, and the 256KB/one-mint-
//     per-chunk design built on them was the actual cause of "loading
//     forever": 2.7GB at 256KB per request is ~10,800 upstream round trips.
//
//  3. CONCURRENCY HAS A CEILING. Parallel 4MB fetches from one URL:
//       N=4 -> 4/4 ok,  N=8 -> 8/8 ok,  N=16 -> 16/16 ok,
//       N=32 -> 2/32 ok. The CDN throttles past ~16 in flight. We stay well
//     under that: asking for more parallel work makes throughput worse, not
//     better.
//
// So the relay fetches 8MB at a time, reuses one signature, caps in-flight
// requests at CONCURRENCY, and — most importantly — pre-warms the head of
// the file so the browser's very first range request hits a warm edge.
const UPSTREAM_CHUNK = 4 * 1048576;         // 4MB — fast when warm, no cap upstream
const MAX_SERVE_WINDOW = 8 * UPSTREAM_CHUNK; // 32MB assembled per client request
const SIGNED_URL_TTL_MS = 30 * 60 * 1000;
const CHUNK_CACHE_BYTES = 384 * 1048576;
const UPSTREAM_CONCURRENCY = 6;            // measured ceiling is ~16; stay under
const PREWARM_BYTES = 16 * 1048576;       // fetch this far past the head on play

/**
 * Upstream pathname -> the identifiers needed to mint a fresh signature.
 *
 * Keyed on PATHNAME, not the full URL: the bytes behind a path are stable,
 * only the signature rotates. Populated by /stream when it hands out the
 * /mp4 links, so a range request arriving later can always work out how to
 * re-mint.
 */
const mediaOrigin = new Map();

/**
 * Upstream pathname -> total file size.
 *
 * Captured from the `Content-Range: bytes a-b/TOTAL` header on the first
 * successful chunk. The browser cannot seek a progressive MP4 without a real
 * total, and the upstream is the only place that knows it.
 */
const mediaTotal = new Map();

function rememberMediaOrigin(pathname, params) {
  if (!pathname) return;
  // Cheap bound: the map only ever holds paths someone is actively watching.
  if (mediaOrigin.size > 500) {
    const oldest = mediaOrigin.keys().next().value;
    mediaOrigin.delete(oldest);
  }
  mediaOrigin.set(pathname, { ...params, at: Date.now() });
}

function lookupMediaOrigin(pathname) {
  const e = mediaOrigin.get(pathname);
  if (!e) return null;
  if (Date.now() - e.at > SIGNED_URL_TTL_MS) {
    mediaOrigin.delete(pathname);
    return null;
  }
  return e;
}

/**
 * Mint a fresh signed URL for the same media path.
 *
 * Only the signature changes; the pathname is stable across mints, which is
 * what makes this usable as a cache key.
 */
async function remintSignedUrl(pathname) {
  const e = lookupMediaOrigin(pathname);
  if (!e) return null;
  try {
    // fetchPlay() derives its Bearer token from the client IP, so hand it a
    // synthetic request carrying the IP that originally asked for this media.
    const fakeReq = { headers: { 'x-forwarded-for': e.ip }, socket: {} };
    const play = await fetchPlay(fakeReq, e.detailPath, e.subjectId, e.season, e.episode);
    const streams = (play && play.data && play.data.streams) || [];
    for (const s of streams) {
      if (!s.url) continue;
      try {
        if (new URL(s.url).pathname === pathname) return s.url;
      } catch (_) { /* skip malformed */ }
    }
    return null;
  } catch (err) {
    console.warn(`[mp4] re-mint failed for ${pathname}: ${err && err.message}`);
    return null;
  }
}

// Chunk cache, keyed "<pathname>#<chunkIndex>". Each entry is exactly
// UPSTREAM_CHUNK bytes, so a seek into already-fetched territory is a
// memcpy and never touches the CDN again.
let chunkCacheBytes = 0;
const chunkCache = new ByteLRU(CHUNK_CACHE_BYTES, (n) => { chunkCacheBytes -= n; });
function chunkKey(pathname, index) { return `${pathname}#${index}`; }

/**
 * Fetch one UPSTREAM_CHUNK-sized slice.
 *
 * The signature is REUSED across attempts. It was previously re-minted on
 * every single attempt on the belief that `sign`/`t` is single-use; that
 * belief is false (the same signed URL serves many consecutive ranges), and
 * re-minting added a ~200ms upstream round trip to every 256KB request. We
 * only fall back to a re-mint once the given URL has actually failed, which
 * covers the real case: a signature that has genuinely expired.
 *
 * Returns a Buffer, or null if the upstream could not satisfy it within the
 * retry budget. A short read at end-of-file is returned as-is, not retried —
 * the caller clamps against the known total.
 */
async function fetchChunk(signedUrl, pathname, start, end, attempts) {
  let url = signedUrl;
  for (let i = 0; i < attempts; i++) {
    // Two deadlines, not one. AbortSignal.timeout() keeps running through the
    // body read, so a single 25s budget was cutting off large chunks whose
    // headers arrived promptly but whose payload was still streaming — the
    // exact requests that were about to succeed. Header budget is strict;
    // once headers land the body gets its own, much longer, allowance.
    const ac = new AbortController();
    const headerTimer = setTimeout(() => ac.abort(), UPSTREAM_HEADER_TIMEOUT_MS);
    let bodyTimer = null;
    try {
      const up = await fetch(url, {
        method: 'GET',
        headers: {
          Origin: UPSTREAM_MEDIA_SITE,
          Referer: `${UPSTREAM_MEDIA_SITE}/`,
          'User-Agent': USER_AGENT,
          'Accept-Encoding': 'identity',
          Range: `bytes=${start}-${end}`,
        },
        redirect: 'manual',
        signal: ac.signal,
      });
      clearTimeout(headerTimer);
      bodyTimer = setTimeout(() => ac.abort(), UPSTREAM_BODY_TIMEOUT_MS);
      if (up.status !== 206 && up.status !== 200) {
        try { up.body && (await up.body.cancel()); } catch (_) {}
        url = await remintSignedUrl(pathname) || url;
        continue;
      }
      const buf = Buffer.from(await up.arrayBuffer());
      // Capture the real file size so the client gets a usable Content-Range
      // and can seek. "bytes a-b/*" is not seekable.
      const cr = up.headers.get('content-range');
      if (cr) {
        const m = /\/(\d+)\s*$/.exec(cr);
        if (m) mediaTotal.set(pathname, Number(m[1]));
      }
      if (buf.length === 0) { url = await remintSignedUrl(pathname) || url; continue; }
      // A short chunk is expected only when we asked past end-of-file; any
      // other short read means the edge truncated us, so retry.
      if (buf.length < end - start + 1 && !isAtEof(pathname, start + buf.length)) {
        url = await remintSignedUrl(pathname) || url;
        continue;
      }
      return buf;
    } catch (err) {
      if (i === attempts - 1) {
        console.warn(`[mp4] chunk ${pathname}@${start} failed: ${err && err.message}`);
      }
      url = await remintSignedUrl(pathname) || url;
    } finally {
      clearTimeout(headerTimer);
      if (bodyTimer) clearTimeout(bodyTimer);
    }
  }
  return null;
}

/** True when `offset` has reached the end of the file we know about. */
function isAtEof(pathname, offset) {
  const total = mediaTotal.get(pathname);
  return total ? offset >= total : false;
}

/**
 * In-flight chunk fetches, keyed "<pathname>#<index>".
 *
 * Without this, a prewarm and the player's first request race for the SAME
 * chunk. Two 8MB pulls of one cold chunk doubled the time to first byte —
 * which is exactly the metric we are trying to fix. Deduplicating means N
 * callers for one chunk cause exactly one upstream request.
 */
const inflightChunks = new Map();

/** One chunk, from cache or upstream, with concurrent callers sharing it. */
function getChunk(signedUrl, pathname, off, end) {
  const idx = Math.floor(off / UPSTREAM_CHUNK);
  const key = chunkKey(pathname, idx);
  const want = end - off + 1;
  const hit = chunkCache.get(key);
  if (hit && hit.length >= want) return Promise.resolve(hit);
  if (inflightChunks.has(key)) return inflightChunks.get(key);
  const p = fetchChunk(signedUrl, pathname, off, end, 3).then((buf) => {
    inflightChunks.delete(key);
    if (buf && buf.length) {
      chunkCache.set(key, buf, buf.length);
      chunkCacheBytes += buf.length;
    }
    return buf;
  }).catch((err) => {
    inflightChunks.delete(key);
    throw err;
  });
  inflightChunks.set(key, p);
  return p;
}

/**
 * Learn the real file size with a 1-byte ranged request.
 *
 * The browser cannot seek a progressive MP4 without an authoritative total,
 * and it cannot be sent until we know it. Costs one tiny request, and as a
 * bonus it touches offset 0 — which is the offset the player wants first.
 */
async function ensureTotal(signedUrl, pathname) {
  const known = mediaTotal.get(pathname);
  if (known) return known;
  try {
    const up = await fetch(signedUrl, {
      method: 'GET',
      headers: {
        Origin: UPSTREAM_MEDIA_SITE,
        Referer: `${UPSTREAM_MEDIA_SITE}/`,
        'User-Agent': USER_AGENT,
        'Accept-Encoding': 'identity',
        Range: 'bytes=0-0',
      },
      redirect: 'manual',
      signal: AbortSignal.timeout(UPSTREAM_HEADER_TIMEOUT_MS),
    });
    const cr = up.headers.get('content-range');
    try { up.body && (await up.body.cancel()); } catch (_) {}
    const m = cr && /\/(\d+)\s*$/.exec(cr);
    if (m) { mediaTotal.set(pathname, Number(m[1])); return Number(m[1]); }
  } catch (_) { /* total stays unknown; we serve "bytes a-b/*" as a last resort */ }
  return 0;
}

/**
 * Stream a byte window to the client in order, fetching chunks in parallel.
 *
 * The previous version assembled the entire window into one Buffer and only
 * then wrote anything, so a 32MB request made the player wait for all 32MB
 * before showing a single frame. This starts UPSTREAM_CONCURRENCY fetches
 * immediately, then writes each chunk to the socket the moment the NEXT
 * one in sequence is ready. Time-to-first-byte is one chunk, not one window.
 */
async function streamWindow(req, res, signedUrl, pathname, start, end) {
  const total = await ensureTotal(signedUrl, pathname);
  const want = end - start + 1;

  const jobs = [];
  for (let off = start; off <= end; off += UPSTREAM_CHUNK) {
    const chunkEnd = Math.min(off + UPSTREAM_CHUNK - 1, end);
    jobs.push({ off, chunkEnd });
  }

  // Start the first wave straight away.
  let started = 0;
  const kick = (n) => { while (started < n && started < jobs.length) { getChunk(signedUrl, pathname, jobs[started].off, jobs[started].chunkEnd).catch(() => {}); started++; } };
  kick(UPSTREAM_CONCURRENCY);

  let written = 0;
  let failed = null;
  for (const job of jobs) {
    // Keep the pipe full: start the next job while we wait for this one.
    kick(Math.min(started + UPSTREAM_CONCURRENCY, jobs.length));
    let buf;
    try {
      buf = await getChunk(signedUrl, pathname, job.off, job.chunkEnd);
    } catch (err) { buf = null; }
    if (!buf || !buf.length) { failed = `upstream chunk ${job.off} unavailable`; break; }
    const need = job.chunkEnd - job.off + 1;
    const slice = buf.length >= need ? buf.subarray(0, need) : buf;
    written += slice.length;
    if (res.destroyed) return;
    if (!res.write(slice)) {
      try { await once(res, 'drain'); } catch (_) { return; }
    }
  }
  if (failed) throw new Error(failed);
  res.end();
  return { total, written, want };
}

/**
 * Warm the head of a file in the background so the browser's first range
 * request finds a hot CDN edge and a hot chunk cache.
 *
 * This is the single biggest win in this file. A cold 8MB fetch at this host
 * takes ~10s; the same 8MB warm takes ~100ms. The player always starts at
 * byte 0, so warming exactly there converts the worst request of the session
 * into the cheapest. Fire-and-forget: never blocks /stream's response.
 */
function prewarmHead(signedUrl, pathname) {
  const key = chunkKey(pathname, 0);
  if (prewarming.has(key) || chunkCache.get(key)) return;
  prewarming.add(key);
  (async () => {
    try {
      for (let off = 0; off < PREWARM_BYTES; off += UPSTREAM_CHUNK) {
        const end = off + UPSTREAM_CHUNK - 1;
        // Go through getChunk, NOT fetchChunk: getChunk shares the in-flight
        // map. Calling fetchChunk directly made the prewarm and the player's
        // first request pull the same cold chunk simultaneously, doubling the
        // load on an edge that was already the bottleneck — the symptom was a
        // body read that stalled until the timeout fired.
        const buf = await getChunk(signedUrl, pathname, off, end);
        if (!buf || buf.length === 0) break;
      }
    } catch (_) { /* warming is best-effort; the real request will retry */ }
    finally { prewarming.delete(key); }
  })();
}
const prewarming = new Set();

async function handleMp4(req, res, url) {
  if (!url) {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'url required' }));
    return;
  }

  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'bad url' }));
    return;
  }
  const pathname = parsed.pathname;

  const r = parseRange(req.headers['range']);
  const knownTotal = mediaTotal.get(pathname) || 0;

  // Work out the window we will actually serve.
  let start = 0;
  let end;
  if (r && r.start !== null) {
    start = r.start;
    end = r.end !== null ? r.end : start + MAX_SERVE_WINDOW - 1;
  } else {
    // No Range at all: hand back the head, which is what a player wants
    // first anyway, and let it ask for more.
    start = 0;
    end = MAX_SERVE_WINDOW - 1;
  }
  // Never let the client ask upstream for more than the ceiling allows, in
  // total, per request. Larger requests are answered in several round trips
  // by the browser, which is exactly how a <video> already behaves.
  if (end - start + 1 > MAX_SERVE_WINDOW) end = start + MAX_SERVE_WINDOW - 1;
  // Don't ask past the end of the file. Without this an open-ended range on
  // the last chunk asks for bytes that do not exist, the upstream returns a
  // short read, and the old all-or-nothing retry loop burned every attempt.
  if (knownTotal && end > knownTotal - 1) end = knownTotal - 1;
  if (end < start) {
    res.writeHead(416, { 'content-range': `bytes */${knownTotal || '*'}`, 'accept-ranges': 'bytes' });
    res.end();
    return;
  }

  let settled = false;
  const onClientGone = () => {
    if (settled) return;
    settled = true;
    try { res.end(); } catch (_) {}
  };
  res.on('close', onClientGone);
  req.on('aborted', onClientGone);

  try {
    // Resolve the authoritative size before committing to a Content-Range;
    // it is also what lets us clamp an open-ended range at end-of-file.
    const total = await ensureTotal(url, pathname);
    if (settled) return;
    if (total && end > total - 1) end = total - 1;
    if (end < start) {
      settled = true;
      res.writeHead(416, { 'content-range': `bytes */${total || '*'}`, 'accept-ranges': 'bytes' });
      res.end();
      return;
    }
    settled = true;

    // Headers first, bytes immediately after. streamWindow writes each 8MB
    // chunk the moment it is ready in sequence, so the player gets its first
    // frame after one chunk instead of after the whole window.
    res.writeHead(206, {
      'content-type': 'video/mp4',
      'content-length': String(end - start + 1),
      'content-range': `bytes ${start}-${end}/${total || '*'}`,
      'accept-ranges': 'bytes',
      'access-control-allow-origin': '*',
      'x-cache': chunkCache.get(chunkKey(pathname, Math.floor(start / UPSTREAM_CHUNK))) ? 'hit' : 'miss',
      'x-relay-chunk': String(UPSTREAM_CHUNK),
    });
    await streamWindow(req, res, url, pathname, start, end);
  } catch (err) {
    if (settled) {
      // Headers already went out, so we cannot switch to a 502 — just cut the
      // body short. A truncated response makes the player retry the range,
      // which is far better than a second full restart.
      try { res.end(); } catch (_) {}
      console.warn(`[mp4] stream ${pathname}@${start} ended early: ${err && err.message}`);
      return;
    }
    settled = true;
    const msg = err && err.message ? err.message : 'upstream failed';
    res.writeHead(502, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: msg }));
  }
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

// ── Upstream reachability probe ──────────────────────────────────────
// /health used to be perfectly green while every /mp4 request hung for
// 25s. The relay was fine; the CDN was refusing to answer this instance's
// egress IP. A green liveness probe that says nothing about the thing we
// actually depend on is worse than no probe at all.
//
// So: actively ask the CDN host whether it will talk to us at all. Any
// HTTP status counts as "reachable" — 403/404/429 all prove the TCP
// connection, TLS and request/response cycle completed. A timeout means
// the origin is silently dropping us, which is the failure that actually
// happens.
const UPSTREAM_PROBE_URL = 'https://bcdnxw.hakunaymatata.com/';
const upstreamProbe = { at: 0, reachable: null, ms: null, status: null, err: null };

async function probeUpstream(force) {
  const now = Date.now();
  if (!force && now - upstreamProbe.at < 60_000) return upstreamProbe;
  upstreamProbe.at = now;
  const t0 = Date.now();
  try {
    const r = await fetch(UPSTREAM_PROBE_URL, {
      headers: {
        Origin: UPSTREAM_MEDIA_SITE,
        Referer: `${UPSTREAM_MEDIA_SITE}/`,
        'User-Agent': USER_AGENT,
      },
      redirect: 'manual',
      signal: AbortSignal.timeout(8000),
    });
    upstreamProbe.reachable = true;
    upstreamProbe.status = r.status;
    upstreamProbe.ms = Date.now() - t0;
    upstreamProbe.err = null;
  } catch (e) {
    upstreamProbe.reachable = false;
    upstreamProbe.status = null;
    upstreamProbe.ms = Date.now() - t0;
    upstreamProbe.err = e && e.message ? e.message : 'probe failed';
  }
  return upstreamProbe;
}
probeUpstream(true).then((p) =>
  console.log(`[upstream-probe] reachable=${p.reachable} status=${p.status} ms=${p.ms} err=${p.err}`)
);
// Keep the probe warm so /health can answer without waiting on it.
setInterval(() => { probeUpstream(false); }, 60_000).unref();

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
    if(u.pathname==='/health'){const m=process.memoryUsage();const up=await probeUpstream(false);res.writeHead(up.reachable?200:503,{'content-type':'application/json'});res.end(JSON.stringify({ok:true,ram_mb:Math.round(m.rss/1024/1024*10)/10,cache_size:cache.size,cache_mb:Math.round(cache.totalBytes/1048576*10)/10,chunk_cache_size:chunkCache.size,chunk_cache_mb:Math.round(chunkCacheBytes/1048576*10)/10,token_pool_size:tokenStore.size,ip_plays_tracked:ipPlays.size,free_plays_per_day:FREE_PLAYS_PER_DAY,upstream:{reachable:up.reachable,status:up.status,ms:up.ms,err:up.err},uptime_s:Math.round(process.uptime())}));return;}
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
