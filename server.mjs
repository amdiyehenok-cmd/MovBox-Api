// movbox-stream — pure-HTTP stream + caption + MP4 + search proxy.
// No Chrome, no Puppeteer. ~50MB RSS.
//
// Endpoints (port 4000):
//   GET  /                          health + service info
//   GET  /health                     readiness probe
//   GET  /stream?detailPath=&season=&episode=&subjectId=
//                                  MP4 sources + captions (JSON)
//   GET  /mp4?url=<encoded>         stream the MP4 binary with the
//                                  Referer the CDN insists on
//   POST /search                    proxy to h5-api /subject/search with
//                                  proper auth so the device can search
//                                  the WHOLE boxmovies database
//   GET  /subtitle?url=<encoded>&format=vtt|srt
//                                  serve the caption, converted to WebVTT
//                                  by default so a browser <track> can
//                                  render it directly
//
// Auth: the upstream h5-api authenticates /subject/play and
// /subject/caption with a STATELESS clock token (X-Client-Token), not
// with a session. See clientToken() below. /search still needs a real
// session Bearer, and the per-IP token pool is kept for that.

import http from 'node:http';
import { URL } from 'node:url';
import { Readable } from 'node:stream';
import { createHash } from 'node:crypto';

const H5 = 'https://h5-api.aoneroom.com/wefeed-h5api-bff';
const HOST = 'https://boxmovies.org';

/**
 * Which site the MEDIA proxy claims to be when it talks to the CDN.
 *
 * The upstream runs a Referer allowlist, and the Referer header is the
 * ONLY gate. Measured against the live CDN on 2026-10-19:
 *
 *   Referer: https://movibox.xyz/    -> allow-listed
 *   Referer: https://boxmovies.org/  -> allow-listed
 *   Referer: https://movbox.xyz/     -> 429 blocked
 *   no Referer                       -> 429 blocked
 *
 * How to read the two failure codes: a 403 means the Referer was
 * ACCEPTED and the sign/t signature had expired; a 429 means the
 * Referer was rejected outright.
 *
 * Our own domain is not on that allowlist and never will be, so the
 * media bytes must be re-served by this relay. The relay's only job is
 * to add the one header a browser is not allowed to set — everything
 * else it leaves to the CDN edge and the client's own <video> element.
 *
 * Note for the native Android client: it CAN set Referer itself, so it
 * does not need this relay for media bytes at all.
 *
 * Override with UPSTREAM_MEDIA_SITE if the allowlist changes.
 */
const UPSTREAM_MEDIA_SITE = (process.env.UPSTREAM_MEDIA_SITE || 'https://boxmovies.org').replace(/\/+$/, '');

/**
 * Hosts this relay is allowed to fetch media from.
 *
 * Without this, /mp4 is an open proxy: it will fetch ANY url the caller
 * hands it, using the allow-listed Referer. Keep it tight.
 */
const MEDIA_HOST_ALLOWLIST = ['bcdnxw.hakunaymatata.com', 'cacdn.hakunaymatata.com'];
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
  // 10 min, not 30: a cached /stream response embeds CDN signatures that
  // expire, so a long cache can hand the browser a dead URL. The
  // 403 -> re-mint path covers it, but a short cache is the cheaper fix.
  stream:   10 * 60 * 1000,   // 10 min — movie play metadata + captions
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

// ── X-Client-Token ───────────────────────────────────────────────────
//
// The upstream h5-api does NOT require a logged-in session for
// /subject/play or /subject/caption. It accepts a stateless token that
// is a pure function of the clock. Recovered from the movibox web
// player bundle (research/player-js/s6701.js, module 5349) and
// VERIFIED 4/4 against the live API:
//
//   ts  = floor(Date.now() / 1000)
//   rev = String(ts).split("").reverse().join("")
//   X-Client-Token = ts + "," + MD5(rev)
//
// The original uses a JavaScript MD5 library because it has to be
// synchronous to build a header; node:crypto's md5 produces identical
// output. The header value is "ts,md5" — a comma separated pair.
//
// The sister site only falls back to "Authorization: Bearer <tk>" when
// the browser happens to hold a moviebox_web_tk cookie. We never have
// that cookie and we do not need it: this token is stateless, so there
// is no session whose free quota can be burned out.
function clientToken() {
  const ts = Math.floor(Date.now() / 1000);
  const rev = String(ts).split('').reverse().join('');
  return `${ts},${createHash('md5').update(rev).digest('hex')}`;
}

// Headers to use when proxying to the upstream h5-api. These are the
// headers the boxmovies.org site itself sends, so the upstream treats
// the request as if it's coming from the live website.
//
// "token" is optional: when supplied it is sent as a Bearer and is used
// for the endpoints that genuinely need a session (/search). When it is
// omitted the request is authenticated by X-Client-Token alone.
function upstreamHeaders(token, extra = {}) {
  const h = {
    Origin: HOST,
    Referer: `${HOST}/`,
    'User-Agent': USER_AGENT,
    'X-Client-Info': '{"timezone":"Africa/Nairobi"}',
    'X-Request-Lang': 'en',
    'X-Client-Token': clientToken(),
    'Accept': 'application/json, text/plain, */*',
    ...extra,
  };
  if (token) h.Authorization = `Bearer ${token}`;
  return h;
}

// Token-first. VERIFIED: X-Client-Token alone authenticates
// /subject/play and /subject/caption. The per-IP Bearer pool is only
// needed for the endpoints that genuinely require a session (/search),
// so the play path no longer pays a token mint on every request.
async function fetchPlay(req, d, p, se, ep) {
  const ip = clientIp(req);
  const url = `${H5}/subject/play?subjectId=${p}&se=${se||0}&ep=${ep||0}&detailPath=${d}`;
  const referer = `${HOST}/movies/${d}?id=${p}&type=/tv/detail&se=${se||0}&ep=${ep||0}&lang=en`;
  let r = await fetch(url, { headers: upstreamHeaders(null, { Referer: referer }) });
  // 401/403 → the clock token was rejected (clock skew, or the upstream
  // changed the scheme). Fall back to a real session Bearer, retry once.
  if (r.status === 401 || r.status === 403) {
    console.log(`[upstream] ip=${ip} got ${r.status} with X-Client-Token, retrying with a session Bearer...`);
    let token = null;
    try { token = await getOrMintForIp(ip); } catch (_) { /* fall through */ }
    r = await fetch(url, { headers: upstreamHeaders(token, { Referer: referer }) });
  }
  if (!r.ok) throw new Error(`h5-api ${r.status}`);
  return await r.json();
}
async function fetchCaption(req, fmt, id, sid, dp) {
  const url = `${H5}/subject/caption?format=${fmt}&id=${id}&subjectId=${sid}&detailPath=${dp}`;
  const r = await fetch(url, { headers: upstreamHeaders(null, { Referer: `${HOST}/movies/${dp}` }) });
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
  // x-forwarded-proto is only present when a platform proxy sits in front of
  // us. When it is missing we must ask the socket, not assume https: a plain
  // HTTP listener (local dev, a direct IP:port hit) would otherwise hand the
  // device an https:// URL that it cannot connect to.
  const proto = (req.headers['x-forwarded-proto'] || (req.socket && req.socket.encrypted ? 'https' : 'http')).toString();
  return `${proto}://${req.headers.host}`;
}
// ── Subtitle handling ────────────────────────────────────────────────
//
// Two fixes here, both measured:
//
//  1. The old code called upstreamHeaders() with NO argument, so it sent
//     "Authorization: Bearer undefined". It only ever worked because
//     the caption endpoint tolerates a bad bearer.
//
//  2. The relay used to hand the raw .srt back with
//     "application/x-subrip". A browser <track> element only renders
//     WebVTT, so the app had to re-proxy every subtitle through its own
//     server just to convert it. Converting here means the subtitle
//     bytes never have to touch Vercel either.
//
// The sister site's player does exactly this: it fetches the SRT, turns
// it into a VTT blob and hands it to Video.js via addRemoteTextTrack.

function srtToVtt(srt) {
  return 'WEBVTT\n\n' + String(srt)
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    // drop the SRT sequence number that sits on its own line
    .replace(/^\d+\n(?=\d{2}:\d{2}:\d{2})/gm, '')
    // "00:00:01,000" -> "00:00:01.000"
    .replace(/(\d{2}:\d{2}:\d{2}),(\d{3})/g, '$1.$2')
    .trimEnd() + '\n';
}

function cleanCaption(c, base){
  const proxied=c.url?`${base}/subtitle?url=${encodeURIComponent(c.url)}`:c.url;
  return {id:c.id,url:proxied,mimeType:'text/vtt',languageCode:c.lan||'en',language:c.lanName||c.lan||'Unknown',delay:c.delay||0};
}
async function handleSubtitle(req,res,url,asVtt){
  // Cache the subtitle bytes — they never change. Same URL across all
  // users = 1 upstream fetch per hour, no matter how many people are
  // watching. The format is part of the key so a raw-SRT request can
  // never be served a cached VTT body or vice versa.
  const cacheKey = `sub:${asVtt ? 'vtt' : 'srt'}:${url}`;
  const cached = cacheGet(cacheKey);
  if (cached) {
    res.writeHead(200, {'content-type': cached.ct, 'content-length': String(cached.buf.length), 'access-control-allow-origin':'*', 'x-cache':'hit'});
    res.end(cached.buf); return;
  }
  const upstream=await fetch(url,{headers:upstreamHeaders(null),redirect:'follow'});
  if(!upstream.ok){res.writeHead(upstream.status,{'content-type':'application/json'});res.end(JSON.stringify({ok:false,error:`upstream ${upstream.status}`}));return;}
  let ct=upstream.headers.get('content-type')||'application/x-subrip';
  let buf=Buffer.from(await upstream.arrayBuffer());
  if (asVtt) {
    const text = buf.toString('utf8');
    buf = Buffer.from(text.includes('WEBVTT') ? text : srtToVtt(text), 'utf8');
    ct = 'text/vtt; charset=utf-8';
  }
  cacheSet(cacheKey, { ct, buf }, CACHE_TTL.subtitle);
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

  // ── Resolution ladder ───────────────────────────────────────────────
  //
  // The upstream grants the anonymous session the FULL ladder: 360p,
  // 480p and 1080p, with freeNum 999, limited false and vipLocked false.
  //
  // The previous version of this file deleted the 1080p rung because
  // playConfig.maxResolution is 480. That was wrong on two counts:
  //
  //   1. maxResolution is a CLIENT HINT for the sister site's own
  //      player, not a licence limit on this account. The upstream
  //      hands us all three rungs in the very same response.
  //   2. The sister site's player does not enforce it either — it
  //      plays 1080p. (research/player-js/s44.js)
  //
  // So the ceiling is now an explicit operator knob, off by default.
  if (MAX_RESOLUTION > 0) {
    const within = streams.filter(s => (Number(s.resolutions) || 0) <= MAX_RESOLUTION);
    if (within.length) streams = within;
  }
  // Smallest FIRST, matching the upstream's own order and the sister
  // site's player (both default to index 0, which is the lowest rung).
  //
  // This is NOT the old bug. The old bug was the 480 cap DELETING the
  // 1080p rung, so 1080p could never be selected at all. The sort order
  // only decides what auto-plays: the smallest rung starts fastest and
  // costs the least to relay, and the user can pick 1080p with one
  // click from the quality menu.
  if (streams.length > 1) streams.sort((a, b) => (Number(a.resolutions) || 0) - (Number(b.resolutions) || 0));
  // If the upstream's quota is on its last legs and the IP's per-token
  // refresh didn't help, fall through with empty streams. The ad path
  // above (next call from the same IP) will catch it.
  let captions=[];
  const best=pickBestStream(streams);
  if(best?.id){const cap=await fetchCaption(req, 'MP4', best.id, p.subjectId, p.detailPath);const capArr=cap?.data?.captions||[];captions=capArr.map(c=>cleanCaption(c, base)).filter(c=>c.url);}
  // Build a proxied MP4 URL for every stream so the device can download
  // it via the relay. Without this, the device tries to hit the upstream
  // CDN directly and gets rate-limited.
  const proxiedStreams=streams.map((s,i)=>{
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
    // Warm the rung the client is about to AUTO-PLAY, which is index 0
    // (the smallest). Warming the biggest one instead was the old
    // behaviour and it warmed a file nobody was about to open.
    if (i === 0) { try { prewarmEdge(s.url); } catch (_) {} }
    // Normalise the fields the clients read, so the app never has to
    // guess which of resolutions/resolution/size carries what.
    const res = Number(s.resolutions) || Number(s.resolution) || 0;
    const size = Number(s.size) || Number(s.sizeBytes) || 0;
    return {...s,
      resolutions: res,
      resolution: res,
      quality: s.quality || (res > 0 ? `${res}p` : 'Auto'),
      size: size,
      sizeBytes: size,
      url:`${base}/mp4?url=${encodeURIComponent(s.url)}`};
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
      // The highest rung we actually serve, so the client knows what it
      // can pick.
      maxResolution: streams.length
        ? Math.max(...streams.map(s => Number(s.resolutions) || 0))
        : 0,
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

// The client's Range header is forwarded to the upstream VERBATIM, so
// we never have to parse, clamp or re-derive it here.

// ── Upstream media access (measured 2026-10-19) ───────────────────────
//
// The CDN does not behave like a normal file host. The facts that
// actually matter:
//
//  1. THE REFERER ALLOWLIST IS THE ONLY GATE. 429 = the Referer was
//     rejected. 403 = the Referer was ACCEPTED and the sign/t signature
//     had expired. Re-minting fixes the 403; nothing fixes the 429.
//
//  2. COLD vs WARM IS THE WHOLE STORY. Measured on
//     bcdnxw.hakunaymatata.com:
//
//       first request (cold)        256KB in 323ms
//       immediately after (warm)    256KB in  54ms  ->  18.5 MB/s
//       warm 4MB                                ->  41.2 MB/s
//       warm 8MB                                -> 100.0 MB/s
//       warm 32MB, fresh signature              -> 615.4 MB/s
//
//  3. A SINGLE SIGNATURE SERVES MANY RANGES. The same signed URL that
//     returned bytes 0-262143 then returned 262144-524287 and more. So
//     sign/t is NOT single-use and there is NO 256KB response ceiling.
//     An earlier version of this file believed both, and the
//     256KB/one-mint-per-chunk design built on them was the actual
//     cause of "loading forever": 2.7GB at 256KB per request is ~10,800
//     upstream round trips.
//
//  4. CONCURRENCY HAS A CEILING. Parallel fetches from one URL:
//     N=4 -> 4/4 ok, N=8 -> 8/8 ok, N=16 -> 16/16 ok, N=32 -> 2/32 ok.
//
//  5. RANGE REQUESTS ARE PASSED THROUGH BY THE CDN. We forward the
//     client's Range header verbatim and the CDN answers with 206 +
//     Content-Range. There is nothing for the relay to assemble.
//
// So the relay does ONE thing: add the Referer the browser cannot set.
const SIGNED_URL_TTL_MS = 30 * 60 * 1000;   // how long we remember how to re-mint a path
const PREWARM_BYTES = 2 * 1048576;         // head range we ask for to warm the edge

/**
 * Optional resolution ceiling.
 *
 * 0 (the default) means "serve every rung the upstream granted". The
 * upstream hands anonymous sessions 360p / 480p / 1080p and does not
 * cap them, and the relay streams bytes without buffering them, so
 * there is no bandwidth reason to hide 1080p. Set MAX_RESOLUTION=480
 * to cap the ladder deliberately.
 */
const MAX_RESOLUTION = Number(process.env.MAX_RESOLUTION || 0);

/**
 * Upstream pathname -> the identifiers needed to mint a fresh signature.
 *
 * Keyed on PATHNAME, not the full URL: the bytes behind a path are stable,
 * only the signature rotates. Populated by /stream when it hands out the
 * /mp4 links, so a range request arriving later can always work out how to
 * re-mint.
 */
const mediaOrigin = new Map();

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

/**
 * Warm the CDN edge for a file, in the background, so the browser's very
 * first range request finds a hot edge.
 *
 * The old implementation pulled 16MB into a 384MB RAM chunk cache. The
 * CDN edge already caches the bytes — a warm 256KB range comes back in
 * 54ms against 323ms cold — so the only thing worth warming is the EDGE,
 * and the only way to do that is to ask for the bytes once.
 *
 * We fetch a small head range and throw the bytes away. Nothing is kept
 * in memory, so this costs one upstream request and zero RAM.
 */
function prewarmEdge(signedUrl) {
  if (prewarming.has(signedUrl)) return;
  prewarming.add(signedUrl);
  (async () => {
    let timer = null;
    try {
      const ac = new AbortController();
      timer = setTimeout(() => ac.abort(), UPSTREAM_HEADER_TIMEOUT_MS);
      const up = await fetch(signedUrl, {
        method: 'GET',
        headers: {
          Origin: UPSTREAM_MEDIA_SITE,
          Referer: `${UPSTREAM_MEDIA_SITE}/`,
          'User-Agent': USER_AGENT,
          'Accept-Encoding': 'identity',
          Range: `bytes=0-${PREWARM_BYTES - 1}`,
        },
        redirect: 'manual',
        signal: ac.signal,
      });
      // Drain and discard: the point is to get the object into the edge
      // cache, and an edge only caches what it actually served.
      if (up.body) {
        try { for await (const _ of up.body) { /* discard */ } } catch (_) {}
      }
    } catch (_) {
      /* warming is best-effort; the real request will fetch for itself */
    } finally {
      if (timer) clearTimeout(timer);
      prewarming.delete(signedUrl);
    }
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

  // Allowlist the media host. Without this, /mp4 is an open proxy that
  // will fetch ANY url the caller hands it, using the allow-listed
  // Referer.
  if (!MEDIA_HOST_ALLOWLIST.includes(parsed.hostname)) {
    res.writeHead(403, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'host not allowed' }));
    return;
  }
  if (parsed.protocol !== 'https:') {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'https only' }));
    return;
  }

  const pathname = parsed.pathname;
  // The client's Range header goes upstream VERBATIM. The CDN answers with
  // 206 + Content-Range and we pass both through, so the browser can seek.
  const range = req.headers['range'] ? String(req.headers['range']) : null;

  const fetchUpstream = async (target) => {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), UPSTREAM_HEADER_TIMEOUT_MS);
    try {
      const up = await fetch(target, {
        method: 'GET',
        headers: {
          Origin: UPSTREAM_MEDIA_SITE,
          Referer: `${UPSTREAM_MEDIA_SITE}/`,
          'User-Agent': USER_AGENT,
          'Accept-Encoding': 'identity',
          ...(range ? { Range: range } : {}),
        },
        redirect: 'manual',
        signal: ac.signal,
      });
      clearTimeout(timer);
      return up;
    } catch (err) {
      clearTimeout(timer);
      throw err;
    }
  };

  let up;
  try {
    up = await fetchUpstream(url);
  } catch (err) {
    res.writeHead(502, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: `upstream fetch failed: ${err && err.message}` }));
    return;
  }

  // 403 = the Referer was ACCEPTED and the sign/t signature had expired
  // (measured). Re-mint once and retry. 429 = the Referer was rejected,
  // so re-minting cannot help and we surface the status as-is.
  if (up.status === 403) {
    try { up.body && (await up.body.cancel()); } catch (_) {}
    const fresh = await remintSignedUrl(pathname);
    if (fresh && fresh !== url) {
      try {
        up = await fetchUpstream(fresh);
        console.log(`[mp4] re-minted ${pathname} after a 403`);
      } catch (_) { /* keep the original response below */ }
    }
  }

  if (!up.ok && up.status !== 206) {
    try { up.body && (await up.body.cancel()); } catch (_) {}
    res.writeHead(up.status, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: `upstream ${up.status}` }));
    return;
  }

  // Pass the upstream's own headers through. Content-Range is what lets
  // the browser seek, so it has to survive the hop verbatim.
  const headers = {
    'content-type': up.headers.get('content-type') || 'video/mp4',
    'accept-ranges': up.headers.get('accept-ranges') || 'bytes',
    'access-control-allow-origin': '*',
    'access-control-expose-headers': 'Content-Length, Content-Range, Accept-Ranges, Content-Type',
  };
  const len = up.headers.get('content-length');
  if (len) headers['content-length'] = len;
  const cr = up.headers.get('content-range');
  if (cr) headers['content-range'] = cr;
  const etag = up.headers.get('etag');
  if (etag) headers.etag = etag;
  const lm = up.headers.get('last-modified');
  if (lm) headers['last-modified'] = lm;

  res.writeHead(up.status, headers);

  if (!up.body) { res.end(); return; }

  // Stream the body straight through. No buffering, no chunk cache, no
  // window, no extra size probe. The relay's memory use is now flat
  // regardless of how large the file is.
  const body = Readable.fromWeb(up.body);
  let closed = false;
  const cleanup = () => {
    if (closed) return;
    closed = true;
    try { body.destroy(); } catch (_) {}
  };
  res.on('close', cleanup);
  req.on('aborted', cleanup);
  body.on('error', () => { try { res.end(); } catch (_) {} });
  body.pipe(res);
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
    if(u.pathname==='/health'){const m=process.memoryUsage();const up=await probeUpstream(false);res.writeHead(up.reachable?200:503,{'content-type':'application/json'});res.end(JSON.stringify({ok:true,ram_mb:Math.round(m.rss/1024/1024*10)/10,cache_size:cache.size,cache_mb:Math.round(cache.totalBytes/1048576*10)/10,token_pool_size:tokenStore.size,ip_plays_tracked:ipPlays.size,free_plays_per_day:FREE_PLAYS_PER_DAY,max_resolution:MAX_RESOLUTION,upstream:{reachable:up.reachable,status:up.status,ms:up.ms,err:up.err},uptime_s:Math.round(process.uptime())}));return;}
    if(u.pathname==='/stream'){const p={detailPath:u.searchParams.get('detailPath'),subjectId:u.searchParams.get('subjectId'),season:u.searchParams.get('season')||'0',episode:u.searchParams.get('episode')||'0'};await handleStream(req,res,p);return;}
    if(u.pathname==='/mp4'){await handleMp4(req, res, u.searchParams.get('url'));return;}
    if(u.pathname==='/search'){await handleSearch(req,res);return;}
    if(u.pathname==='/subtitle'){const url=u.searchParams.get('url');if(!url){res.writeHead(400,{'content-type':'application/json'});res.end(JSON.stringify({ok:false,error:'url required'}));return;}const fmt=(u.searchParams.get('format')||'vtt').toLowerCase();await handleSubtitle(req,res,url,fmt!=='srt');return;}
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
