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

const H5 = 'https://h5-api.aoneroom.com/wefeed-h5api-bff';
const HOST = 'https://boxmovies.org';
const PORT = Number(process.env.PORT || 4000);
const HOSTNAME = process.env.HOST || '0.0.0.0';
const PUBLIC_HOST = process.env.PUBLIC_HOST || '';
const CACHE_MAX = 500;
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
const cache = new Map();   // key → { ts, ttl, data }
// Compose the cache key for a /stream call from the parts that uniquely
// identify a play: detailPath + subjectId + season + episode. We use the
// same shape as the old single-TTL cache so existing entries don't get
// duplicated after a redeploy.
function keyOf(p) {
  return [p.detailPath, p.subjectId, p.season || 0, p.episode || 0].join('|');
}
function cacheGet(k) {
  const e = cache.get(k);
  if (!e) return null;
  if (Date.now() - e.ts > e.ttl) { cache.delete(k); return null; }
  return e.data;
}
function cacheSet(k, d, ttl) {
  if (cache.size >= CACHE_MAX) cache.delete(cache.keys().next().value);
  cache.set(k, { ts: Date.now(), ttl, data: d });
}

// Per-IP rate limiter. Simple sliding window: we keep a counter per IP
// per route and reject with 429 once the cap is hit. Caps are sized for
// a real human on a slow network (~2 req/s on /stream during playback
// of a sideloaded player, ~3 req/s on /search while typing). Anything
// beyond that is a bug or a bot — we drop it and let the client back
// off. This is the last line of defence; primary protection is the
// in-memory cache above.
const RATE_LIMIT = {
  stream:   { windowMs: 60_000, max: 120 },  // 2/s avg
  search:   { windowMs: 60_000, max:  30 },  // 0.5/s avg
  subtitle: { windowMs: 60_000, max: 120 },
};
const rateCounters = new Map();   // key → { count, resetAt }
function rateOk(key, route) {
  const r = RATE_LIMIT[route];
  if (!r) return true;
  const now = Date.now();
  const e = rateCounters.get(key);
  if (!e || now > e.resetAt) {
    rateCounters.set(key, { count: 1, resetAt: now + r.windowMs });
    return true;
  }
  e.count += 1;
  return e.count <= r.max;
}
function clientIp(req) {
  // DO / Render / etc. forward the original client IP in x-forwarded-for.
  // Take the leftmost entry (the real client, per RFC).
  const xff = req.headers['x-forwarded-for'];
  if (typeof xff === 'string' && xff) return xff.split(',')[0].trim();
  return req.socket?.remoteAddress || 'unknown';
}

// Captured from boxmovies.org session (a real Chrome request). This is the
// same kind of token the device's WebView would mint from the mb_token
// cookie. Rotate by re-loading boxmovies.org in Chrome and grabbing
// a fresh Authorization: Bearer header.
const BEARER_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjE5OTU4MjQ2ODY0ODgyNDg3NDQsImF0cCI6MywiZXh0IjoiMTc4ODI1Nzg0NSIsImV4cCI6MTc5NjAzMzg0NSwiaWF0IjoxNzg4MjU3NTQ1fQ.6uSVeSlSvZHGe3Ox8offdeZoDWOn_8SMlnYKaHNW3zI';

const USER_AGENT='Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36';

// Headers to use when proxying to the upstream h5-api. These are the
// headers the boxmovies.org site itself sends, so the upstream treats
// the request as if it's coming from the live website.
function upstreamHeaders(extra = {}) {
  return {
    Origin: HOST,
    Referer: `${HOST}/`,
    'User-Agent': USER_AGENT,
    'x-client-info': '{"timezone":"Africa/Nairobi"}',
    'x-request-lang': 'en',
    'Accept': 'application/json, text/plain, */*',
    Authorization: `Bearer ${BEARER_TOKEN}`,
    ...extra,
  };
}

async function fetchPlay(d,p,se,ep){
  const url=`${H5}/subject/play?subjectId=${p}&se=${se||0}&ep=${ep||0}&detailPath=${d}`;
  const r=await fetch(url,{headers:upstreamHeaders({Referer:`${HOST}/movies/${d}?id=${p}&type=/tv/detail&se=${se||0}&ep=${ep||0}&lang=en`})});
  if(!r.ok) throw new Error(`h5-api ${r.status}`); return await r.json();
}
async function fetchCaption(fmt,id,sid,dp){
  const url=`${H5}/subject/caption?format=${fmt}&id=${id}&subjectId=${sid}&detailPath=${dp}`;
  const r=await fetch(url,{headers:upstreamHeaders({Referer:`${HOST}/movies/${dp}`})});
  if(!r.ok) return null; try{return await r.json();}catch{return null;}
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
  if (!rateOk(`sub:${clientIp(req)}`, 'subtitle')) {
    res.writeHead(429, {'content-type':'application/json'});
    res.end(JSON.stringify({ok:false,error:'rate limited'})); return;
  }
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
  if (!rateOk(`stream:${clientIp(req)}`, 'stream')) {
    res.writeHead(429, {'content-type':'application/json'});
    res.end(JSON.stringify({ok:false,error:'rate limited'})); return;
  }
  const k=keyOf(p);let cached=cacheGet(k);
  if(cached){res.writeHead(200,{'content-type':'application/json','x-cache':'hit'});res.end(JSON.stringify(cached));return;}
  const play=await fetchPlay(p.detailPath,p.subjectId,p.season||0,p.episode||0);
  const data=(play&&play.data)||{};const streams=data.streams||[];let captions=[];
  const best=pickBestStream(streams);
  if(best?.id){const cap=await fetchCaption('MP4',best.id,p.subjectId,p.detailPath);const capArr=cap?.data?.captions||[];captions=capArr.map(c=>cleanCaption(c, base)).filter(c=>c.url);}
  // Build a proxied MP4 URL for every stream so the device can download
  // it via the relay. Without this, the device tries to hit the upstream
  // CDN directly and gets rate-limited.
  const base = baseUrl(req);
  const proxiedStreams=streams.map(s=>{
    if(!s.url) return s;
    return {...s,url:`${base}/mp4?url=${encodeURIComponent(s.url)}`};
  });
  const out={ok:true,source:'movbox-stream',code:play?.code??0,message:play?.message??'ok',data:{streams:proxiedStreams,freeNum:data.freeNum??0,limited:!!data.limited,hasResource:streams.length>0,vipLocked:!!data.vipLocked,codecPriority:data.codecPriority||[],captions}};
  cacheSet(k, out, CACHE_TTL.stream);
  res.writeHead(200,{'content-type':'application/json','x-cache':'miss'});
  res.end(JSON.stringify(out));
}

/**
 * /mp4?url=<encoded>
 *
 * Streams the upstream MP4 back to the phone with the right Referer/
 * Origin headers so the upstream CDN accepts the request. The phone
 * can't fetch the MP4 directly (its IP is rate-limited), so we proxy
 * it. Range requests are passed through so DownloadManager can
 * resume partial downloads.
 */
async function handleMp4(req, res, url) {
  if (!url) {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'url required' }));
    return;
  }
  const upstreamHeaders = {
    Origin: HOST,
    Referer: `${HOST}/`,
    'User-Agent': USER_AGENT,
  };
  if (req.headers['range']) upstreamHeaders['Range'] = req.headers['range'];

  const upstream = await fetch(url, {
    headers: upstreamHeaders,
    redirect: 'follow',
  });
  if (!upstream.ok && upstream.status !== 206) {
    res.writeHead(upstream.status, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: `upstream ${upstream.status}` }));
    return;
  }
  const outHeaders = {
    'content-type': upstream.headers.get('content-type') || 'video/mp4',
    'accept-ranges': upstream.headers.get('accept-ranges') || 'bytes',
    'access-control-allow-origin': '*',
  };
  const len = upstream.headers.get('content-length');
  if (len) outHeaders['content-length'] = len;
  const range = upstream.headers.get('content-range');
  if (range) outHeaders['content-range'] = range;
  res.writeHead(upstream.status, outHeaders);
  const reader = upstream.body.getReader();
  const pump = async () => {
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) { res.end(); return; }
        if (!res.write(value)) {
          await new Promise((r) => res.once('drain', r));
        }
      }
    } catch (e) {
      try { res.end(); } catch (_) {}
    }
  };
  pump();
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
  if (!rateOk(`search:${clientIp(req)}`, 'search')) {
    res.writeHead(429, {'content-type':'application/json'});
    res.end(JSON.stringify({ok:false,error:'rate limited'})); return;
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

  // The upstream search endpoint takes a JSON body with these exact
  // field names. We pass through the user's keyword + pagination +
  // subjectType filter unchanged.
  const upstream = await fetch(`${H5}/subject/search`, {
    method: 'POST',
    headers: upstreamHeaders({ 'Content-Type': 'application/json' }),
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

const server=http.createServer(async(req,res)=>{
  try{
    const u=new URL(req.url,`http://${req.headers.host}`);
    if(u.pathname==='/health'){const m=process.memoryUsage();res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify({ok:true,ram_mb:Math.round(m.rss/1024/1024*10)/10,cache_size:cache.size,uptime_s:Math.round(process.uptime())}));return;}
    if(u.pathname==='/stream'){const p={detailPath:u.searchParams.get('detailPath'),subjectId:u.searchParams.get('subjectId'),season:u.searchParams.get('season')||'0',episode:u.searchParams.get('episode')||'0'};await handleStream(req,res,p);return;}
    if(u.pathname==='/mp4'){await handleMp4(req, res, u.searchParams.get('url'));return;}
    if(u.pathname==='/search'){await handleSearch(req,res);return;}
    if(u.pathname==='/subtitle'){const url=u.searchParams.get('url');if(!url){res.writeHead(400,{'content-type':'application/json'});res.end(JSON.stringify({ok:false,error:'url required'}));return;}await handleSubtitle(req,res,url);return;}
    res.writeHead(404,{'content-type':'application/json'});res.end(JSON.stringify({ok:false,error:'not found'}));
  }catch(e){res.writeHead(500,{'content-type':'application/json'});res.end(JSON.stringify({ok:false,error:e.message}));}
});
server.listen(PORT,HOSTNAME,()=>{console.log(`movbox-stream listening on http://${HOSTNAME}:${PORT}`);});
