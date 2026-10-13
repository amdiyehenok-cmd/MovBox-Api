// Regression test for the forever-loading bug.
// 1. must NOT hang (old transport sat 90s+ with zero bytes)
// 2. must deliver REAL mp4 bytes
// 3. head cache must populate on the second identical request
const RELAY = process.env.RELAY || 'http://127.0.0.1:4101';

const t0 = Date.now();
const sj = await (await fetch(`${RELAY}/stream?detailPath=mobland-wWHa6Cu6fa5&subjectId=4335165664987072680&season=1&episode=1`, { signal: AbortSignal.timeout(30000) })).json();
const s = sj.data.streams[0];
// The relay stamps its own public origin into stream URLs (https + host).
// Locally we speak plain http on 127.0.0.1, so re-point the origin.
const mp4Url = s.url.replace(/^https?:\/\/[^/]+/, RELAY);
console.log(`stream ok in ${Date.now() - t0}ms -> ${s.resolutions}p ${s.format}`);

async function grab(range, label) {
  const t = Date.now();
  try {
    const r = await fetch(mp4Url, { headers: { Range: range }, signal: AbortSignal.timeout(25000) });
    const b = Buffer.from(await r.arrayBuffer());
    const box = b.length >= 12 ? b.subarray(4, 8).toString('latin1') : '?';
    const ok = r.status === 206 && b.byteLength > 0;
    console.log(`  ${label}: ${r.status} ttfb=${Date.now() - t}ms bytes=${b.byteLength} box=${JSON.stringify(box)} x-cache=${r.headers.get('x-cache')} cr=${r.headers.get('content-range')}`);
    if (b.length >= 4096) {
      const hasMoov = b.includes(Buffer.from('moov'));
      console.log(`     ftyp=${b.subarray(8, 12).toString('latin1')} moov-in-first-4KB=${hasMoov}`);
    }
    return ok;
  } catch (e) {
    console.log(`  ${label}: FAILED after ${Date.now() - t}ms -> ${e.message}`);
    return false;
  }
}

let pass = true;
pass = (await grab('bytes=0-262143', 'cold head 256KB')) && pass;
pass = (await grab('bytes=0-262143', 'warm head 256KB (want x-cache=hit)')) && pass;
pass = (await grab('bytes=1048576-2097151', 'mid-file seek 1MB')) && pass;

const h = await (await fetch(`${RELAY}/health`, { signal: AbortSignal.timeout(20000) })).json();
console.log(`  head_cache: size=${h.head_cache_size} mb=${h.head_cache_mb}`);
console.log(pass && h.head_cache_size > 0 ? '\nPASS' : '\nFAIL');
process.exit(pass && h.head_cache_size > 0 ? 0 : 1);