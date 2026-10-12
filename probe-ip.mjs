/**
 * Same URL, same moment: relay running on MY IP (localhost:4001) vs the
 * deployed relay on Cloud Run's IP. Isolates IP reputation from code.
 */
const LOCAL = 'http://127.0.0.1:4001';
const PROD = 'https://p01--movbox-api--d6kx854d4rvy.code.run';

const sj = await (await fetch(`${PROD}/stream?detailPath=mobland-wWHa6Cu6fa5&subjectId=4335165664987072680&season=1&episode=1`)).json();
const s = sj.data.streams[0];
const up = decodeURIComponent(/\/mp4\?url=([^&]+)/.exec(s.url)[1]);
const enc = encodeURIComponent(up);
console.log(`source: ${s.resolutions}p  ${new URL(up).pathname}`);

async function go(label, url) {
  const t0 = Date.now();
  try {
    const r = await fetch(url, { headers: { Range: 'bytes=0-262143' }, signal: AbortSignal.timeout(22000) });
    const ttfb = Date.now() - t0;
    const b = Buffer.from(await r.arrayBuffer());
    console.log(`${label.padEnd(34)} ${r.status} ${b.byteLength}B ttfb=${ttfb}ms total=${Date.now() - t0}ms x-cache=${r.headers.get('x-cache')}`);
  } catch (e) {
    console.log(`${label.padEnd(34)} ${e.name} after ${Date.now() - t0}ms`);
  }
}

console.log('\n--- through LOCAL relay (my residential IP) ---');
await go('localhost:4001', `${LOCAL}/mp4?url=${enc}`);
await go('localhost:4001 again (head cache)', `${LOCAL}/mp4?url=${enc}`);

console.log('\n--- through DEPLOYED relay (Cloud Run egress IP) ---');
await go('Cloud Run prod', `${PROD}/mp4?url=${enc}`);
await go('Cloud Run prod again', `${PROD}/mp4?url=${enc}`);

const h = await (await fetch(`${PROD}/health`)).json();
console.log(`\nprod health: ram=${h.ram_mb}MB head=${h.head_cache_size}/${h.head_cache_mb}MB uptime=${h.uptime_s}s`);