// Local A/B for the rebuilt relay: /stream -> head -> seek -> continuation.
const BASE = 'http://127.0.0.1:4000';
const DETAIL = 'crystal-lake-q12XEyhcad';
const SUBJECT = '178208782890193416';

// The relay builds absolute URLs from x-forwarded-proto, which defaults to
// https. Local test speaks plain http, so rewrite.
const local = (u) => u.replace(/^https:\/\/127\.0\.0\.1:4000/, BASE);

const s0 = Date.now();
const sr = await fetch(`${BASE}/stream?detailPath=${DETAIL}&subjectId=${SUBJECT}&season=1&episode=1`);
const sj = await sr.json();
console.log(`=== /stream === status=${sr.status} in ${Date.now() - s0}ms x-cache=${sr.headers.get('x-cache')}`);
console.log(`  ok=${sj.ok} streams=${sj.data?.streams?.length} captions=${sj.data?.captions?.length}`);
sj.data.streams.forEach((s, i) =>
  console.log(`   [${i}] id=${s.id} res=${s.resolutions} size=${(Number(s.size) / 1048576).toFixed(0)}MB codec=${s.codecName} dur=${s.duration}s`));

// Play the highest resolution, same as the relay's own pickBestStream.
const pick = sj.data.streams.reduce((b, s) =>
  (!b || Number(s.resolutions) > Number(b.resolutions) ? s : b), null);
const mp4 = local(pick.url);
const total = Number(pick.size);
console.log(`\n  playing res=${pick.resolutions} ${(total / 1048576).toFixed(0)}MB`);

async function range(label, rs, re) {
  const t0 = Date.now();
  const r = await fetch(mp4, { headers: { Range: `bytes=${rs}-${re}` } });
  let n = 0; const rd = r.body.getReader();
  for (;;) { const { done, value } = await rd.read(); if (done) break; n += value.length; }
  const ms = Date.now() - t0;
  console.log(`  ${label.padEnd(24)} ${String(r.status).padEnd(4)} ${(n / 1048576).toFixed(2).padStart(7)}MB ${String(ms).padStart(6)}ms = ${((n / 1048576) / (ms / 1000)).toFixed(2).padStart(7)} MB/s  x=${r.headers.get('x-cache')}`);
  return { n, ms };
}

console.log('\n=== range requests (prewarm runs in background from /stream) ===');
await new Promise((r) => setTimeout(r, 1200));
const a = await range('head 0-8MB', 0, 8388607);
const b = await range('replay head (cached)', 0, 8388607);
await range('continue 8-32MB', 8388608, 33554431);
await range('seek mid-file', Math.floor(total * 0.5), Math.floor(total * 0.5) + 8388607);
await range('seek 75%', Math.floor(total * 0.75), Math.floor(total * 0.75) + 8388607);
await range('open-ended near EOF', total - 1000, undefined);
await range('past EOF (416)', total + 10, total + 20);

const h = await (await fetch(`${BASE}/health`)).json();
console.log(`\n=== health === chunk_cache=${h.chunk_cache_size} entries / ${h.chunk_cache_mb}MB`);
console.log(`\nhead cold+warm total = ${a.ms + b.ms}ms for 2 passes (was 2900-6600ms per 2MB before)`);
