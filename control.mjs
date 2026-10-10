/**
 * Control experiment.
 *
 * OLD (pre-streaming-rewrite) server on 4002 vs NEW (fixed) server on 4001,
 * same upstream URL, same moment in time. If BOTH hang, the fault is not the
 * streaming rewrite. If only NEW hangs, it is.
 */
const NEW = 'http://127.0.0.1:4001';
const OLD = 'http://127.0.0.1:4002';
const PROD = 'https://p01--movbox-api--d6kx854d4rvy.code.run';

const sj = await (await fetch(`${PROD}/stream?detailPath=breaking-bad-ej6Bp0MCAo7&subjectId=6207982430134357800&season=5&episode=1`)).json();
const pick = sj.data.streams[0];
const upstream = decodeURIComponent(/\/mp4\?url=([^&]+)/.exec(pick.url)[1]);
const enc = encodeURIComponent(upstream);
const BUDGET = 20000;

async function go(label, url, range) {
  const t0 = Date.now();
  let ttfb = null, status = null;
  try {
    const r = await fetch(url, { headers: range ? { Range: range } : {}, signal: AbortSignal.timeout(BUDGET) });
    ttfb = Date.now() - t0; status = r.status;
    const reader = r.body.getReader();
    let got = 0; const first = [];
    while (got < 256 * 1024) {
      const { value, done } = await reader.read();
      if (done) break;
      if (!first.length) first.push(value.subarray(0, 8).toString('hex'));
      got += value.length;
    }
    try { await reader.cancel(); } catch {}
    console.log(`${label.padEnd(30)} status=${status} got=${(got/1024).toFixed(0).padStart(5)}KB ttfb=${ttfb}ms total=${Date.now()-t0}ms first8=${first[0]||'-'} x-cache=${r.headers.get('x-cache')}`);
    return true;
  } catch (e) {
    console.log(`${label.padEnd(30)} ${ttfb === null ? 'NO-HEADERS' : `status=${status}`} -> ${e.name} after ${Date.now()-t0}ms`);
    return false;
  }
}

console.log('=== upstream CDN direct (control: is the origin healthy at all?) ===');
await go('upstream direct 256KB', upstream, 'bytes=0-262143');
await go('upstream direct 256KB #2', upstream, 'bytes=0-262143');

console.log('\n=== OLD relay code (pre-rewrite, the version that worked on Oct 7) ===');
await go('OLD :4002 256KB', `${OLD}/mp4?url=${enc}`, 'bytes=0-262143');
await go('OLD :4002 256KB #2', `${OLD}/mp4?url=${enc}`, 'bytes=0-262143');

console.log('\n=== NEW relay code (streaming rewrite + socket fix) ===');
await go('NEW :4001 256KB', `${NEW}/mp4?url=${enc}`, 'bytes=0-262143');
await go('NEW :4001 256KB #2', `${NEW}/mp4?url=${enc}`, 'bytes=0-262143');

console.log('\n=== NEW relay: 3MB moov range, then repeat (head cache) ===');
await go('NEW moov 3MB cold', `${NEW}/mp4?url=${enc}`, 'bytes=0-3145727');
await go('NEW moov 3MB warm', `${NEW}/mp4?url=${enc}`, 'bytes=0-3145727');

console.log('\n=== NEW relay: 12 sequential requests (pool-poisoning stress) ===');
let ok = 0;
for (let i = 1; i <= 12; i++) {
  const t0 = Date.now();
  try {
    const r = await fetch(`${NEW}/mp4?url=${enc}`, { headers: { Range: 'bytes=0-65535' }, signal: AbortSignal.timeout(15000) });
    const b = await r.arrayBuffer();
    if (r.status === 206 && b.byteLength === 65536) { ok++; process.stdout.write('.'); }
    else process.stdout.write('x');
  } catch { process.stdout.write('H'); }
}
console.log(`\n  ${ok}/12 succeeded`);