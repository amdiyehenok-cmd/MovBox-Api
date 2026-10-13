// Regression test for the "plays for a few minutes then stalls" bug.
//
// Reproduces what an HTML5 <video> actually does: an initial bounded read,
// then successive ranges moving forward, then a seek. Every one of those
// used a DIFFERENT range against ONE signed URL upstream, which is what the
// CDN silently drops. It must now all come back, and quickly.
const RELAY = process.env.RELAY || 'http://127.0.0.1:4102';
const MB = 1048576;

const sj = await (await fetch(`${RELAY}/stream?detailPath=mobland-wWHa6Cu6fa5&subjectId=4335165664987072680&season=1&episode=1`, { signal: AbortSignal.timeout(40000) })).json();
const s0 = sj.data.streams[0];
const mp4 = s0.url.replace(/^https?:\/\/[^/]+/, RELAY);
console.log(`source ${s0.resolutions}p ${Math.round(s0.size / MB)}MB\n`);

async function grab(label, range, ms = 60000) {
  const t0 = Date.now();
  const ac = new AbortController();
  const kill = setTimeout(() => ac.abort(), ms);
  try {
    const res = await fetch(mp4, { headers: { Range: range }, signal: ac.signal });
    const b = Buffer.from(await res.arrayBuffer());
    const box = b.length >= 12 ? b.subarray(4, 8).toString('latin1') : '';
    console.log(`  ${label.padEnd(30)} ${res.status} ${(b.length / 1024).toFixed(0)}KB in ${((Date.now() - t0) / 1000).toFixed(1)}s cr=${res.headers.get('content-range')} x-cache=${res.headers.get('x-cache')} box=${JSON.stringify(box)}`);
    return b.length;
  } catch (e) {
    console.log(`  ${label.padEnd(30)} FAIL (${e.name})`);
    return 0;
  } finally { clearTimeout(kill); }
}

// What a player does: index first, then walk forward, then seek.
const steps = [
  ['index       0-2MB', 'bytes=0-2097151'],
  ['continue    2-4MB', 'bytes=2097152-4194303'],
  ['continue    4-6MB', 'bytes=4194304-6291455'],
  ['continue    6-8MB', 'bytes=6291456-8388607'],
  ['replay      0-2MB (cached)', 'bytes=0-2097151'],
  ['seek back   1-3MB', 'bytes=1048576-3145727'],
];

let ok = true;
for (const [label, range] of steps) {
  const n = await grab(label, range);
  if (!n) ok = false;
}

const h = await (await fetch(`${RELAY}/health`, { signal: AbortSignal.timeout(20000) })).json();
console.log(`\nchunk_cache=${h.chunk_cache_size} entries / ${h.chunk_cache_mb}MB`);
console.log(ok ? 'PASS — all ranges served' : 'FAIL — some ranges stalled');
process.exit(ok ? 0 : 1);