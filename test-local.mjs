/**
 * Local end-to-end test of the reworked relay.
 * Boots nothing — expects the server already listening on $PORT.
 * Run: node test-local.mjs
 */
const PORT = process.env.TEST_PORT || 4001;
const LOCAL = `http://127.0.0.1:${PORT}`;
const PROD = 'https://p01--movbox-api--d6kx854d4rvy.code.run';

async function timeFetch(label, url, headers, bytes = 1048576) {
  const t0 = Date.now();
  try {
    const r = await fetch(url, { headers, signal: AbortSignal.timeout(60000) });
    const ttfb = Date.now() - t0;
    const b = Buffer.from(await r.arrayBuffer());
    const total = Date.now() - t0;
    const mbps = b.length / 1048576 / (total / 1000);
    const mb = b.length / 1048576;
    console.log(
      `${label.padEnd(30)} ${String(r.status).padEnd(4)} ${mb.toFixed(2).padStart(6)}MB ` +
        `ttfb=${String(ttfb).padStart(5)}ms total=${String(total).padStart(6)}ms ` +
        `${mbps.toFixed(2)} MB/s  x-cache=${r.headers.get('x-cache')}`
    );
    return { mbps, bytes: b.length };
  } catch (e) {
    console.log(`${label.padEnd(30)} FAIL ${Date.now() - t0}ms ${e.message}`);
    return { mbps: 0, bytes: 0 };
  }
}

// Get a fresh signed URL from production so an expired `t=` can't be
// mistaken for a regression.
const sr = await fetch(
  `${PROD}/stream?detailPath=breaking-bad-ej6Bp0MCAo7&subjectId=6207982430134357800&season=5&episode=1`
);
const sj = await sr.json();
const streams = sj.data?.streams || [];
if (!streams.length) {
  console.log('no streams from production:', JSON.stringify(sj).slice(0, 200));
  process.exit(1);
}
const upstream = decodeURIComponent(/\/mp4\?url=([^&]+)/.exec(streams[0].url)[1]);
console.log(`title streams=${streams.length}, first res=${streams[0].resolutions}\n`);

const localUrl = `${LOCAL}/mp4?url=${encodeURIComponent(upstream)}`;

console.log('=== local relay: 1MB stream (native https pipe) ===');
const a = await timeFetch('1MB first (cold)', localUrl, { Range: 'bytes=0-1048575' });

console.log('\n=== local relay: 1MB again (upstream socket now pooled) ===');
const b = await timeFetch('1MB second (warm agent)', localUrl, { Range: 'bytes=0-1048575' });

console.log('\n=== head cache: moov range, 3 requests ===');
for (let i = 1; i <= 3; i++) {
  await timeFetch(`moov bytes=0-3145727 #${i}`, localUrl, { Range: 'bytes=0-3145727' }, 3145728);
}

console.log('\n=== seek behaviour: mid-file range (must stream, not cache) ===');
await timeFetch('mid-file bytes=50000000-51048575', localUrl, { Range: 'bytes=50000000-51048575' });

console.log('\n=== correctness: byte-identical vs upstream? ===');
const rh = await fetch(upstream, {
  headers: { Range: 'bytes=0-65535', Origin: 'https://boxmovies.org', Referer: 'https://boxmovies.org/' },
});
const direct = Buffer.from(await rh.arrayBuffer());
const rl = await fetch(localUrl, { headers: { Range: 'bytes=0-65535' } });
const viaproxy = Buffer.from(await rl.arrayBuffer());
console.log(`direct ${direct.length}B  proxy ${viaproxy.length}B  identical=${direct.equals(viaproxy)}`);

console.log('\n=== correctness: Content-Range passthrough ===');
console.log(`upstream: ${rh.headers.get('content-range')}`);
console.log(`proxy:    ${rl.headers.get('content-range')}`);
console.log(`proxy accept-ranges: ${rl.headers.get('accept-ranges')}  ct: ${rl.headers.get('content-type')}`);

console.log('\n=== local /health ===');
const h = await (await fetch(`${LOCAL}/health`)).json();
console.log(JSON.stringify(h, null, 2));

console.log(`\nSUMMARY cold=${a.mbps.toFixed(2)} MB/s  warm=${b.mbps.toFixed(2)} MB/s`);