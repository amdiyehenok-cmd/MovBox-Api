// Live end-to-end test of the patched relay.
//  - /stream  : full ladder, correct order, correct fields
//  - /mp4     : Range pass-through, 206 + content-range, warm/cold timing
//  - /subtitle: SRT -> VTT conversion
//  - /health  : no crash after removing the chunk cache

const RELAY = process.env.RELAY || 'http://127.0.0.1:4000';
const DETAIL = 'the-florist-efahwcdehu5';
const SUBJECT = '4606371260002628112';

const raw = (proxied) =>
  decodeURIComponent((new URL(proxied).searchParams.get('url') || ''));

const mb = (n) => (n / 1048576).toFixed(2) + ' MB';

async function timeIt(label, fn) {
  const t0 = Date.now();
  const r = await fn();
  const ms = Date.now() - t0;
  console.log(`  ${label}: ${ms}ms`);
  return { r, ms };
}

(async () => {
  let pass = 0, fail = 0;
  const ok = (cond, msg) => {
    if (cond) { pass++; console.log(`  PASS  ${msg}`); }
    else { fail++; console.log(`  FAIL  ${msg}`); }
  };

  // ── /health ──────────────────────────────────────────────────────────
  console.log('\n=== /health ===');
  const h = await fetch(`${RELAY}/health`).then((r) => r.json());
  console.log('  ' + JSON.stringify(h));
  ok(h.ok === true, 'health returns ok');
  ok(h.chunk_cache_size === undefined, 'no chunk-cache fields left in /health');
  ok(h.max_resolution === 0, 'max_resolution is 0 (unlimited)');

  // ── /stream ─────────────────────────────────────────────────────────
  console.log('\n=== /stream ===');
  const s = await fetch(
    `${RELAY}/stream?detailPath=${DETAIL}&subjectId=${SUBJECT}&season=0&episode=0`
  ).then((r) => r.json());
  const streams = s.data?.streams || [];
  console.log('  ladder: ' + streams.map((x) => `${x.resolutions}p/${mb(x.size)}`).join('  '));
  ok(s.ok === true, 'stream ok');
  ok(streams.length === 3, `3 rungs returned (got ${streams.length})`);
  ok(
    streams.map((x) => Number(x.resolutions)).join(',') === '360,480,1080',
    'ladder is ascending: 360,480,1080'
  );
  ok(s.data.maxResolution === 1080, 'maxResolution reports 1080');
  ok(streams.every((x) => x.quality), 'every rung has a quality label');
  ok(streams.every((x) => x.sizeBytes > 0), 'every rung has a byte size');
  ok(streams.every((x) => x.url.includes('/mp4?url=')), 'every rung is proxied via /mp4');
  ok((s.data.captions || []).length > 0, `captions present (${(s.data.captions || []).length})`);

  // ── /mp4 Range pass-through ─────────────────────────────────────────
  console.log('\n=== /mp4 range pass-through (360p, the auto-played rung) ===');
  const smallest = streams[streams.length - 1]; // ascending => last is 1080
  const auto = streams[0];
  const url = `${RELAY}/mp4?url=${encodeURIComponent(raw(auto.url))}`;

  const r1 = await fetch(url, { headers: { Range: 'bytes=0-1048575' } });
  const b1 = Buffer.from(await r1.arrayBuffer());
  const cr1 = r1.headers.get('content-range');
  console.log(`  status=${r1.status}  content-range=${cr1}  bytes=${b1.length}  ct=${r1.headers.get('content-type')}`);
  ok(r1.status === 206, 'range request returns 206');
  ok(/^bytes 0-1048575\/\d+$/.test(cr1 || ''), `content-range carries the real total (${cr1})`);
  ok(r1.headers.get('accept-ranges') === 'bytes', 'accept-ranges: bytes');
  ok(r1.headers.get('access-control-allow-origin') === '*', 'CORS: *');
  ok(b1.length === 1048576, 'exactly 1MB returned');
  ok(b1.subarray(4, 8).toString('latin1') === 'ftyp', 'bytes are a real MP4 (ftyp box)');

  // second identical request = warm edge
  const t0 = Date.now();
  const r2 = await fetch(url, { headers: { Range: 'bytes=0-1048575' } });
  const b2 = Buffer.from(await r2.arrayBuffer());
  const warm = Date.now() - t0;
  console.log(`  warm repeat: ${warm}ms (${mb(b2.length)} in ${warm}ms)`);
  ok(r2.status === 206, 'warm repeat still 206');

  // mid-file seek
  const mid = 100000000;
  const r3 = await fetch(url, { headers: { Range: `bytes=${mid}-${mid + 1048575}` } });
  const b3 = Buffer.from(await r3.arrayBuffer());
  console.log(`  seek: status=${r3.status} content-range=${r3.headers.get('content-range')} bytes=${b3.length}`);
  ok(r3.status === 206, 'mid-file seek returns 206');
  ok(
    (r3.headers.get('content-range') || '').startsWith(`bytes ${mid}-`),
    'seek content-range starts at the requested offset'
  );
  ok(b3.length === 1048576, 'seek returns the full 1MB');

  // no-range request — read the headers only, then drop the body. The
  // full file is 235MB; we do not want to pull it down to prove a
  // status code.
  const r4 = await fetch(url);
  console.log(`  no-range: status=${r4.status} ct=${r4.headers.get('content-type')} len=${r4.headers.get('content-length')}`);
  await r4.body?.cancel();
  ok(r4.status === 200 || r4.status === 206, 'no-range request is answered (200/206)');

  // open-ended range — headers only
  const r5 = await fetch(url, { headers: { Range: `bytes=${mid + 20000000}-` } });
  console.log(`  open-ended: status=${r5.status} content-range=${r5.headers.get('content-range')}`);
  ok(r5.status === 206, 'open-ended range returns 206');
  await r5.body?.cancel();

  // ── host allowlist ──────────────────────────────────────────────────
  console.log('\n=== security: host allowlist ===');
  const bad = await fetch(
    `${RELAY}/mp4?url=${encodeURIComponent('https://example.com/evil.mp4')}`
  );
  ok(bad.status === 403, `non-allowlisted host rejected (got ${bad.status})`);

  // ── /subtitle -> VTT ────────────────────────────────────────────────
  console.log('\n=== /subtitle (SRT -> VTT) ===');
  const capUrl = s.data.captions?.[0]?.url;
  if (capUrl) {
    const inner = raw(capUrl);
    const sv = await fetch(capUrl);
    const text = await sv.text();
    console.log(`  status=${sv.status}  ct=${sv.headers.get('content-type')}  bytes=${text.length}`);
    console.log('  first line: ' + text.split('\n')[0]);
    ok(sv.status === 200, 'subtitle fetched');
    ok(
      (sv.headers.get('content-type') || '').includes('text/vtt'),
      'served as text/vtt'
    );
    ok(text.startsWith('WEBVTT'), 'body starts with the WEBVTT magic header');
    ok(!/\n\d{2}:\d{2}:\d{2},\d{3}\n/.test(text), 'SRT comma timestamps converted to dots');
    // raw srt still available
    const rawSrt = await fetch(`${capUrl}&format=srt`).catch(() => null);
    if (rawSrt) {
      const rt = await rawSrt.text();
      console.log(`  raw srt: ct=${rawSrt.headers.get('content-type')} startsWith=${rt.slice(0, 6)}`);
    }
    void inner;
  } else {
    fail++; console.log('  FAIL  no caption url to test');
  }

  console.log(`\n=== RESULT: ${pass} passed, ${fail} failed ===`);
  process.exit(fail ? 1 : 0);
})().catch((e) => {
  console.error('TEST CRASHED:', e);
  process.exit(1);
});
