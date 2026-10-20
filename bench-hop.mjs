// Isolate relay overhead from network path.
// 1) get a signed CDN url from the relay
// 2) time a direct CDN GET (range 0-1MB) with the right Referer
// 3) time the same bytes through the relay
const RELAY = 'http://127.0.0.1:4000';
const SID = '4606371260002628112';
const DP = 'the-florist-efahwcdehu5';

async function j(u, o) { const r = await fetch(u, o); return { status: r.status, body: await r.json().catch(() => null) }; }

const st = await j(`${RELAY}/stream?subjectId=${SID}&se=0&ep=0&detailPath=${DP}`);
if (!st.body?.ok) { console.log('stream failed', JSON.stringify(st.body).slice(0, 400)); process.exit(1); }
const src = st.body.data?.streams?.[0] || st.body.streams?.[0];
const proxied = src?.url;
console.log('relay url:', proxied);
const cdn = decodeURIComponent(new URL(proxied).searchParams.get('url'));
console.log('cdn url :', cdn);

async function timed(label, url, headers) {
  const t0 = Date.now();
  const r = await fetch(url, { headers });
  const b = await r.arrayBuffer();
  const ms = Date.now() - t0;
  console.log(`${label}: status=${r.status} bytes=${b.byteLength} ms=${ms} -> ${(b.byteLength / 1024 / 1024 / (ms / 1000)).toFixed(2)} MB/s`);
  return ms;
}

for (let i = 0; i < 3; i++) {
  console.log(`--- round ${i + 1} ---`);
  await timed('direct CDN ', cdn, { Referer: 'https://boxmovies.org/', Range: 'bytes=0-1048575' });
  await timed('via relay  ', proxied, { Range: 'bytes=0-1048575' });
}
