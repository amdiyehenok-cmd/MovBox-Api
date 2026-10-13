// Definitive test: on a REAL media path, which (Origin, Referer) combos get a
// real 206 from the CDN, and how fast? Run from THIS machine (residential IP).
const RELAY = 'https://p01--movbox-api--d6kx854d4rvy.code.run';

const sj = await (await fetch(`${RELAY}/stream?detailPath=mobland-wWHa6Cu6fa5&subjectId=4335165664987072680&season=1&episode=1`)).json();
const s480 = sj.data.streams.find((s) => s.resolutions === 480) || sj.data.streams[0];
const up = decodeURIComponent(/\/mp4\?url=([^&]+)/.exec(s480.url)[1]);
console.log('upstream URL host:', new URL(up).host);
console.log('upstream path    :', new URL(up).pathname);
console.log();

const combos = [
  { label: 'no referer, no origin', headers: {} },
  { label: 'Referer movibox.xyz', headers: { Referer: 'https://movibox.xyz/' } },
  { label: 'Referer movibox.xyz + Origin movibox.xyz', headers: { Referer: 'https://movibox.xyz/', Origin: 'https://movibox.xyz' } },
  { label: 'Referer boxmovies.org', headers: { Referer: 'https://boxmovies.org/' } },
  { label: 'Referer boxmovies.org + Origin boxmovies.org', headers: { Referer: 'https://boxmovies.org/', Origin: 'https://boxmovies.org' } },
  { label: 'Referer movbox.xyz', headers: { Referer: 'https://movbox.xyz/' } },
  { label: 'Referer movbox.xyz + Origin movbox.xyz', headers: { Referer: 'https://movbox.xyz/', Origin: 'https://movbox.xyz' } },
];

for (const c of combos) {
  const t0 = Date.now();
  try {
    const r = await fetch(up, {
      headers: { ...c.headers, Range: 'bytes=0-262143' },
      signal: AbortSignal.timeout(20000),
    });
    const b = Buffer.from(await r.arrayBuffer());
    console.log(
      `${String(r.status).padEnd(4)} ttfb=${String(Date.now() - t0).padStart(6)}ms bytes=${String(b.byteLength).padStart(7)}  ${c.label}`,
    );
  } catch (e) {
    console.log(`FAIL ttfb=${String(Date.now() - t0).padStart(6)}ms   ${c.label}  -> ${e.message}`);
  }
}

// And the CDN's own view: what does a blocked response body actually say?
try {
  const r = await fetch(up, { headers: { Range: 'bytes=0-1023', Referer: 'https://movbox.xyz/' }, signal: AbortSignal.timeout(20000) });
  const t = await r.text();
  console.log('\n--- blocked body (movbox.xyz referer), status', r.status, '---');
  console.log(t.slice(0, 600));
} catch (e) { console.log('blocked-body read failed:', e.message); }