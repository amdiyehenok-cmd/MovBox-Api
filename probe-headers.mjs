/**
 * Does the upstream CDN actually require the boxmovies Origin/Referer?
 *
 * If a plain browser-shaped request works, the relay's entire reason to
 * exist disappears: the <video> element could fetch the CDN directly at
 * full speed, and we'd lose the region problem, the proxy bottleneck
 * and the whole class of "relay hangs" bugs.
 */
const RELAY = 'https://p01--movbox-api--d6kx854d4rvy.code.run';
const sj = await (await fetch(`${RELAY}/stream?detailPath=breaking-bad-ej6Bp0MCAo7&subjectId=6207982430134357800&season=5&episode=1`)).json();
const up = decodeURIComponent(/\/mp4\?url=([^&]+)/.exec(sj.data.streams[0].url)[1]);

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36';
const RANGE = { Range: 'bytes=0-65535' };

const cases = [
  ['bare, no headers at all', {}],
  ['UA only', { 'User-Agent': UA }],
  ['Referer: movbox.xyz', { 'User-Agent': UA, Referer: 'https://movbox.xyz/watch/tv/breaking-bad-ej6Bp0MCAo7?s=5&e=1' }],
  ['Referer: movibox.xyz', { 'User-Agent': UA, Referer: 'https://movibox.xyz/' }],
  ['Origin+Referer boxmovies', { 'User-Agent': UA, Origin: 'https://boxmovies.org', Referer: 'https://boxmovies.org/' }],
  ['Origin+Referer aoneroom', { 'User-Agent': UA, Origin: 'https://h5-api.aoneroom.com', Referer: 'https://h5-api.aoneroom.com/' }],
];

for (const [label, hdrs] of cases) {
  const t0 = Date.now();
  try {
    const r = await fetch(up, { headers: { ...RANGE, ...hdrs }, signal: AbortSignal.timeout(20000) });
    const b = Buffer.from(await r.arrayBuffer());
    const ok = r.status === 206 || r.status === 200;
    console.log(`${label.padEnd(28)} ${String(r.status).padEnd(4)} ${ok ? '*** WORKS ***' : 'blocked'}  ${b.byteLength}B in ${Date.now() - t0}ms`);
  } catch (e) {
    console.log(`${label.padEnd(28)} FAIL ${e.name} after ${Date.now() - t0}ms`);
  }
  await new Promise(r => setTimeout(r, 1500));
}