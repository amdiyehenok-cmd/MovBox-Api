// Decisive throughput test: is the signature single-use PER CHUNK, or per URL?
// And how fast can we go with parallel mint + parallel chunk fetch?
const HOST = 'https://boxmovies.org';
const H5 = 'https://h5-api.aoneroom.com/wefeed-h5api-bff';
const UA = 'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36';
const DETAIL = 'crystal-lake-q12XEyhcad';
const SUBJECT = '178208782890193416';
const CHUNK = 262144;

const hdrs = (token, referer) => ({
  Origin: HOST, Referer: referer, 'User-Agent': UA,
  'x-client-info': '{"timezone":"Africa/Nairobi"}', 'x-request-lang': 'en',
  Accept: 'application/json, text/plain, */*', Authorization: `Bearer ${token}`,
});

const r0 = await fetch(`${H5}/app/get-latest-app-pkgs?app_name=moviebox`, {
  headers: { Origin: HOST, Referer: `${HOST}/`, 'User-Agent': UA, Accept: 'application/json, text/plain, */*' },
});
let token = (r0.headers.get('set-cookie') || '').match(/(?:^|,\s*)token=([^;]+)/i)?.[1];
if (!token) { console.log('no token'); process.exit(1); }

async function mint() {
  const referer = `${HOST}/movies/${DETAIL}?id=${SUBJECT}&type=/tv/detail&se=1&ep=1&lang=en`;
  let r = await fetch(`${H5}/subject/play?subjectId=${SUBJECT}&se=1&ep=1&detailPath=${DETAIL}`, { headers: hdrs(token, referer) });
  if (r.status === 401 || r.status === 403) {
    const rr = await fetch(`${H5}/app/get-latest-app-pkgs?app_name=moviebox`, {
      headers: { Origin: HOST, Referer: `${HOST}/`, 'User-Agent': UA, Accept: 'application/json, text/plain, */*' },
    });
    token = (rr.headers.get('set-cookie') || '').match(/(?:^|,\s*)token=([^;]+)/i)?.[1];
    r = await fetch(`${H5}/subject/play?subjectId=${SUBJECT}&se=1&ep=1&detailPath=${DETAIL}`, { headers: hdrs(token, referer) });
  }
  const j = await r.json();
  const s = (j?.data?.streams || [])[0];
  if (!s?.url) throw new Error('no stream: ' + JSON.stringify(j).slice(0, 200));
  return s.url;
}

// Range request against the CDN with the boxmovies Referer.
const CDN_HDRS = {
  Referer: `${HOST}/`, Origin: HOST, 'User-Agent': UA,
};
async function grab(url, start, len, timeoutMs = 20000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  const t0 = Date.now();
  try {
    const r = await fetch(url, { headers: { ...CDN_HDRS, Range: `bytes=${start}-${start + len - 1}` }, signal: ac.signal });
    const tt = Date.now() - t0;
    if (!r.ok) return { ok: false, status: r.status, ms: tt, bytes: 0 };
    const buf = Buffer.from(await r.arrayBuffer());
    return { ok: true, status: r.status, cr: r.headers.get('content-range'), ms: tt, bytes: buf.length, buf };
  } catch (e) {
    return { ok: false, ms: Date.now() - t0, bytes: 0, err: e.name };
  } finally { clearTimeout(t); }
}

console.log('=== TEST A: ONE minted URL, two sequential chunks ===');
const u1 = await mint();
const a0 = await grab(u1, 0, CHUNK);
console.log(` chunk0 ${a0.ok ? 'OK' : 'FAIL'} status=${a0.status} bytes=${a0.bytes} ms=${a0.ms} cr=${a0.cr}`);
const a1 = await grab(u1, CHUNK, CHUNK);
console.log(` chunk1 SAME-URL ${a1.ok ? 'OK' : 'FAIL'} status=${a1.status} bytes=${a1.bytes} ms=${a1.ms}`);

console.log('\n=== TEST B: FRESH signature per chunk ===');
const u2 = await mint();
const b1 = await grab(u2, CHUNK, CHUNK);
console.log(` chunk1 FRESH-URL ${b1.ok ? 'OK' : 'FAIL'} status=${b1.status} bytes=${b1.bytes} ms=${b1.ms} cr=${b1.cr}`);
const u3 = await mint();
const b2 = await grab(u3, CHUNK * 2, CHUNK);
console.log(` chunk2 FRESH-URL ${b2.ok ? 'OK' : 'FAIL'} status=${b2.status} bytes=${b2.bytes} ms=${b2.ms} cr=${b2.cr}`);

console.log('\n=== TEST C: mint cost (5 sequential) ===');
for (let i = 0; i < 5; i++) {
  const t0 = Date.now();
  try { await mint(); console.log(`  mint#${i} ${Date.now() - t0}ms`); }
  catch (e) { console.log(`  mint#${i} ERR ${e.message}`); }
}

console.log('\n=== TEST D: PARALLEL mint(N) then parallel fetch(N) ===');
for (const N of [8, 24]) {
  const tm = Date.now();
  let urls;
  try { urls = await Promise.all(Array.from({ length: N }, mint)); }
  catch (e) { console.log(` N=${N} mint fail ${e.message}`); continue; }
  const mintMs = Date.now() - tm;
  const tf = Date.now();
  const res = await Promise.all(urls.map((u, i) => grab(u, i * CHUNK, CHUNK)));
  const fetchMs = Date.now() - tf;
  const okBytes = res.reduce((a, r) => a + (r.ok ? r.bytes : 0), 0);
  const okCount = res.filter((r) => r.ok).length;
  console.log(` N=${N}: mint ${mintMs}ms (${(mintMs / N).toFixed(0)}ms each) | fetch ${fetchMs}ms | ok ${okCount}/${N} | ${(okBytes / 1048576).toFixed(2)} MB | total ${((okBytes / 1048576) / ((mintMs + fetchMs) / 1000)).toFixed(2)} MB/s`);
}