// THE PERMANENT-FIX TEST.
// If the CDN serves a signed URL to a Referer it does NOT know (or to none),
// then <video src="signed-url"> works natively in the browser and the entire
// byte-proxying relay can be deleted. That removes 2.6GB-per-session through a
// 512MB free-tier box, which is the only way this is actually fixed.
const HOST = 'https://boxmovies.org';
const H5 = 'https://h5-api.aoneroom.com/wefeed-h5api-bff';
const UA = 'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36';
const DETAIL = 'crystal-lake-q12XEyhcad';
const SUBJECT = '178208782890193416';

const r0 = await fetch(`${H5}/app/get-latest-app-pkgs?app_name=moviebox`, {
  headers: { Origin: HOST, Referer: `${HOST}/`, 'User-Agent': UA, Accept: 'application/json, text/plain, */*' },
});
const token = (r0.headers.get('set-cookie') || '').match(/(?:^|,\s*)token=([^;]+)/i)?.[1];
const play = await (await fetch(`${H5}/subject/play?subjectId=${SUBJECT}&se=1&ep=1&detailPath=${DETAIL}`, {
  headers: {
    Origin: HOST, Referer: `${HOST}/movies/${DETAIL}?id=${SUBJECT}&type=/tv/detail&se=1&ep=1&lang=en`,
    'User-Agent': UA, 'x-client-info': '{"timezone":"Africa/Nairobi"}', 'x-request-lang': 'en',
    Accept: 'application/json, text/plain, */*', Authorization: `Bearer ${token}`,
  },
})).json();
const low = (play?.data?.streams || []).find((s) => Number(s.resolutions) <= 480) || play?.data?.streams?.[0];
console.log(`target: res=${low.resolutions} size=${(Number(low.size) / 1048576).toFixed(0)}MB`);
const url = new URL(low.url);
console.log(`host:   ${url.hostname}`);
console.log(`path:   ${url.pathname}\n`);

async function tryRef(label, headers) {
  const t0 = Date.now();
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), 15000);
  try {
    const r = await fetch(url.href, { headers: { 'User-Agent': UA, Range: 'bytes=0-262143', ...headers }, signal: ac.signal });
    const n = Number(r.headers.get('content-length') || 0);
    let got = 0;
    try { const b = await r.arrayBuffer(); got = b.byteLength; } catch (_) {}
    console.log(`  ${label.padEnd(34)} ${String(r.status).padEnd(4)} ${String(got).padStart(7)}B  ${Date.now() - t0}ms  ${r.headers.get('content-range') || ''}`);
    return { status: r.status, got };
  } catch (e) {
    console.log(`  ${label.padEnd(34)} ERR  ${Date.now() - t0}ms  ${e.name}`);
    return { status: 0, got: 0 };
  } finally { clearTimeout(to); }
}

console.log('=== REFERER MATRIX on the signed URL ===');
const cases = [
  ['boxmovies.org (current hack)', { Referer: 'https://boxmovies.org/', Origin: HOST }],
  ['movbox.xyz (OUR domain)', { Referer: 'https://movbox.xyz/' }],
  ['movibox.xyz (allow-listed)', { Referer: 'https://movibox.xyz/' }],
  ['NO Referer at all', {}],
  ['Origin-only, no Referer', { Origin: HOST }],
  ['spurious.example (unknown)', { Referer: 'https://spurious.example/' }],
];
const results = {};
for (const [label, h] of cases) results[label] = await tryRef(label, h);

console.log('\n=== VERDICT ===');
const native = results['NO Referer at all']?.status === 206 || results['spurious.example (unknown)']?.status === 206 || results['movbox.xyz (OUR domain)']?.status === 206;
console.log(native
  ? '  >>> CDN DOES NOT ENFORCE REFERER for this URL. The browser can load it\n      directly. The byte-proxying relay can be deleted. THIS IS THE FIX.'
  : '  >>> CDN DOES enforce Referer. Only the allow-listed domain works, so the\n      proxy stays until the supplier allowlists movbox.xyz.');