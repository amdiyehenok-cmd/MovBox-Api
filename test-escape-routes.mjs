// Two escape routes that do NOT involve the current method:
//  A) another CDN host that does not enforce the same allowlist
//  B) the size cap the upstream itself imposes (playConfig.maxResolution)
// Then: what does 480p actually cost vs 1080p through the relay?
const HOST = 'https://boxmovies.org';
const H5 = 'https://h5-api.aoneroom.com/wefeed-h5api-bff';
const UA = 'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36';

const r0 = await fetch(`${H5}/app/get-latest-app-pkgs?app_name=moviebox`, {
  headers: { Origin: HOST, Referer: `${HOST}/`, 'User-Agent': UA, Accept: 'application/json, text/plain, */*' },
});
const token = (r0.headers.get('set-cookie') || '').match(/(?:^|,\s*)token=([^;]+)/i)?.[1];

async function play(detailPath, subjectId, se = 1, ep = 1) {
  const r = await fetch(`${H5}/subject/play?subjectId=${subjectId}&se=${se}&ep=${ep}&detailPath=${detailPath}`, {
    headers: {
      Origin: HOST, Referer: `${HOST}/movies/${detailPath}?id=${subjectId}&type=/tv/detail&se=${se}&ep=${ep}&lang=en`,
      'User-Agent': UA, 'x-client-info': '{"timezone":"Africa/Nairobi"}', 'x-request-lang': 'en',
      Accept: 'application/json, text/plain, */*', Authorization: `Bearer ${token}`,
    },
  });
  return r.json();
}

const j = await play('crystal-lake-q12XEyhcad', '178208782890193416');
const streams = j?.data?.streams || [];
const ref = streams.find((s) => s.url);
const refPath = new URL(ref.url).pathname;
console.log(`upstream playConfig: ${JSON.stringify(j?.data?.playConfig)}`);
console.log(`streams: ${streams.map((s) => `${s.resolutions}/${(Number(s.size) / 1048576).toFixed(0)}MB`).join('  ')}\n`);

console.log('=== A) SAME PATH ON OTHER CDN HOSTS, Referer = movbox.xyz ===');
const hosts = ['bcdnxw.hakunaymatata.com', 'pacdn.aoneroom.com', 'macdn.aoneroom.com', 'pbcdnw.aoneroom.com', 'spa.aoneroom.com', 'pbcdn.aoneroom.com'];
const qs = ref.url.split('?')[1];
for (const h of hosts) {
  const u = `https://${h}${refPath}${qs ? '?' + qs : ''}`;
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), 10000);
  try {
    const r = await fetch(u, { headers: { Referer: 'https://movbox.xyz/', 'User-Agent': UA, Range: 'bytes=0-262143' }, signal: ac.signal });
    let n = 0; try { n = (await r.arrayBuffer()).byteLength; } catch (_) {}
    console.log(`  ${h.padEnd(28)} ${String(r.status).padEnd(4)} ${String(n).padStart(7)}B  ${r.headers.get('content-range') || ''}`);
  } catch (e) {
    console.log(`  ${h.padEnd(28)} ERR  ${e.name}`);
  } finally { clearTimeout(to); }
}

console.log('\n=== B) SIZE AT EACH RESOLUTION (what the relay must carry) ===');
let total1080 = 0;
for (const s of streams) {
  const u = new URL(s.url);
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), 10000);
  const t0 = Date.now();
  try {
    const r = await fetch(u.href, { headers: { Referer: `${HOST}/`, 'User-Agent': UA, Range: 'bytes=0-1048575' }, signal: ac.signal });
    let n = 0; try { n = (await r.arrayBuffer()).byteLength; } catch (_) {}
    const mb = Number(s.size) / 1048576;
    const oneMovie = mb;
    console.log(`  ${String(s.resolutions).padEnd(5)} ${mb.toFixed(0).padStart(6)}MB   1MB probe in ${Date.now() - t0}ms (${n}B)`);
    if (Number(s.resolutions) > 480) total1080 += oneMovie;
  } catch (e) { console.log(`  ${s.resolutions} ERR ${e.name}`); }
  finally { clearTimeout(to); }
}
console.log(`\n  Capping at <=480 avoids ${(total1080).toFixed(0)}MB of 1080p per title.`);