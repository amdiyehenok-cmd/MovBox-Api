const RELAY = 'https://p01--movbox-api--d6kx854d4rvy.code.run';
const BOX = { Origin: 'https://boxmovies.org', Referer: 'https://boxmovies.org/' };

const targets = [
  ['BreakingBad 360', 'breaking-bad-ej6Bp0MCAo7', '6207982430134357800', 5, 1, 0],
  ['MobLand', 'mobland-wWHa6Cu6fa5', '4335165664987072680', 1, 1, 0],
  ['StrangerThings', 'stranger-things-wsymkZvcaU5', '4956801153355891744', 1, 1, 0],
];

for (const [label, dp, sid, se, ep, idx] of targets) {
  const sj = await (await fetch(`${RELAY}/stream?detailPath=${dp}&subjectId=${sid}&season=${se}&episode=${ep}`)).json();
  const streams = sj.data?.streams || [];
  console.log(`\n######## ${label} — ${streams.length} source(s) ########`);
  for (const s of streams.slice(idx, idx + 2)) {
    const up = decodeURIComponent(/\/mp4\?url=([^&]+)/.exec(s.url)[1]);
    const path = new URL(up).pathname;
    const tag = `${s.resolutions}p ${new URL(up).hostname}${path.slice(0, 28)}`;

    // 1) direct to the CDN with the headers the site itself sends
    let d = 'n/a';
    const t1 = Date.now();
    try {
      const r = await fetch(up, { headers: { Range: 'bytes=0-65535', ...BOX }, signal: AbortSignal.timeout(20000) });
      const b = Buffer.from(await r.arrayBuffer());
      d = `${r.status} ${b.byteLength}B in ${Date.now() - t1}ms`;
    } catch (e) { d = `FAIL ${e.name} ${Date.now() - t1}ms`; }

    // 2) through the relay
    let g = 'n/a';
    const t2 = Date.now();
    try {
      const r = await fetch(s.url, { headers: { Range: 'bytes=0-65535' }, signal: AbortSignal.timeout(25000) });
      const b = Buffer.from(await r.arrayBuffer());
      g = `${r.status} ${b.byteLength}B in ${Date.now() - t2}ms x-cache=${r.headers.get('x-cache')}`;
    } catch (e) { g = `FAIL ${e.name} ${Date.now() - t2}ms`; }

    console.log(`  ${tag}`);
    console.log(`     direct CDN : ${d}`);
    console.log(`     via relay  : ${g}`);
  }
}

const h = await (await fetch(`${RELAY}/health`)).json();
console.log(`\nhealth: ram=${h.ram_mb}MB head=${h.head_cache_size}/${h.head_cache_mb}MB meta=${h.cache_size} uptime=${h.uptime_s}s`);