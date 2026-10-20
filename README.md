# MovBox API

Thin relay for the MovBox Android app. Pure Node.js, no Puppeteer/Chrome, ~50MB RSS.

## What it does

Proxies the boxmovies.org `h5-api` and serves MP4 bytes back to the device with the right upstream headers so the device can browse, search, and download.

| Endpoint | Method | Purpose |
|---|---|---|
| `/health` | GET | Liveness + RSS/cache/uptime |
| `/stream` | GET | Stream URLs + captions for a given subject (movie or TV episode) |
| `/mp4?url=...` | GET | Stream the MP4 binary. The client's `Range` header is forwarded verbatim and the upstream's `Content-Range` is passed straight back — a true pass-through, no buffering, no chunking. |
| `/subtitle?url=...` | GET | Serve the caption, converted SRT → WebVTT by default (`?format=srt` for the raw SRT). |
| `/search` | POST | Proxy to upstream `/subject/search` with Bearer auth — lets the device search the whole boxmovies DB |
| `/remint?path=...` | GET | Mint a fresh signed URL for a media path we minted earlier. The edge media hop calls this when the CDN answers 403 (expired signature) — it cannot mint one itself, it has no upstream token. |

## Run locally

```bash
npm start
# → movbox-stream listening on http://0.0.0.0:4000
```

## Smoke test

```bash
node test-relay.mjs
# 28 assertions: ladder shape, 206 + content-range, mid-file seek, SRT→VTT, host allowlist
```

`bench-hop.mjs` answers "is it the relay or the network?" — it times the same byte range
direct to the CDN and again through the relay. If the two numbers match, the relay is not
the bottleneck.

## Media path

The CDN demands a `Referer` the browser is not allowed to set. Measured live on
2026-10-20, against the media URL:

```
Referer: https://boxmovies.org/   -> 206, bytes flow
Referer: https://movbox.xyz/     -> 429 rejected
no Referer                       -> 429 rejected
Origin: <anything>               -> ignored, only Referer counts
```

So some server-side hop has to add that header. There are two of them, and
**where** it runs decides how fast playback starts:

| Hop | Throughput | Time to the 3.9 MB `moov` | Time to first frame |
|---|---|---|---|
| this relay (DigitalOcean) | 0.13 - 0.53 MB/s | 7 - 12 s | ~5x slower |
| Cloudflare Worker (edge) | 1.8 - 3.1 MB/s | ~1.8 s | baseline |

The files are faststart: `ftyp@0(32) moov@32(3.7-4.1 MB)`. The browser cannot
play a single frame until it has that whole index, so the hop's throughput IS
the startup time. At 1080p the stream needs 325 KB/s just to keep up, and the
relay path could not sustain it — that is the buffering.

**So: keep the API here, move the media to the edge.**

- This relay keeps `/stream` (it owns the upstream token) and `/subtitle`
  (a few KB).
- The Cloudflare Worker (`workers/stream-proxy/worker.js` in the app repo)
  does the media hop: it fetches the CDN directly with the right `Referer`
  and streams the bytes back. It calls this relay's `/remint` when a
  signature expires, and falls back to streaming through this relay if the
  CDN refuses Cloudflare's egress. Nothing there can take playback down.

Set `MEDIA_BASE=https://<worker>` and the `/stream` response hands out Worker
media links automatically. The app needs no change: it passes the URLs
through untouched.

## Env vars

| Var | Default | Notes |
|---|---|---|
| `PORT` | `4000` | HTTP port |
| `HOSTNAME` | `0.0.0.0` | Bind address |
| `PUBLIC_HOST` | (empty) | **Set this in production** to your public URL, e.g. `https://movbox-api.onrender.com`. The relay uses it to build absolute `/mp4` and `/subtitle` URLs in the `/stream` JSON response. |
| `MAX_RESOLUTION` | `0` | Ceiling on the quality ladder. `0` = unlimited (the default). Set e.g. `720` to cap it. The old hard-coded 480p cap is gone — the full 360/480/1080 ladder is served. |
| `UPSTREAM_MEDIA_SITE` | `https://boxmovies.org` | The `Referer`/`Origin` sent to the media CDN. Only `boxmovies.org` and `movibox.xyz` are allow-listed upstream — never point this at our own domain (it returns 429). |
| `MEDIA_BASE` | (empty) | Base URL used to build the `/mp4` links in the `/stream` response. Empty = the media bytes flow through this relay. Set it to the Cloudflare Worker (the edge media hop) so they do not. See **Media path** below. |
| `PREWARM` | `1` | Set `0` to switch the CDN edge warm-up off. |
| `PREWARM_BYTES` | `262144` | Head range fetched to warm the CDN edge. Was 2 MB, which cost ~15 s of this host's egress and competed with the stream the user was waiting for. |

## Deploy

The repo ships a `Dockerfile` and a `render.yaml` blueprint. Fastest path:

1. Push to GitHub
2. Render → New → Blueprint → connect this repo
3. Render detects `render.yaml` and provisions the service
4. Once live, set `PUBLIC_HOST` to the rendered URL in the service's env

Other targets that work as-is: fly.io (`fly launch` will pick up the Dockerfile), Railway, any Docker host.

## Notes

- **Auth is a stateless clock token, not a session.** `/subject/play` and `/subject/caption` accept `X-Client-Token: <ts>,<md5(reversed ts)>`, so there is no login, no cookie, no 90-day token expiry and no headless Chrome. The per-IP Bearer pool is only used for `/subject/search` and as a fallback.
- **The video bytes are re-served by a hop that adds the `Referer`** — currently the Cloudflare Worker (edge), with this relay as the fallback. The upstream CDN demands a `Referer` the browser cannot set (`Referer` is a forbidden header name for `fetch`), so something server-side has to add it. See **Media path** above for why that hop must not be this host.
- **The quality ladder is ascending and the smallest rung auto-plays.** `/stream` returns 360p → 480p → 1080p in that order, so index 0 is the fastest-starting, cheapest rung. A client can still pick any rung.
- `/search` is the only endpoint that accepts a request body. It is POST because the upstream h5-api requires a JSON body.
