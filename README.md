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

## Env vars

| Var | Default | Notes |
|---|---|---|
| `PORT` | `4000` | HTTP port |
| `HOSTNAME` | `0.0.0.0` | Bind address |
| `PUBLIC_HOST` | (empty) | **Set this in production** to your public URL, e.g. `https://movbox-api.onrender.com`. The relay uses it to build absolute `/mp4` and `/subtitle` URLs in the `/stream` JSON response. |
| `MAX_RESOLUTION` | `0` | Ceiling on the quality ladder. `0` = unlimited (the default). Set e.g. `720` to cap it. The old hard-coded 480p cap is gone — the full 360/480/1080 ladder is served. |
| `UPSTREAM_MEDIA_SITE` | `https://boxmovies.org` | The `Referer`/`Origin` sent to the media CDN. Only `boxmovies.org` and `movibox.xyz` are allow-listed upstream — never point this at our own domain (it returns 429). |

## Deploy

The repo ships a `Dockerfile` and a `render.yaml` blueprint. Fastest path:

1. Push to GitHub
2. Render → New → Blueprint → connect this repo
3. Render detects `render.yaml` and provisions the service
4. Once live, set `PUBLIC_HOST` to the rendered URL in the service's env

Other targets that work as-is: fly.io (`fly launch` will pick up the Dockerfile), Railway, any Docker host.

## Notes

- **Auth is a stateless clock token, not a session.** `/subject/play` and `/subject/caption` accept `X-Client-Token: <ts>,<md5(reversed ts)>`, so there is no login, no cookie, no 90-day token expiry and no headless Chrome. The per-IP Bearer pool is only used for `/subject/search` and as a fallback.
- **The relay carries the video bytes.** The upstream CDN demands a `Referer` the browser cannot set (`Referer` is a forbidden header name for `fetch`), so the relay adds `Referer: https://boxmovies.org/` and streams the response through. The device and the web app therefore download from the relay, not from the CDN. The relay buffers nothing, so its memory use is flat regardless of file size.
- **The quality ladder is ascending and the smallest rung auto-plays.** `/stream` returns 360p → 480p → 1080p in that order, so index 0 is the fastest-starting, cheapest rung. A client can still pick any rung.
- `/search` is the only endpoint that accepts a request body. It is POST because the upstream h5-api requires a JSON body.
