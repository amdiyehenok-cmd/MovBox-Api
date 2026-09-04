# MovBox API

Thin relay for the MovBox Android app. Pure Node.js, no Puppeteer/Chrome, ~50MB RSS.

## What it does

Proxies the boxmovies.org `h5-api` and serves MP4 bytes back to the device with the right upstream headers so the device can browse, search, and download.

| Endpoint | Method | Purpose |
|---|---|---|
| `/health` | GET | Liveness + RSS/cache/uptime |
| `/stream` | GET | Stream URLs + captions for a given subject (movie or TV episode) |
| `/mp4?url=...` | GET | Stream the MP4 binary with `Range` support (used by the Android download manager) |
| `/subtitle?url=...` | GET | Serve the .srt/.vtt caption with proper content-type |
| `/search` | POST | Proxy to upstream `/subject/search` with Bearer auth — lets the device search the whole boxmovies DB |

## Run locally

```bash
npm start
# → movbox-stream listening on http://0.0.0.0:4000
```

## Env vars

| Var | Default | Notes |
|---|---|---|
| `PORT` | `4000` | HTTP port |
| `HOSTNAME` | `0.0.0.0` | Bind address |
| `PUBLIC_HOST` | (empty) | **Set this in production** to your public URL, e.g. `https://movbox-api.onrender.com`. The relay uses it to build absolute `/mp4` and `/subtitle` URLs in the `/stream` JSON response. |

## Deploy

The repo ships a `Dockerfile` and a `render.yaml` blueprint. Fastest path:

1. Push to GitHub
2. Render → New → Blueprint → connect this repo
3. Render detects `render.yaml` and provisions the service
4. Once live, set `PUBLIC_HOST` to the rendered URL in the service's env

Other targets that work as-is: fly.io (`fly launch` will pick up the Dockerfile), Railway, any Docker host.

## Notes

- The Bearer token in `server.mjs` was captured from a real boxmovies.org session. When it expires the device will get 401s from `/stream` and `/search` — rotate by re-loading boxmovies.org in Chrome and pasting a fresh `Authorization: Bearer …` value.
- The relay is contacted only ~5-30s per movie watch (browse, resolve, download, caption). The 2hr video bytes go CDN → device directly. So even 256MB RAM handles hundreds of concurrent users.
- `/search` is the only endpoint that accepts a request body. It is POST because the upstream h5-api requires a JSON body.
