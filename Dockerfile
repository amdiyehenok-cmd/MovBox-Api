# Multi-stage would be overkill for ~12KB of JS. Use a slim node base.
FROM node:20-alpine

# wget is handy for HEALTHCHECK and curl-style debugging inside the container.
RUN apk add --no-cache wget

WORKDIR /app

# Copy package files first so the layer caches when only server.mjs changes.
COPY package.json ./
COPY server.mjs ./

# No npm install needed — server.mjs only uses node built-ins. Keep the
# image as small as possible. If you add deps later, uncomment:
# COPY package-lock.json* ./
# RUN npm ci --omit=dev

ENV NODE_ENV=production
ENV PORT=4000
ENV HOSTNAME=0.0.0.0
# PUBLIC_HOST is set in the platform's env (Render/etc) at runtime so
# proxied /mp4 and /subtitle URLs are absolute and resolvable by the
# device. Example: https://movbox-api.onrender.com

EXPOSE 4000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:4000/health > /dev/null || exit 1

CMD ["node", "server.mjs"]
