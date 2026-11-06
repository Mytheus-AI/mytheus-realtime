# syntax=docker/dockerfile:1
#
# realtime-gateway: Socket.io fan-out plane (port 8090).
#
# BUILD CONTEXT IS THE REPO ROOT, not this directory, because the TypeScript build roots at the
# repo root (tsconfig rootDir "../..") and compiles the shared ../../libs tree alongside src:
#   docker build -f services/realtime-gateway/Dockerfile -t mytheus-realtime-gateway .
#
# Stages: build (full deps + tsc) -> deps (production deps only) -> runtime (node + dist +
# prod node_modules, non-root, tini as PID 1). No dev tooling reaches the runtime image.

ARG NODE_IMAGE=node:22-slim

FROM ${NODE_IMAGE} AS build
WORKDIR /app
# libs/ has its own package.json (jose) and tsc resolves libs/*.ts imports from libs/node_modules,
# so it needs its own install next to the gateway's. Manifests first so the install layers cache.
COPY libs/package.json libs/package-lock.json libs/
COPY services/realtime-gateway/package.json services/realtime-gateway/package-lock.json services/realtime-gateway/
RUN --mount=type=cache,target=/root/.npm \
    npm ci --prefix libs && npm ci --prefix services/realtime-gateway
COPY libs libs
COPY services/realtime-gateway services/realtime-gateway
# Emits services/realtime-gateway/dist/{libs,services/realtime-gateway/src}/... (rootDir = repo root).
RUN npm run build --prefix services/realtime-gateway

FROM ${NODE_IMAGE} AS deps
WORKDIR /app/services/realtime-gateway
COPY services/realtime-gateway/package.json services/realtime-gateway/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --omit=dev

FROM ${NODE_IMAGE} AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/*
ENV NODE_ENV=production
# dist/libs/*.js resolves `jose` by walking up to this directory's node_modules, so the layout
# below (dist and node_modules as siblings) must be kept.
WORKDIR /app/services/realtime-gateway
COPY --from=deps --chown=node:node /app/services/realtime-gateway/node_modules ./node_modules
COPY --from=build --chown=node:node /app/services/realtime-gateway/dist ./dist
COPY --chown=node:node services/realtime-gateway/package.json ./package.json
USER node
EXPOSE 8090

# tini reaps zombies and forwards SIGTERM so socket.io connections drain on rollout.
ENTRYPOINT ["tini", "--"]
CMD ["node", "dist/services/realtime-gateway/src/server.js"]
