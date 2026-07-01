# PREVIOUS.md

Running log of what was built here and why, so any human or agent can pick up cleanly.

## Session 1 — Plan 0, Phase 0E: scaffold the two real-time services

Executed the `mytheus-realtime` half of **Phase 0E** of `mytheus-docs/plan/plan-0-scaffold-and-contracts.md`. (The `mytheus-ui` half of 0E was deliberately NOT touched — active work is ongoing there, per an explicit instruction from the maintainer. This repo's two services stand up independently.)

Two services in two stacks that scale on different signals (`realtime-gateway` on A6 = 100k connections, `transcription-service` on A5 = 20k streams) — kept fully decoupled, no shared process, no shared in-memory state. Scaffold only: both boot and answer `/healthz`; the live Deepgram/Redpanda/Socket.io flows land in the realtime band.

### `services/realtime-gateway/` (Node + Socket.io + TypeScript)

- `package.json` — deps `socket.io`, `@socket.io/redis-adapter`, `ioredis`; dev `typescript`, `@types/node`. Scripts: `build` (tsc), `start` (`node dist/server.js`), `typecheck`.
- `tsconfig.json` — `strict`, CommonJS/ES2022, `outDir dist`, `noUnusedLocals/Parameters`.
- `src/server.ts` — an HTTP server exposing `/healthz` with a Socket.io server attached, wired to the **Redis adapter** for cross-instance fan-out. `ioredis` uses `lazyConnect` + `error` handlers so the process (and `/healthz`) stays up even when Redis is down in dev; it reconnects when Redis returns. No Redpanda consume yet.
- `src/keys.ts` — `pubsubChannel(orgId, meetingId)` returns the **verbatim canonical** channel `nexus:{org_id}:pubsub:meeting:{meeting_id}` (`_canon/02-data-model.md` §5). Frozen names are built here, never inline.
- `.env.example` — `PORT`, `REDIS_URL`, `REDPANDA_BROKERS`, `SCHEMA_REGISTRY_URL`. `.gitignore` for `node_modules/`, `dist/`, `.env`.

### `services/transcription-service/` (Python + FastAPI)

- Standalone uv project (its own `.venv`), `src/`-layout package `transcription_service` (hatchling `packages = ["src/transcription_service"]`), Python 3.12 pinned. Deps: `fastapi`, `uvicorn`, `websockets`, `confluent-kafka[avro,schemaregistry]`.
- `src/transcription_service/main.py` — FastAPI app with `/healthz` and the internal WS route **`/internal/audio`** (the LiveKit server-side audio-hook target). Scaffold accepts then closes the WS so the transport contract exists; the Deepgram WSS proxy, interim/final split, `transcript_chunks` writes (finals only), and the `transcript-chunks` producer land in the realtime band. The Deepgram key stays server-side.
- `tests/test_healthz.py` — `/healthz` 200 + a WS handshake assertion. `.env.example` with `REDIS_URL`, `REDPANDA_BROKERS`, `SCHEMA_REGISTRY_URL`, `DATABASE_URL`, `DEEPGRAM_API_KEY`.

### Verification (all green)

- gateway: `npm install` (38 pkgs, 0 vulns) → `npm run build` compiles TS clean → boot smoke: `/healthz` → `200 {"status":"ok","service":"realtime-gateway"}`, unknown path → `404`, Redis adapter connected (smoke pointed at the isolated dev Redis on `:6380` so no other local project was disturbed).
- transcription-service: `ruff` clean, `black --check` clean, `mypy .` clean (`strict`, 2 files), `pytest -q` 2 passed; boot smoke: `/healthz` → `200`, `/internal/audio` WS accepted+closed.
