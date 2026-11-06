# Mytheus - Real-time Tier (`mytheus-realtime`)

The WebSocket and streaming tier of Mytheus: live captions, live transcription, and the fan-out of agent output to every meeting participant.

## What this is

`mytheus-realtime` is the **real-time tier** of Mytheus - the part of the platform that holds long-lived WebSocket connections and moves low-latency signals in and out of a live meeting. It does two jobs that scale on two different signals:

- It **transcribes** live meetings - taps server-side audio from LiveKit, proxies it to Deepgram, and persists finalized utterances as `transcript_chunks`.
- It **fans out** real-time output - live captions, streamed agent token deltas, agent results, and presence - to the `web-app` and mobile clients over WebSockets, fanning across instances so any participant on any pod sees every event.

This tier carries **no media** (that is LiveKit's SFU) and owns **no durable domain data** beyond `transcript_chunks` - its other state is ephemeral connection and presence state held in Redis. Durable state lives in the CRUD and AI tiers.

## Services in this repo

| Service | Responsibility | Runtime | Scales on |
|---|---|---|---|
| `realtime-gateway` | WebSocket/Socket.io fan-out plane for non-AV signals: live captions, agent-sidebar output, streamed agent token deltas, presence, chat, priority alerts. Rooms keyed by `meeting_id`; fans across instances via Redis pub/sub; produces `presence-events`; owns ephemeral connection + presence state in Redis. | Node + Socket.io (Redis adapter) | Concurrent WebSocket connections (anchor `A6` = 100k peak participants) |
| `transcription-service` | FastAPI WS proxy: receives the LiveKit server-side audio hook over an internal WS, proxies to Deepgram over WSS (key server-side), separates interim vs final, persists **only finals** to `transcript_chunks`, produces `transcript-chunks` (Avro), emits live captions to `realtime-gateway`, consumes `meeting-room-events` to start streams. | FastAPI (Python) WS proxy | Concurrent live streams (anchor `A5` = 20k meetings) |

The split is deliberate: `transcription-service` scales on **stream count** (`A5` = 20k meetings, one diarized stream each) while `realtime-gateway` scales on **connection count** (`A6` = 100k participant connections). They sit in the same real-time scaling family but on opposite anchors, so each gets its own autoscaler signal.

## The three WebSocket flows

1. **Audio in.** The LiveKit server-side audio hook streams mixed room audio to `transcription-service` over an internal WS; `transcription-service` proxies it to Deepgram over WSS, keeping the Deepgram key server-side.
2. **Captions out.** Deepgram interims drive live captions immediately; Deepgram finals fan out through `realtime-gateway` to `web-app`/mobile within the 2-second caption budget. Only finals are persisted to `transcript_chunks` and produced to the `transcript-chunks` topic.
3. **Agent output out.** `agent-stream` token deltas and `agent-results` are pushed by `realtime-gateway` to the client agent sidebar; Socket.io rooms are keyed by `meeting_id` and fanned across instances via the `nexus:{org_id}:pubsub:meeting:{meeting_id}` Redis pub/sub channel.

## Tech stack

- **`realtime-gateway`** - Node + Socket.io with the Redis adapter for cross-instance fan-out; Redis Cluster pub/sub backbone (channel `nexus:{org_id}:pubsub:meeting:{meeting_id}`); presence sets in Redis; `aiokafka`/Kafka-API client for Redpanda consume (`agent-stream`, `agent-results`, `notifications`) and `presence-events` produce.
- **`transcription-service`** - FastAPI WS proxy; Deepgram streaming SDK over WSS; LiveKit server-side audio hook (internal WS in); `aiokafka` for Redpanda; produces `transcript-chunks` (Avro + Schema Registry).
- **Shared** - Redpanda (Kafka API, Avro + Schema Registry), Redis (pub/sub + presence), OpenTelemetry for tracing/metrics.

## Repo structure

```
mytheus-realtime/
├── services/
│   ├── realtime-gateway/        # Node + Socket.io fan-out plane
│   └── transcription-service/   # Python (FastAPI) Deepgram WS proxy
├── proto/                       # shared event/Avro schemas & contracts
├── libs/                        # shared helpers (tenancy, envelope, Redis keys)
├── README.md
├── system.md
├── CLAUDE.md
└── .gitignore
```

## Quick start

**Prerequisites**

- Node 20+
- Python 3.11+
- Docker (Compose) for local Redis + Redpanda
- LiveKit dev URL + API key/secret
- Deepgram dev API key (kept server-side)

**Run**

This repo has no compose file of its own: local Postgres (host port 5443), Redis (6390),
Redpanda (9092) and Schema Registry (8081) come from `mytheus-backend`'s `docker compose`.

```bash
# 1. Local infra, from the mytheus-backend checkout (Postgres 5443, Redis 6390, Redpanda 9092)
(cd ../mytheus-backend && docker compose up -d)

# 2. Shared libs + realtime-gateway (Node) on port 8090
cd libs && npm install && cd ..
cd services/realtime-gateway
cp .env.example .env            # PORT=8090, CORE_API_URL=http://localhost:8002 (core-api)
npm install
npm run build && npm start      # http://localhost:8090
cd ../..

# 3. transcription-service (Python, uv) on port 8091
cd services/transcription-service
cp .env.example .env            # set INTERNAL_AUDIO_TOKEN and TRANSCRIPTION_ALLOW_FAKE_PROVIDER=1
uv sync
uv run alembic upgrade head     # transcript_chunks + its grant (after mytheus-backend's migrations)
uv run uvicorn transcription_service.main:app --reload --port 8091
# liveness:  curl localhost:8091/healthz
# readiness: curl localhost:8091/readyz   (db / redis / broker)

# 4. Checks (or `make ci` from the repo root)
make lint typecheck test
```

## transcription-service `/internal/audio` contract

The server-side audio hook (`meeting-service`'s audio tap) connects to
`ws://<transcription-service>:8091/internal/audio`. The authoritative definition is the module
docstring of `services/transcription-service/src/transcription_service/audio_in.py`; this is
its summary.

1. One connection PER PARTICIPANT AUDIO TRACK; a meeting has several concurrent connections.
   No query parameters.
2. Handshake header `X-Internal-Auth: <INTERNAL_AUDIO_TOKEN>` (constant-time compare). Missing,
   wrong, or the server's `INTERNAL_AUDIO_TOKEN` unset: the upgrade is refused (HTTP 403).
3. First frame: a text frame with one JSON object, all keys required:
   `{"org_id": "<uuid>", "meeting_id": "<uuid>", "participant_identity": "<users.id>",
   "sample_rate": 16000, "encoding": "pcm_s16le"}`. Must arrive within 10 s. Resent on every
   reconnect.
4. Then binary frames of raw PCM, 16 kHz mono s16le, even length, at most 65536 bytes each
   (about 10 ms per frame from the sender).
5. Later text frames are JSON control only, never audio: `{"type": "stop"}` flushes queued
   audio and closes 1000; anything else is ignored.
6. Each connection gets its own provider stream (`diarize=false`). Everything it produces is
   attributed to its `participant_identity`, persisted as `transcript_chunks.speaker_id` (and
   the `transcript-chunks` `speaker_id`, and the caption `speakerId`), which the existing
   `meeting_speakers` mapping resolves. No schema change.
7. Supersede key is `(org_id, meeting_id, participant_identity)`: a reconnect by the same
   participant closes only that participant's old connection (4409). Other participants keep
   streaming. The meeting session (org check, region/redaction settings, audit entry, captions
   room) closes when its last participant connection ends.

Close codes: 1000 normal (stop / stream ended), 1001 server shutting down (reconnect),
1007 odd-length frame, 1009 frame too large, 1011 internal error or no provider configured
(reconnect with backoff), 4400 bad init frame, 4403 meeting not in org, 4408 init or idle
timeout (30 s), 4409 superseded by the same participant's newer connection, 4451 compliance
gate closed (audio dropped, nothing emitted, do not reconnect).

While the compliance gate is closed (`COMPLIANCE_GATE_GREEN = False`), any connection served
by a real (non-fake) provider is closed 4451 right after the init frame, before any database
access or provider contact. With no `DEEPGRAM_API_KEY`, the fake provider is used only when
`TRANSCRIPTION_ALLOW_FAKE_PROVIDER=1` is set explicitly (local development); otherwise the
service fails closed and refuses every connection with 1011.

## Environment variables

| Variable | Service | Purpose |
|---|---|---|
| `DEEPGRAM_API_KEY` | transcription-service | Deepgram WSS key - **stays server-side, never sent to a client** |
| `LIVEKIT_URL` | both | LiveKit server URL for the audio hook / token validation |
| `LIVEKIT_API_KEY` / `LIVEKIT_API_SECRET` | transcription-service | Validate the server-side audio hook handshake |
| `REDIS_URL` | both | Redis pub/sub + presence (keys prefixed `nexus:{org_id}:…`) |
| `REDPANDA_BROKERS` | both | Kafka-API bootstrap brokers |
| `SCHEMA_REGISTRY_URL` | both | Avro Schema Registry for `transcript-chunks` / `presence-events` |
| `WS_BIND_HOST` / `WS_BIND_PORT` | realtime-gateway | WebSocket listener bind config |
| `TRANSCRIPTION_WS_BIND` | transcription-service | Internal WS bind for the LiveKit audio hook (default `0.0.0.0:8091`) |
| `INTERNAL_AUDIO_TOKEN` | transcription-service | Shared secret the audio sender presents in `X-Internal-Auth`; unset refuses every connection |
| `AUDIO_INIT_TIMEOUT_SECONDS` / `AUDIO_IDLE_TIMEOUT_SECONDS` | transcription-service | `/internal/audio` init (10 s) and idle (30 s) timeouts |
| `AUDIO_MAX_FRAME_BYTES` / `AUDIO_QUEUE_MAX_FRAMES` | transcription-service | Max PCM frame size (65536) and backpressure queue depth (100 frames) |
| `TRANSCRIPTION_ALLOW_FAKE_PROVIDER` | transcription-service | `1` lets the fake provider run when no Deepgram key is set (local dev only); default off = fail closed |
| `DATABASE_URL` | transcription-service | Shared Postgres (`transcript_chunks`, `outbox`, tenant reads) |
| `PORT` | realtime-gateway | Gateway listen port (default `8090`) |
| `CORE_API_URL` | realtime-gateway | `core-api` base URL (dev: `http://localhost:8002`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | both | OpenTelemetry traces/metrics export |

## Testing

- **`realtime-gateway`** - Node test runner for unit/integration; **load/soak tests** for WS fan-out (many concurrent connections per `meeting_id` room, cross-pod Redis pub/sub delivery, reconnect storms, backpressure/dropped-frame behavior).
- **`transcription-service`** - `pytest` for the Deepgram proxy, interim-vs-final separation, finals-only persistence, and `transcript-chunks` production; soak tests for many concurrent live streams.
- CI runs both stacks plus WS load tests before any image is built.

## Related repos

- `mytheus-realtime` - this repo: the real-time tier (`realtime-gateway`, `transcription-service`).
- `mytheus-control-plane` - CRUD/control-plane tier (identity, meeting, billing, integration, admin-ops, notification, core-api).
- `mytheus` - AI/inference tier (agent-runtime, embedding, reranker, rag, ingestion, knowledge-graph, search).
- `mytheus-workflows` - async/orchestration (Temporal workers).
- `mytheus-web` - `web-app` (Next.js 14) and the agent sidebar surface.
- `mytheus-gateway` - `api-gateway` / `bff` edge (WAF, rate limit, JWT, `org_id` injection).
- `mytheus-infra` - Kubernetes (EKS/GKE) + Terraform IaC, deployment.
- `mytheus-docs` - architecture canon and the frozen `_canon/` naming authority.

## License

Proprietary © Mytheus. All rights reserved.
