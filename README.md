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

```bash
# 1. Bring up local infra (Redis + Redpanda + Schema Registry)
docker compose up -d

# 2. realtime-gateway (Node)
cd services/realtime-gateway
npm install
npm run dev

# 3. transcription-service (Python)
cd services/transcription-service
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
uvicorn app.main:app --reload --port 8081

# 4. Smoke test
npm test                       # gateway
pytest                         # transcription-service
```

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
| `TRANSCRIPTION_WS_BIND` | transcription-service | Internal WS bind for the LiveKit audio hook |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | both | OpenTelemetry traces/metrics export |

## Container images

Two images, built from the **repo root** (one root `.dockerignore` keeps every env file out of both). CI (`.github/workflows/images.yml`) builds them on every PR and pushes `:main` and `:<git sha>` to GHCR on merge to `main`.

| Image | Dockerfile | Port | Runs |
|---|---|---|---|
| `ghcr.io/mytheus-ai/mytheus-realtime-gateway` | `services/realtime-gateway/Dockerfile` | 8090 | `node dist/services/realtime-gateway/src/server.js` (Node 22, user `node`) |
| `ghcr.io/mytheus-ai/mytheus-transcription` | `services/transcription-service/Dockerfile` | 8091 | `uvicorn transcription_service.main:app --host 0.0.0.0 --port 8091` (Python 3.12, uid 10001) |

```bash
docker build -f services/realtime-gateway/Dockerfile -t mytheus-realtime-gateway .
docker build -f services/transcription-service/Dockerfile -t mytheus-transcription .

# Config comes from the environment only (see Environment variables); nothing is baked in.
docker run --rm -p 8090:8090 -e REDIS_URL=... -e REDPANDA_BROKERS=... -e SCHEMA_REGISTRY_URL=... -e CLERK_JWKS_URL=... mytheus-realtime-gateway
docker run --rm -p 8091:8091 -e DATABASE_URL=... -e REDIS_URL=... -e REDPANDA_BROKERS=... -e SCHEMA_REGISTRY_URL=... mytheus-transcription

# One-off migration of transcript_chunks from the same image (not run by the compose today):
docker run --rm -e DATABASE_URL=... mytheus-transcription alembic upgrade head
```

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
