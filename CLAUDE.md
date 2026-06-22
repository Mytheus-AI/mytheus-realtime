# CLAUDE.md — `reprise-realtime`

Working guide for AI coding agents in this repo.

## Purpose

`reprise-realtime` is the **real-time tier** of Reprise AI: the WebSocket/streaming layer that transcribes live meetings and fans out live captions, streamed agent output, agent results, and presence to web and mobile clients. It scales on concurrent connections and live streams — not on request rate. It carries no media (LiveKit does), runs no LLM (the AI tier does), and is not a system of record for agent output (the durable truth lives in `core-api`). Two services live here: a Node `realtime-gateway` (fan-out) and a Python `transcription-service` (Deepgram WS proxy).

## Two-stack layout

```
services/realtime-gateway/        # Node + Socket.io + Redis adapter (fan-out plane)
services/transcription-service/   # Python FastAPI WS proxy → Deepgram
proto/                            # shared Avro/event schemas
libs/                             # shared helpers (tenancy, envelope, Redis keys)
```

- `realtime-gateway` — owns WebSocket connections, Socket.io rooms keyed by `meeting_id`, Redis pub/sub fan-out, presence; consumes `agent-stream`/`agent-results`/`notifications`; produces `presence-events`.
- `transcription-service` — owns the Deepgram proxy and `transcript_chunks`; consumes `meeting-room-events`; produces `transcript-chunks`.

## Stack & versions

| | `realtime-gateway` | `transcription-service` |
|---|---|---|
| Language | TypeScript, Node 20+ | Python 3.11+ (typed) |
| Framework | Socket.io (+ Redis adapter) | FastAPI WS proxy |
| External | Redis Cluster, Redpanda (Kafka API) | Deepgram SDK (WSS), LiveKit audio hook, Redpanda |
| Lint/format | ESLint + Prettier | ruff + black |
| Test | Node test runner + WS load/soak | pytest + stream soak |

## Cross-cutting rules

1. **`org_id` on every WS handshake and every message.** Resolve it from the Clerk-verified JWT (pre-validated at `api-gateway`), never from the client payload. Authz on room join: a socket may only join a `meeting_id` room within its own `org_id`. Fail closed if `org_id` is unresolved.
2. **Prefix every Redis key with `nexus:{org_id}:`.** Pub/sub `nexus:{org_id}:pubsub:meeting:{meeting_id}`, presence `nexus:{org_id}:presence:{meeting_id}`, priority queue `nexus:{org_id}:queue:agent:{user_id}`.
3. **Persist ONLY finals to `transcript_chunks`.** Interims drive captions only — never persisted, never on the bus. Only `is_final` results become a `transcript_chunks` row and a `transcript-chunks` event.
4. **The Deepgram key stays server-side.** Audio is proxied backend-to-backend; no client ever holds the key.
5. **Ephemeral state only.** Durable state lives elsewhere (Postgres, Redpanda). `realtime-gateway` holds only connection/presence state in Redis; `transcription-service` writes only `transcript_chunks`. A pod may be lost and replaced with no data loss.
6. **At-least-once + idempotency on produced events.** Use the transactional outbox; put a stable idempotency key in the envelope (`transcript-chunks` → `chunk:{chunk_id}`). Partition key is always `org_id`.

## Naming authority

Topic, channel, table, and schema names come from the `reprise-docs` `_canon/` folder — the frozen source of truth. **Keep these verbatim; never invent or rename:**

- Tables: `transcript_chunks`
- Topics: `transcript-chunks`, `presence-events`, `meeting-room-events`, `agent-stream`, `agent-results`, `notifications`
- Redis channel: `nexus:{org_id}:pubsub:meeting:{meeting_id}` (and the `nexus:{org_id}:` prefix on all keys)

If a name seems missing or wrong, it changes in `_canon/` first — do not introduce an alternative here.

## Do

- Keep `realtime-gateway` and `transcription-service` decoupled — they scale on different anchors (`A6` connections vs `A5` streams).
- Drop `agent-stream` token deltas first under backpressure; never drop captions or confirmed agent output.
- Re-hydrate caption/agent history from `core-api` on reconnect (finals are durable).
- Put `org_id`, `meeting_id`, `correlation_id` on every structured log line.

## Don't

- Don't persist interims, or emit them to the bus.
- Don't send the Deepgram key, LiveKit secret, or any provider key to a client.
- Don't hold durable domain state on a real-time pod.
- Don't produce `usage-events` from `transcription-service` — minutes are metered by `billing-metering-service`.
- Don't treat streamed agent output as a system of record.
- Don't read `org_id` from a request/message body.

## Run / test

```bash
# realtime-gateway (Node)
cd services/realtime-gateway && npm install && npm run dev
npm run lint && npm test           # includes WS fan-out load tests

# transcription-service (Python)
cd services/transcription-service && pip install -e ".[dev]"
uvicorn app.main:app --reload --port 8081
ruff check . && black --check . && pytest
```

## PR conventions

- Conventional commits; trunk-based with short-lived branches and PR review.
- CI must pass: lint + test both stacks, Avro BACKWARD compatibility gate, WS load tests.
- **Commits in this project must NOT include a `Co-Authored-By` footer.**
