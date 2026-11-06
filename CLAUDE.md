# CLAUDE.md

This is `mytheus-realtime`, the real-time tier of Mytheus: a Node/Socket.io `realtime-gateway` (WebSocket fan-out) and a Python/FastAPI `transcription-service` (LiveKit audio hook to Deepgram). The single most important thing: these are two services that scale on different signals and stay decoupled, only Deepgram finals persist to `transcript_chunks` (interims never do), and the Deepgram key stays server-side.

## Repo at a glance

The real-time tier owns the live path of a Mytheus meeting: server-side audio capture, Deepgram transcription, and the fan-out of live captions and streamed agent output to the participant's browser or mobile client. It holds the platform's long-lived WebSocket connections and its only live audio proxy.

It contains exactly two services, in two separate containers, because they scale on different and non-interchangeable signals:

- `realtime-gateway` scales on anchor `A6` = 100,000 concurrent WebSocket connections (one per participant).
- `transcription-service` scales on anchor `A5` = 20,000 concurrent live streams (one diarized Deepgram session per meeting).

20,000 meetings carry roughly 100,000 connections, so the anchors are not interchangeable. Coupling them in one process would force one to over-provision for the other's load. Separate containers with separate KEDA scalers is the structural fix. Keep them independent: no shared process, no shared in-memory state.

This tier carries no media (LiveKit's SFU does), mints no LiveKit tokens and takes no LiveKit webhooks (that is `meeting-service` in the control-plane tier), runs no LLM (the AI tier does), and is not the system of record for agent output (durable truth is `agent-results` and confirmed `decisions`/`actions`, owned by `core-api`).

## Architecture orientation

Three WebSocket flows pass through this tier. Socket.io rooms are keyed by `meeting_id` and fanned across instances via the `nexus:{org_id}:pubsub:meeting:{meeting_id}` Redis channel: a connection for a meeting may land on any gateway pod, but a bus event arrives at one pod, so that pod publishes on the channel and every subscribed pod emits to its local sockets in the room.

1. Audio in. The LiveKit server-side audio hook delivers mixed room audio to `transcription-service` over an internal WS. The service opens a single Deepgram WebSocket session per meeting and proxies the audio over WSS, keeping the Deepgram key server-side. Audio never crosses the backend trust boundary to reach Deepgram.

2. Live captions out. Deepgram interims drive captions immediately (low-latency, mutable, never persisted, never on the bus). Deepgram finals are the durable truth: each is persisted to `transcript_chunks`, produced to the `transcript-chunks` topic, and also emitted as a caption. Captions reach clients through `realtime-gateway` within the budget of p95 < 2 s (Deepgram-bound, no LLM in this path).

3. Agent output out. `agent-stream` token deltas and the final `agent-results` message are consumed from Redpanda by `realtime-gateway` and pushed over WebSocket to the client agent sidebar. Streaming is a UX optimization, never a system of record: a client that misses every delta still gets the final `agent-results` card.

## Service map

| | `realtime-gateway` (Node/Socket.io) | `transcription-service` (Python/FastAPI) |
|---|---|---|
| Job | WebSocket fan-out plane for every non-AV signal: captions, agent output, presence, chat, priority alerts | Deepgram WS proxy: server-side audio in, ASR with diarization, interim vs final separation |
| Scales on | `A6` = 100k concurrent connections | `A5` = 20k concurrent live streams |
| State | Ephemeral only: connection/presence sets and per-user queue in Redis; nothing durable | Writes only `transcript_chunks` (Postgres); otherwise stateless |
| Produces | `presence-events` | `transcript-chunks` (only finals, `TranscriptChunk` Avro) |
| Consumes | `agent-stream`, `agent-results`, `notifications` | `meeting-room-events` (start/stop Deepgram streams on LiveKit lifecycle) |
| Rooms / keys | Socket.io rooms keyed by `meeting_id`; fan-out via `nexus:{org_id}:pubsub:meeting:{meeting_id}` | One Deepgram session per `meeting_id` |
| Persists | Nothing durable | One `transcript_chunks` row per final, merged by speaker turn |

`transcript_chunks` columns: `org_id`, `meeting_id`, `speaker_id`, `start_ms`, `end_ms`, `text`, `is_final`, `confidence`. The diarized `speaker_id` is emitted as-is; the `speaker_id` to `user_id` mapping is resolved downstream via `meeting_speakers`, never guessed at write time. `transcription-service` does not produce `usage-events`: transcription minutes are metered by `billing-metering-service` in the control-plane tier.

## Where things live

Two-stack layout:

```
services/realtime-gateway/        # Node + Socket.io + Redis adapter: the fan-out plane
services/transcription-service/   # Python FastAPI WS proxy: LiveKit audio hook to Deepgram
proto/                            # shared event / Avro schemas and contracts
libs/                             # shared helpers: tenancy, envelope, Redis key builders
README.md  system.md  CLAUDE.md
```

- `services/realtime-gateway` (Node): Socket.io server, Redis adapter for cross-instance fan-out, presence sets, Redpanda consumers for `agent-stream`/`agent-results`/`notifications`, `presence-events` producer.
- `services/transcription-service` (Python): the internal WS endpoint for the LiveKit audio hook, the Deepgram WSS client, interim/final handling, `transcript_chunks` writes, the `transcript-chunks` producer, the `meeting-room-events` consumer.
- `proto/`: Avro schemas and event contracts shared across the repo. Changes here must be BACKWARD-compatible and land in `_canon/` first.
- `libs/`: shared cross-cutting helpers, especially the `nexus:{org_id}:` Redis key builders and the event envelope. Reuse these rather than hand-building keys or envelopes.

## Invariants you must not break

| Rule | Why | If you break it |
|---|---|---|
| `org_id` on every WS handshake and message, resolved from the Clerk-verified JWT (pre-validated at `api-gateway`), never from the client payload | Multi-tenancy is enforced per connection; authz on room join means a socket may only join a `meeting_id` room within its own `org_id` | Cross-tenant data leak; a socket joins another org's meeting. Fail closed if `org_id` cannot be resolved |
| Prefix every Redis key with `nexus:{org_id}:` | Per-tenant isolation by channel namespace; a noisy tenant's traffic, eviction, or flush cannot reach another tenant's sockets | Cross-tenant fan-out; one tenant's events land on another's connections |
| Persist ONLY finals to `transcript_chunks` | Interims are mutable and provisional; only `is_final` results are durable truth | Duplicated, contradictory, and unstable transcript rows; downstream consumers ingest garbage |
| The Deepgram key never leaves the server | Audio is proxied backend-to-backend; no client should ever hold a provider key | Credential exfiltration; anyone can run Deepgram on your account |
| Ephemeral state only on real-time pods | Any pod can serve any meeting; losing a pod loses only live sockets, which reconnect with no data loss because durable facts are in Postgres/Redpanda | Pod loss becomes data loss; scaling and rollouts become unsafe |
| At-least-once plus idempotency on produced events | Redelivery is expected; a stable idempotency key makes redelivery harmless | Double-writes. `transcript-chunks` uses `chunk:{chunk_id}` so a redelivered final never double-writes |
| Caption latency p95 < 2 s (p99 < 3 s) | This is the live-captions SLO and a user-visible promise | Captions lag the speaker; the live experience breaks |

## Naming authority

Topic, channel, table, and schema names are frozen in `mytheus-docs` `_canon/`, the source of truth. Keep these verbatim, never invent or rename:

- Tables: `transcript_chunks`
- Topics: `transcript-chunks`, `presence-events`, `meeting-room-events`, `agent-stream`, `agent-results`, `notifications`
- Redis channel: `nexus:{org_id}:pubsub:meeting:{meeting_id}` (and the `nexus:{org_id}:` prefix on every key)
- Anchors: `A5` (20k streams), `A6` (100k connections)

If a name seems missing or wrong, it changes in `_canon/` first. Do not introduce an alternative in this repo.

## Common tasks (recipes)

### Add a new outbound realtime event type (gateway)
1. Confirm the topic/channel name exists in `_canon/`; if not, land it there first.
2. In `services/realtime-gateway`, add a Redpanda consumer (or extend an existing one) for the source topic.
3. Map the bus event to a Socket.io emit, publishing on `nexus:{org_id}:pubsub:meeting:{meeting_id}` so every pod fans out to its local room sockets. Use the `libs/` key builder; do not hand-build the channel.
4. Carry `org_id`, `meeting_id`, and `correlation_id` through to the emit and onto every log line.
5. Add a fan-out test proving the event reaches a socket on a different pod.

### Add a new Socket.io room or channel
1. Key rooms by `meeting_id`; namespace any new Redis key under `nexus:{org_id}:`.
2. Enforce authz on join: a socket may only join within its JWT-resolved `org_id`, deny by default.
3. Use the `libs/` Redis key builder so the prefix is never missed.

### Add a transcription event field (BACKWARD-safe schema)
1. Edit the `TranscriptChunk` Avro schema in `proto/`. Add the field with a default (optional/nullable) so it is BACKWARD-compatible; do not remove or retype existing fields.
2. In `services/transcription-service`, populate the field only on finals before producing to `transcript-chunks`.
3. Keep the `transcript_chunks` row and the Avro value in sync if the field is persisted.
4. Run the Avro BACKWARD compatibility gate locally before pushing.

### Handle a new meeting-room-events case
1. In `services/transcription-service`, extend the `meeting-room-events` consumer to handle the new LiveKit lifecycle case (for example joined / left / recording-ready).
2. Start or stop the per-meeting Deepgram stream accordingly; ensure idempotency so a redelivered event does not open a duplicate stream.
3. Add a test for the new case, including redelivery.

### Add a presence signal
1. Update the presence set `nexus:{org_id}:presence:{meeting_id}` in `services/realtime-gateway`.
2. Produce the change as `presence-events` (partitioned by `org_id`, envelope per the contract) for `core-api` analytics.
3. Keep presence ephemeral: it lives in Redis, not in any durable store.

## Testing

Per stack:
- `realtime-gateway` (Node): Node test runner for unit/integration. The canonical hard tests are WS fan-out load/soak: many concurrent connections per `meeting_id` room, cross-pod Redis pub/sub delivery, reconnect storms, and backpressure/dropped-frame behavior. Quality gates: ESLint + Prettier.
- `transcription-service` (Python): `pytest` for the Deepgram proxy, interim-vs-final separation, finals-only persistence, and `transcript-chunks` production; soak tests for many concurrent live streams. Quality gates: ruff + black.

Mock these in tests: LiveKit (the server-side audio hook / internal WS), Deepgram (the WSS ASR session, interims and finals), and Redis pub/sub (cross-pod fan-out). CI runs both stacks plus WS load tests and the Avro BACKWARD compatibility gate before any image is built.

## Local development

Both stacks plus local infra:

```bash
# 1. Local infra (Postgres 5443, Redis 6390, Redpanda 9092, Schema Registry 8081) comes from
#    mytheus-backend's compose; this repo has none of its own
(cd ../mytheus-backend && docker compose up -d)

# 2. realtime-gateway (Node 20+), port 8090
(cd libs && npm install)
cd services/realtime-gateway
npm install
npm run build && npm start        # PORT=8090; CORE_API_URL=http://localhost:8002
npm run lint && npm test          # test includes WS fan-out load tests

# 3. transcription-service (Python 3.11+, uv), port 8091
cd services/transcription-service
uv sync
uv run uvicorn transcription_service.main:app --reload --port 8091
uv run ruff check . && uv run black --check . && uv run mypy . && uv run pytest
```

Dev keys: a LiveKit dev URL + API key/secret (audio hook and validation) and a Deepgram dev API key (kept server-side).

Environment variables:

| Variable | Service | Purpose |
|---|---|---|
| `DEEPGRAM_API_KEY` | transcription-service | Deepgram WSS key. Stays server-side, never sent to a client |
| `LIVEKIT_URL` | both | LiveKit server URL for the audio hook / token validation |
| `LIVEKIT_API_KEY` / `LIVEKIT_API_SECRET` | transcription-service | Validate the server-side audio hook handshake |
| `REDIS_URL` | both | Redis pub/sub + presence (keys prefixed `nexus:{org_id}:`) |
| `REDPANDA_BROKERS` | both | Kafka-API bootstrap brokers |
| `SCHEMA_REGISTRY_URL` | both | Avro Schema Registry for `transcript-chunks` / `presence-events` |
| `WS_BIND_HOST` / `WS_BIND_PORT` | realtime-gateway | WebSocket listener bind config |
| `TRANSCRIPTION_WS_BIND` | transcription-service | Internal WS bind for the LiveKit audio hook |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | both | OpenTelemetry traces/metrics export |

## Pitfalls

- Persisting interims to `transcript_chunks` or putting them on the bus. Only `is_final` results are durable; interims drive captions and nothing else.
- Leaking the Deepgram key (or LiveKit secret, or any provider key) to a client. Audio is proxied backend-to-backend only.
- Missing `org_id` on the WS handshake, or reading it from a request/message body. Resolve it from the JWT and fail closed if absent.
- Holding durable domain state on a gateway pod. Keep state ephemeral and in Redis so pod loss never means data loss.
- Fan-out not crossing instances. If pub/sub on `nexus:{org_id}:pubsub:meeting:{meeting_id}` is misconfigured, a participant on another pod silently misses events. Always test cross-pod delivery.
- Reconnect storms. On reconnect, clients re-mint the LiveKit token and re-join the `meeting_id` room with exponential backoff; re-hydrate caption/agent history from `core-api` (finals are durable). Do not have reconnect logic hammer the gateway in lockstep.
- Dropping the wrong thing under backpressure. Shed `agent-stream` token deltas first; never drop captions or confirmed agent output.

## Working with other repos

- Consumes event schemas defined in `mytheus-events` (`mytheus-docs` `_canon/` is the naming authority).
- Produces `transcript-chunks`, consumed by `mytheus-intelligence` and `mytheus-backend`.
- Fans out `agent-stream` and `agent-results` from `mytheus-intelligence` to `mytheus-ui` and `mytheus-mobile` clients.
- Consumes `meeting-room-events` produced by `meeting-service` (control-plane); produces `presence-events` consumed by `core-api`.

## Commit and PR conventions

- Trunk-based development with short-lived branches and PR review.
- Conventional commits.
- CI must pass: lint + test on both stacks, the Avro BACKWARD compatibility gate, and WS load tests.
- Commits in this project must NOT include a `Co-Authored-By` footer.

## Progress documentation - single home

Every record of work done - after a session, a feature, a fix, or a band step - lands in ONE place: `mytheus-docs/work-done-till-now/`. Not in this repo.

- One file per plan band, covering every repo that band touches: `work-done-till-now/01-plan-0-scaffold-and-contracts.md`, then `02-plan-1-2-...`, and so on. Add to the current band's file; start a new file only when a new band starts.
- Write it comprehensively enough that another person, or their coding agent, can pick the work up cold: what was built, what was verified and by which exact command, and **why** each decision was taken. Record every deviation from the plan together with its rationale. A deviation with no recorded reason is a bug, not a decision.
- Read `mytheus-docs/work-done-till-now/README.md` first. It holds the required structure and the writing conventions.
- Do NOT create or extend a per-repo progress log. `PREVIOUS.md` in this repo is historical: leave it in place, do not add to it, and fold anything new into the band file.
- That folder **records**; it never **defines**. A new or changed name, number, or decision still goes canon-first (`_canon/`), then an ADR, then the reader docs. Cite them from the band file.
- Do not overcomplicate. Build the optimal, professional, simple working version first, and add complexity only when something concrete requires it.
