# Reprise AI - Real-time Tier: System Design (`reprise-realtime`)

Deep design doc for the real-time tier of Reprise AI. This is the implementation-facing companion to the canon documents `10-realtime-and-transcription.md`, `03-container-architecture.md`, and `07-event-architecture.md`. Where this doc names a topic, table, Redis channel, or anchor, the canon is authoritative and the name is kept verbatim.

---

## 1. Purpose & bounded context

The real-time tier owns the **live path** of a Reprise AI meeting: from server-side audio capture, through Deepgram transcription, to live captions and streamed agent output landing in the participant's browser/mobile client. It holds the platform's long-lived WebSocket connections and its only live audio proxy.

It contains exactly two services, and the boundary between them exists because they scale on **different signals** and must never be coupled:

| Service | Responsibility | Runtime | Autoscaler signal |
|---|---|---|---|
| `transcription-service` | Deepgram WS proxy: server-side audio in, interim/final separation, **only finals** persisted to `transcript_chunks`, produces `transcript-chunks`. | FastAPI WS proxy (Python) | Concurrent **live streams** - anchor `A5` = 20,000 peak meetings |
| `realtime-gateway` | Socket.io/WSS fan-out plane for non-AV signals: captions, agent output, presence, chat, priority alerts; Redis pub/sub backbone; produces `presence-events`. | Node + Socket.io + Redis adapter | Concurrent **WebSocket connections** - anchor `A6` = 100,000 peak participants |

Both services are stateless against durable stores: `transcription-service` writes only `transcript_chunks`; `realtime-gateway` writes nothing durable and holds only ephemeral connection/presence state in Redis.

What this tier does **not** do: it carries no media (LiveKit's SFU does), mints no LiveKit tokens and receives no LiveKit webhooks (that is `meeting-service` in the control-plane tier), runs no LLM (the AI tier does), and is not a system of record for agent output (the durable truth is `agent-results` and confirmed `decisions`/`actions`, owned by `core-api`).

---

## 2. The three WebSocket flows in detail

### 2.1 Audio in - LiveKit audio hook → `transcription-service` → Deepgram

Audio never leaves the backend trust boundary to reach Deepgram. The LiveKit **server-side audio hook** delivers mixed room audio to `transcription-service` over an internal WS. `transcription-service` opens a **single Deepgram WebSocket session per meeting** and proxies the audio over WSS, keeping the Deepgram API key server-side.

```
LiveKit SFU (server-side audio hook)
   │  internal WS (PCM)
   ▼
transcription-service  ──WSS (key server-side)──▶  Deepgram
   │ interims  → live captions (to realtime-gateway)
   │ finals    → transcript_chunks (Postgres) + transcript-chunks (Redpanda, Avro)
```

One diarized stream per meeting (mixed room audio, server-side subscription) is the locked cost lever - it is why the tier scales on `A5` = 20,000 streams, not on participant count.

### 2.2 Captions out - Deepgram finals → `realtime-gateway` → client

Deepgram **interims** drive live captions in the UI immediately (low-latency, mutable, never persisted, never on the bus). Deepgram **finals** are the durable truth: each final is persisted as a `transcript_chunks` row and produced to the `transcript-chunks` topic, and is also emitted as a caption. Captions reach `web-app`/mobile through `realtime-gateway` within the **caption latency budget of p95 < 2 s** (Deepgram-bound; no LLM in this path).

### 2.3 Agent output out - `agent-stream` + `agent-results` → `realtime-gateway` → sidebar

The agent sidebar renders LLM output progressively. `agent-stream` token deltas and the final `agent-results` message are consumed from Redpanda by `realtime-gateway` and pushed over WebSocket to the client agent sidebar.

```
agent-runtime-service → agent-stream (token deltas, Avro) ─┐
agent-runtime-service → agent-results (final, durable) ────┤
                                                           ▼
                                        realtime-gateway (consume)
                                                           │ PUBLISH nexus:{org_id}:pubsub:meeting:{meeting_id}
                                                           ▼
                                        all gateway pods → local sockets in room meeting_id → sidebar
```

Streaming is a UX latency optimization, never a system of record: a client that misses every delta still receives the final `agent-results` card. `agent-stream` is the highest-count, lowest-value topic and is the first thing shed under backpressure (§7).

---

## 3. `transcription-service` detail

- **Server-side audio path.** Receives the LiveKit audio hook over an internal WS; proxies to Deepgram over WSS. The **Deepgram key never leaves the server**.
- **Interim vs final.** The Deepgram session runs streaming ASR with **speaker diarization** and **interim + final** results. Interims drive captions only - never persisted, never on the bus. **Only finals** are durable.
- **Persist finals only.** Each final utterance, merged by speaker turn, is written as one `transcript_chunks` row (`org_id`, `meeting_id`, `speaker_id`, `start_ms`, `end_ms`, `text`, `is_final`, `confidence`) and then produced to the `transcript-chunks` topic as the `TranscriptChunk` Avro value, enveloped per `07-event-architecture.md`.
- **Diarization mapping.** The diarized `speaker_id` label is emitted as-is; the confirmed `speaker_id → user_id` mapping is resolved downstream (via `meeting_speakers`), never guessed at write time.
- **Consumes `meeting-room-events`.** Opens/closes Deepgram streams in response to LiveKit lifecycle events (joined / left / recording-ready) produced by `meeting-service`.
- **Produces `transcript-chunks` only.** It does not produce `usage-events`; transcription minutes are metered by `billing-metering-service` in the control-plane tier.

---

## 4. `realtime-gateway` detail

`realtime-gateway` is the WebSocket/Socket.io fan-out plane for every **non-AV** real-time signal. Socket.io **rooms are keyed by `meeting_id`**.

| Channel | Source | Direction |
|---|---|---|
| Live captions | `transcription-service` (interims + finals) | server → client |
| Agent sidebar output | `agent-results` | server → client |
| Streamed agent tokens | `agent-stream` | server → client |
| In-meeting chat | `web-app` ↔ `core-api` | bidirectional |
| Presence | join/leave → `presence-events` | bidirectional |
| Priority alerts | per-user queue `nexus:{org_id}:queue:agent:{user_id}` | server → client |

**Cross-instance fan-out.** A connection for meeting `M` may land on any gateway pod, but a bus event for `M` arrives at one pod. Cross-pod fan-out uses a **Redis Cluster pub/sub backbone** (the Socket.io Redis adapter) publishing on the canonical channel **`nexus:{org_id}:pubsub:meeting:{meeting_id}`**. Every pod subscribed to that channel emits to its local sockets in room `M`.

**Presence.** Join/leave is published as `presence-events` (consumed by `core-api` for analytics); the live presence set is `nexus:{org_id}:presence:{meeting_id}`.

**Ephemeral state only.** A pod holds no authoritative state. Room membership, fan-out, presence sets, and the per-user priority queue all live in Redis. Any pod can serve any meeting; losing a pod loses only its live sockets, which reconnect elsewhere with no data loss because every durable fact is in Postgres/Redpanda.

---

## 5. Why two separate containers

The two services share a scaling family but ride **opposite anchors**:

- `transcription-service` scales on `A5` = **20,000 concurrent live streams** (one diarized Deepgram session per meeting).
- `realtime-gateway` scales on `A6` = **100,000 concurrent WebSocket connections** (one per participant).

20,000 meetings carry ~100,000 live connections (effective occupancy `A8` ≈ 5.0), not `20,000 × 6` nominal - which is exactly why the two anchors are not interchangeable. Coupling them in one process forces one to over-provision for the other's load. Separate containers with separate HPAs/KEDA scalers is the structural fix.

---

## 6. Multi-tenancy

`org_id` is non-optional and present on every connection and every message:

- The WS handshake carries the Clerk-verified JWT (pre-validated at `api-gateway`); `org_id` is resolved from it, never from the client payload. Authz is enforced on room join (a socket may only join a `meeting_id` room within its own `org_id`).
- Every Redis key is prefixed `nexus:{org_id}:…` - pub/sub channel `nexus:{org_id}:pubsub:meeting:{meeting_id}`, presence `nexus:{org_id}:presence:{meeting_id}`, priority queue `nexus:{org_id}:queue:agent:{user_id}`.
- Fan-out is **per-tenant isolated** by the channel namespace, so a noisy tenant's traffic, eviction, or flush cannot reach another tenant's sockets.
- Every produced event (`transcript-chunks`, `presence-events`) is partitioned by `org_id` and carries it in the envelope header.

---

## 7. Event contracts

Topic and channel names are owned by `07-event-architecture.md` and kept verbatim.

| Direction | Topic | Role |
|---|---|---|
| Produces | `transcript-chunks` | `transcription-service` emits finalized diarized utterances (`TranscriptChunk` Avro). |
| Produces | `presence-events` | `realtime-gateway` emits Socket.io presence join/leave (`PresenceEvent`, 12 partitions, 1h retention). |
| Consumes | `meeting-room-events` | `transcription-service` starts/stops streams on LiveKit lifecycle events. |
| Consumes | `agent-stream` | `realtime-gateway` fans token deltas to the sidebar (120 partitions, 1h retention). |
| Consumes | `agent-results` | `realtime-gateway` fans the final agent card to the sidebar. |
| Consumes | `notifications` | `realtime-gateway` fans notification intents to clients. |

All produced events follow the at-least-once contract: transactional outbox on produce, idempotency key in the envelope, and `org_id` partition key. `transcript-chunks` uses idempotency key `chunk:{chunk_id}` so a redelivered final never double-writes.

---

## 8. Backpressure & resilience

- **Sticky connections, stateless state.** A WebSocket is pinned to its owning pod for its lifetime (connection-hash / `sid` affinity at the ingress). This is connection stickiness, not session stickiness - the application state behind it is stateless and in Redis.
- **Reconnect.** Socket.io auto-reconnect with exponential backoff. On reconnect the client re-mints its LiveKit token (via `meeting-service`) and re-joins the `meeting_id` room; caption/agent history is re-hydrated from `core-api` (finals are durable). Target: Deepgram WS proxy reconnect **> 99.9% within 2 s**.
- **At-least-once caption delivery.** Captions and confirmed agent outputs are preserved under congestion; a dropped socket never loses transcript or confirmed agent output because finals are durable.
- **Per-connection backpressure / dropped-frame handling.** Each socket has a bounded outbound buffer. Under client slow-consume, **`agent-stream` token deltas are dropped first** (throwaway - the UI still gets the final `agent-results`); captions and confirmed outputs are never dropped. This mirrors the bus-level shed order.
- **Priority pre-emption.** The per-user queue `nexus:{org_id}:queue:agent:{user_id}` lets urgent alerts (conflict banner, workload warning) pre-empt routine note deltas on a congested connection.
- **Redis pub/sub backbone.** Cross-pod fan-out and presence ride Redis Cluster; pod loss is recoverable because no durable state is on the pod.

---

## 9. Security

- **Deepgram key never leaves the server.** Audio is proxied backend-to-backend; no client ever holds the Deepgram key.
- **JWT + `org_id` on the WS handshake.** The Clerk JWT is pre-validated at `api-gateway`; `org_id` and role are extracted before the connection reaches `realtime-gateway`. The handshake fails closed if `org_id` cannot be resolved.
- **Authz on room join.** Deny-by-default: a socket may only join a `meeting_id` room within its JWT-resolved `org_id`; cross-tenant room joins are rejected.
- **Server-side audio only.** The LiveKit audio hook is a backend track subscription - no client audio uplink to the transcription path.

---

## 10. Scaling & SLOs

| Path / subsystem | Metric | Target |
|---|---|---|
| Live captions (Deepgram final → UI) | p95 | < 2 s |
| Live captions | p99 | < 3 s |
| Deepgram WS proxy reconnect | success | > 99.9% within 2 s |
| `realtime-gateway` | capacity | `A6` = 100,000 concurrent connections |
| `transcription-service` | capacity | `A5` = 20,000 concurrent live streams |

Autoscaling is KEDA-driven: `realtime-gateway` scales on **concurrent WebSocket connections**, `transcription-service` on **concurrent live streams**. The two scalers are independent.

---

## 11. Statelessness

Both services are in the stateless-services-durable-stores invariant. `transcription-service` writes only `transcript_chunks` (and produces to the bus). `realtime-gateway` holds **only ephemeral connection and presence state, backed by Redis** - pub/sub channels, presence sets, the per-user priority queue. No durable domain state lives on a real-time pod; losing a pod loses only live sockets, which reconnect with no data loss.

---

## 12. Dev practices

- **Languages.** TypeScript for `realtime-gateway`; typed Python (3.11+, full type hints) for `transcription-service`.
- **Lint/format.** ESLint + Prettier (Node); ruff + black (Python).
- **TDD.** Tests first; the canonical hard tests are WS fan-out load/soak (gateway) and interim-vs-final + finals-only persistence (transcription).
- **Conventional commits**, trunk-based development with short-lived branches and PR review.

---

## 13. CI/CD

- **GitHub Actions.** Lint + test both stacks (Node and Python) on every PR; Avro schema compatibility (BACKWARD) gate for produced topics.
- **WS load tests** run in CI to guard fan-out throughput and reconnect behavior before merge.
- Docker images built per service and deployed to the Kubernetes (EKS/GKE) real-time tier.

---

## 14. Observability

- **OpenTelemetry** traces across the live path (`correlation_id` ties a meeting window's fan-out).
- **Key metrics.** Connection count (per pod, per tenant), caption latency (p95/p99), consumer lag on `agent-stream`/`agent-results`/`meeting-room-events`, Deepgram reconnect success, per-tenant fan-out volume.
- **Sentry** for error tracking; **structured JSON logs** with `org_id`, `meeting_id`, and `correlation_id` on every line.

---

## 15. Canon references

- `10-realtime-and-transcription.md` - the authoritative real-time & transcription spec (LiveKit transport, Deepgram proxy, `transcript_chunks`, captions, Socket.io fan-out, latency budget).
- `03-container-architecture.md` - C4 L2 view; `realtime-gateway` & `transcription-service` containers, the three WebSocket flows (§4.3), scaling families (§8).
- `07-event-architecture.md` - topic catalog, Avro envelope, partitioning, at-least-once delivery for `transcript-chunks`, `presence-events`, `agent-stream`, `agent-results`, `meeting-room-events`, `notifications`.

Any change to a frozen name, number, or contract must land in `reprise-docs` `_canon/` first and propagate outward.
