/**
 * realtime-gateway: the Socket.io WebSocket fan-out plane.
 *
 * Boots an HTTP server with `/healthz` and a Socket.io server (namespace `/v1`) wired to the
 * Redis adapter for cross-instance fan-out. Handshake authz resolves `org_id`/`user_id` from a
 * verified Clerk JWT only — never from the client payload — and fails closed. Room join is
 * deny-by-default via a `core-api` participant check. Presence lives only in Redis and is mirrored
 * to the `presence-events` topic. Chat is relayed over the socket and durably written by
 * `core-api`. Captions (Task 8) are bridged live from `transcription-service`'s publish on the
 * explicit `nexus:{org_id}:pubsub:meeting:{meeting_id}` channel to the room's sockets — scoped
 * per-socket by that literal channel, not by Socket.io room membership (see the caption-relay
 * comment below for why that distinction is load-bearing for tenant isolation). This gateway
 * holds no caption state: nothing is buffered, and a reconnecting client is never replayed
 * anything it missed.
 *
 * Run: npm run build && npm start
 */
import { createServer, type Server as HttpServer } from "node:http";

import { createAdapter } from "@socket.io/redis-adapter";
import { Redis } from "ioredis";
import pino, { type Logger } from "pino";
import { Namespace, Server, type Socket } from "socket.io";

import { createClerkJwtVerifier, type ClerkJwtVerifier } from "../../../libs/auth";
import { presenceKey, pubsubChannel, roomName } from "../../../libs/keys";
import {
  createAgentOutputConsumer,
  type AgentOutputConsumer,
  type AgentPubsubMessage,
} from "./agentOutput";
import { createCoreApiClient, type CoreApiClient } from "./coreApiClient";
import { handleMetricsRequest, setClientCountSource } from "./metrics";
import { heartbeat, listPresence, removeMember, sweepStalePresence } from "./presence";
import {
  createPresenceEventsProducer,
  type PresenceEventsProducer,
} from "./presenceEventsProducer";

// ---- Socket.io event contract (namespace `/v1`) ----------------------------------------------
//
// Client -> server:
//   "join" ({ meetingId: string }, ack?: (res: JoinAck) => void)
//     Joins the room for `meetingId` if the JWT-resolved `org_id`/`user_id` passes the core-api
//     participant check; deny-by-default otherwise. Always resolves via `ack` (never throws to
//     the client): { ok: true, presence: string[] } | { ok: false, error: "FORBIDDEN" |
//     "INVALID_PAYLOAD" }. On success, also broadcasts "presence" to the room (see below).
//   "chat" ({ meetingId: string, content: string, clientMsgId: string })
//     Requires a prior successful "join" for that `meetingId` on this socket (else silently
//     dropped). Broadcasts "chat" to the room immediately, then durably persists via `core-api`
//     POST /api/v1/chat with `Idempotency-Key: clientMsgId`.
//
// Server -> client (broadcast to the room `meeting:{org_id}:{meeting_id}` — org-scoped, not
// meeting-only; this is the actual tenancy boundary for cross-pod presence/chat fan-out, see
// libs/keys.ts's `roomName` doc comment):
//   "presence" (string[])            — full current member list, sent on every join/leave
//                                       transition in that room. CONTRACT NOTE: these are
//                                       core-api's internal `users.id` UUIDs (resolved from the
//                                       participant check on join), NOT the Clerk `sub` claim —
//                                       this changed from an earlier version of this contract
//                                       that put the raw Clerk id here; see coreApiClient.ts's
//                                       header comment. Matches ChatMessage.user_id's shape from
//                                       core-api's POST /chat response, unlike the "chat" event
//                                       below, which still carries the Clerk id (see its note).
//   "chat" ({ meetingId, userId, content, clientMsgId }) — `userId` here is still the Clerk `sub`
//                                       from the sender's JWT, not resolved to an internal UUID;
//                                       out of scope for the presence-events fix this file's
//                                       history refers to, flagged as a related follow-up rather
//                                       than silently left inconsistent.
//   "caption" ({ meetingId, payload })  — forwarded verbatim (Task 8) from `transcription-
//                                          service`'s publish on the explicit pubsub channel.
//                                          `payload` is `{ chunkId, speakerId, startMs, endMs,
//                                          text, isFinal, confidence }` (transcription-
//                                          service's `captions.py`), forwarded byte-for-byte —
//                                          this file does not read or interpret any field of it.
//                                          A client tells an interim from a final by `isFinal`;
//                                          `chunkId` is stable across an utterance's interim
//                                          revisions and its own final, which is what lets a
//                                          client replace-in-place by `chunkId` until `isFinal`
//                                          arrives, then append. EVERY caption — interim or
//                                          final — is delivered via a normal, reliable
//                                          `socket.emit`, never intentionally dropped: this
//                                          repo's own CLAUDE.md pitfalls list is explicit
//                                          ("never drop captions or confirmed agent output" —
//                                          the thing to shed under backpressure is
//                                          `agent-stream` token deltas, a different, not-yet-
//                                          built feature, not captions of either kind). NO
//                                          caption is ever buffered or replayed by this
//                                          gateway — a reconnecting client gets nothing from
//                                          before it (re)joined; finals are re-hydrated from
//                                          core-api's REST read model, never from this socket.
//
//   "agent-delta" ({ meetingId, payload: { windowId, seq, delta, done } }) and
//   "agent-note"  ({ meetingId, payload: { windowId, agent, content, confidence,
//                   relevanceScore, sourceChunkIds } })
//                                       — PersonalListenerAgent output (Plan 7-9 section 3.4,
//                                          mytheus-docs ADR-0030 D5), bridged from `agent-stream`
//                                          and `agent-results` by agentOutput.ts. PRIVATE: emitted
//                                          only to this pod's sockets on the meeting channel whose
//                                          resolved internal user id is the note's owner, never to
//                                          the room. Deltas are volatile (shed first under
//                                          backpressure); a note is a reliable emit. Neither is
//                                          replayed: re-hydrate from core-api's worklogs route.
//
//   "agent-suggestion" ({ meetingId, payload: { windowId, agent, kind, suggestions } })
//                                       — QuestionAgent `suggestion` output (mytheus-docs
//                                          ADR-0034), bridged from `agent-results` by
//                                          agentOutput.ts. PRIVATE like "agent-note": emitted
//                                          only to the owner's sockets, never to the room.
//                                          Not replayed: re-hydrate from core-api's
//                                          GET /api/v1/suggestions?meeting_id=.
//
//   "agent-candidate" ({ meetingId, payload: { windowId, agent, kind, candidates } })
//                                       — DecisionAgent `decision_candidate` / ActionAgent
//                                          `action_candidate` output (Phase 8 Part A contract
//                                          section 3), bridged from `agent-results` by
//                                          agentOutput.ts. MEETING-SCOPED: emitted (reliable, not
//                                          volatile) to EVERY socket of this pod joined to the
//                                          meeting; the org-scoped channel keeps it inside the
//                                          tenant even when another org's meeting has the same
//                                          id. `candidates` holds only `surfaced: true` entries
//                                          ({ id, content, confidence, surfaced, rationale,
//                                          stakeholder_ids } for decisions; { id, content,
//                                          confidence, surfaced, assignee_id, due_date,
//                                          depends_on } for actions). Proposals only: confirm and
//                                          dismiss go through core-api. Not replayed: re-hydrate
//                                          from GET /api/v1/meetings/:id/candidates.
//
// Disconnect: presence is removed from every meeting the socket had joined, "presence" is
// re-broadcast per meeting, and a "leave" `presence-events` record is produced per meeting. This
// is the GRACEFUL path — it requires this process to still be alive to run it. Presence is also
// heartbeated (see `heartbeatIntervalMs`) and periodically swept (see `sweepIntervalMs`) so that a
// pod that dies without running `disconnect` at all (SIGKILL, OOM-kill, eviction) still has its
// presence entries expire from reads quickly and get physically reaped — and a "leave" event
// produced — by whichever pod survives to run the next sweep.

export interface JoinPayload {
  meetingId: string;
}

export interface JoinAck {
  ok: boolean;
  presence?: string[];
  error?: "FORBIDDEN" | "INVALID_PAYLOAD";
}

export interface ChatPayload {
  meetingId: string;
  content: string;
  clientMsgId: string;
}

interface SocketData {
  /** From the JWT `org_id` claim — Clerk's opaque org id, NOT a UUID. Used only for the trust
   * boundary (deny-by-default authz) and forwarded to core-api, which resolves it server-side.
   * Never use this for presence storage or presence-events — see `presenceOrgId`. */
  orgId: string;
  /** From the JWT `sub` claim — Clerk's user id, NOT a UUID. Same caveat as `orgId`. */
  userId: string;
  bearerToken: string;
  joinedMeetings: Set<string>;
  /** One heartbeat interval per joined meeting, so it can be cleared on leave/disconnect. */
  heartbeatTimers: Map<string, NodeJS.Timeout>;
  /**
   * The identity actually used for presence storage (Redis) and `presence-events` production.
   * Set once, on the socket's first successful join, from core-api's participant-check response
   * (see `coreApiClient.ts`). Both fields are the resolved internal `users.id`/`organizations.id`
   * UUIDs on essentially every real join today. `presenceOrgId` still falls back to the Clerk
   * `orgId` claim on the rare response that lacks it (see `coreApiClient.ts`'s "Identity
   * resolution" note) — kept as a safety net, not because that's expected — which keeps
   * join/presence/room fully functional either way; it can never reintroduce the original bug
   * because `presenceEventsProducer.produce()` itself refuses to emit anything that isn't
   * UUID-shaped. Initialized to `""` in the handshake middleware (never read before a successful
   * join sets it for real — `joinedMeetings` only gains entries after that point).
   */
  presenceOrgId: string;
  presenceUserId: string;
}

export interface AppConfig {
  port: number;
  wsBindHost?: string;
  redisUrl: string;
  coreApiUrl: string;
  redpandaBrokers: string[];
  schemaRegistryUrl: string;
  /** How often a joined socket refreshes its own presence heartbeat. Production default 15s. */
  presenceHeartbeatIntervalMs?: number;
  /** A member with no heartbeat in this long is treated as gone by reads (`listPresence`) and
   * eventually reaped by the sweep. Should be a small multiple of the heartbeat interval so a
   * couple of missed beats (not one) triggers it. Production default 45s. */
  presenceStaleAfterMs?: number;
  /** How often the reconciliation sweep runs, reaping stale entries left behind by a pod that
   * died without running its `disconnect` handler. Production default 20s. */
  presenceSweepIntervalMs?: number;
  /** Consume `agent-stream`/`agent-results` and bridge them to sockets. On in production
   * (`AGENT_OUTPUT_ENABLED`, default on); off by default for in-process tests. */
  agentOutput?: boolean;
}

export interface AppDeps {
  /** Required: no default, because there is no live Clerk instance to default against. */
  jwtVerifier: ClerkJwtVerifier;
  coreApiClient?: CoreApiClient;
  presenceEventsProducer?: PresenceEventsProducer;
  /** Overrides the Redpanda agent-output bridge; built from config when `agentOutput` is on. */
  agentOutputConsumer?: AgentOutputConsumer;
  logger?: Logger;
}

export interface App {
  httpServer: HttpServer;
  io: Server;
  nsp: Namespace;
  start(): Promise<void>;
  stop(): Promise<void>;
}

export function createApp(config: AppConfig, deps: AppDeps): App {
  const logger = deps.logger ?? pino({ name: "realtime-gateway" });
  const coreApiClient =
    deps.coreApiClient ?? createCoreApiClient({ baseUrl: config.coreApiUrl, logger });
  const presenceEventsProducer =
    deps.presenceEventsProducer ??
    createPresenceEventsProducer({
      brokers: config.redpandaBrokers,
      schemaRegistryUrl: config.schemaRegistryUrl,
      logger,
    });

  const heartbeatIntervalMs = config.presenceHeartbeatIntervalMs ?? 15_000;
  const staleAfterMs = config.presenceStaleAfterMs ?? 45_000;
  const sweepIntervalMs = config.presenceSweepIntervalMs ?? 20_000;

  const httpServer = createServer((req, res) => {
    if (handleMetricsRequest(req, res)) return;
    if (req.url === "/healthz") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "ok", service: "realtime-gateway" }));
      return;
    }
    res.writeHead(404);
    res.end();
  });

  const io = new Server(httpServer, { path: "/socket.io" });
  setClientCountSource(() => io.engine.clientsCount);

  // Cross-instance fan-out via Redis. lazyConnect + error handlers keep the process (and
  // /healthz) alive even when Redis is unreachable; the adapter reconnects when Redis returns.
  const pubClient = new Redis(config.redisUrl, { lazyConnect: true });
  const subClient = pubClient.duplicate();
  // A third, independent connection subscribed to the *explicit* canonical channel (see
  // libs/keys.ts) for non-Socket.io publishers. This is separate from the adapter's own pub/sub
  // (subClient above), which is a private implementation detail of `@socket.io/redis-adapter`.
  const captionSubClient = pubClient.duplicate();
  pubClient.on("error", (err) => logger.error({ err }, "redis pub client error"));
  subClient.on("error", (err) => logger.error({ err }, "redis sub client error"));
  captionSubClient.on("error", (err) => logger.error({ err }, "redis caption-sub client error"));
  io.adapter(createAdapter(pubClient, subClient, { key: "nexus" }));

  const agentOutputConsumer =
    deps.agentOutputConsumer ??
    (config.agentOutput
      ? createAgentOutputConsumer({
          brokers: config.redpandaBrokers,
          schemaRegistryUrl: config.schemaRegistryUrl,
          publisher: pubClient,
          logger,
        })
      : undefined);

  const nsp = io.of("/v1");

  // Handshake: org_id ONLY from the Clerk JWT, never from the client payload. Fail closed.
  nsp.use(async (socket, next) => {
    const token = socket.handshake.auth?.token as string | undefined;
    try {
      const identity = await deps.jwtVerifier.verify(token);
      const data = socket.data as SocketData;
      data.orgId = identity.orgId;
      data.userId = identity.userId;
      data.bearerToken = token as string;
      data.joinedMeetings = new Set();
      data.heartbeatTimers = new Map();
      data.presenceOrgId = "";
      data.presenceUserId = "";
      next();
    } catch (err) {
      logger.warn({ err: err instanceof Error ? err.message : err }, "handshake rejected");
      next(new Error("UNAUTHENTICATED"));
    }
  });

  // Local sockets currently subscribed to each explicit caption channel, keyed by the FULL
  // channel string (which embeds org_id, not just meeting_id). One Set per (org_id, meeting_id)
  // pair that has at least one locally-connected socket.
  //
  // Why per-socket tracking here instead of `nsp.local.to(roomName(orgId, meetingId)).emit(...)`
  // — i.e. why captions don't just use the now-org-scoped room the way presence/chat do below:
  // the caption channel is fed by a non-Socket.io publisher (`transcription-service`) via plain
  // Redis pub/sub, and EVERY gateway pod that has subscribed to a channel receives that raw
  // message independently — that's what plain pub/sub does. Routing this back through
  // `nsp.to(room)` would ALSO re-broadcast it via the Redis adapter's own internal cross-pod
  // channels on top of that, delivering the same caption once per subscribed pod per client.
  // Delivering directly to locally-tracked sockets (`.local` in spirit, without needing the
  // literal flag) avoids that double-delivery. This is a delivery-mechanism reason, not a
  // tenancy one — `roomName(orgId, meetingId)` is in fact a safe, org-scoped room now (see
  // libs/keys.ts), and presence/chat below use it directly for exactly that reason. See
  // test/captions.test.ts's "zero cross-tenant caption delivery" test and
  // test/cross-tenant-room-collision.test.ts (presence/chat) for the tenant-isolation proofs on
  // each path.
  const localChannelSockets = new Map<string, Set<Socket>>();

  // In-flight per-socket disconnect cleanups (see the `disconnect` handler below). Tracked so
  // `stop()` can wait for them to settle before disconnecting `pubClient`/`captionSubClient` —
  // otherwise a shutdown-in-progress `io.close()` races an async disconnect handler still
  // mid-flight against those same Redis connections, which then reject with "Connection is
  // closed" (fix round 1: this was firing in real test teardowns, not just a hypothetical).
  const pendingDisconnectCleanups = new Set<Promise<void>>();

  captionSubClient.on("message", (channel: string, raw: string) => {
    const localSockets = localChannelSockets.get(channel);
    if (!localSockets || localSockets.size === 0) return;
    let msg: { type?: string; payload?: unknown };
    try {
      msg = JSON.parse(raw) as { type?: string; payload?: unknown };
    } catch {
      logger.warn({ channel }, "discarding malformed message on caption channel");
      return;
    }
    if (msg.type === "agent-candidate") {
      deliverAgentCandidate(channel, localSockets, msg as AgentPubsubMessage);
      return;
    }
    if (
      msg.type === "agent-delta" ||
      msg.type === "agent-note" ||
      msg.type === "agent-suggestion"
    ) {
      deliverAgentOutput(channel, localSockets, msg as AgentPubsubMessage);
      return;
    }
    if (msg.type !== "caption") return;
    // `meetingId` is ALWAYS derived from the channel string, never from the message body: the
    // channel is the authenticated fact (it's what this process subscribed to, tied to the
    // sockets in `localSockets`); the body is untrusted JSON from a publisher this gateway does
    // not authenticate per-message. transcription-service's captions.py does send a matching
    // `meetingId` field in the body today, but that's an invariant of the current publisher,
    // not something this gateway can enforce — deriving from the channel means a body/channel
    // mismatch (bug or otherwise) can never mislabel a caption into the wrong meeting's UI.
    const meetingId = extractMeetingId(channel);
    if (meetingId === null) return;
    const outbound = { meetingId, payload: msg.payload };
    // Every caption — interim or final — is delivered via a normal, reliable emit. This repo's
    // own CLAUDE.md pitfalls list is explicit and unqualified: "never drop captions or confirmed
    // agent output"; the thing to shed under backpressure is `agent-stream` token deltas (a
    // separate, not-yet-built feature in this repo — see Task 6's report), not captions of
    // either kind. An interim being superseded by its own next revision (same `chunkId`) is a
    // reason a CLIENT may discard a stale one it still has buffered once a newer one arrives,
    // never a reason this gateway may skip sending one.
    for (const socket of localSockets) {
      socket.emit("caption", outbound);
    }
  });

  function deliverAgentOutput(
    channel: string,
    localSockets: Set<Socket>,
    msg: AgentPubsubMessage,
  ): void {
    if (msg.type === "agent-candidate") return;
    const meetingId = extractMeetingId(channel);
    if (meetingId === null || typeof msg.userId !== "string" || msg.userId.length === 0) return;
    const outbound = { meetingId, payload: msg.payload };
    for (const socket of localSockets) {
      if ((socket.data as SocketData).presenceUserId !== msg.userId) continue;
      if (msg.type === "agent-delta") {
        socket.volatile.emit("agent-delta", outbound);
      } else {
        // a note or a suggestion: private to its owner, a reliable emit
        socket.emit(msg.type, outbound);
      }
    }
  }

  // Meeting-scoped candidates: every local socket on this (org-scoped) channel gets them. The
  // channel, not the body, is the tenant fact, exactly as for captions above.
  function deliverAgentCandidate(
    channel: string,
    localSockets: Set<Socket>,
    msg: AgentPubsubMessage,
  ): void {
    if (msg.type !== "agent-candidate") return;
    const meetingId = extractMeetingId(channel);
    if (meetingId === null) return;
    const outbound = { meetingId, payload: msg.payload };
    for (const socket of localSockets) {
      socket.emit("agent-candidate", outbound);
    }
  }

  function extractMeetingId(channel: string): string | null {
    const match = /^nexus:(.+):pubsub:meeting:(.+)$/.exec(channel);
    return match ? match[2] : null;
  }

  async function subscribeCaptionChannel(
    orgId: string,
    meetingId: string,
    socket: Socket,
  ): Promise<void> {
    const channel = pubsubChannel(orgId, meetingId);
    let localSockets = localChannelSockets.get(channel);
    if (!localSockets) {
      localSockets = new Set();
      localChannelSockets.set(channel, localSockets);
    }
    const isNewChannel = localSockets.size === 0;
    localSockets.add(socket);
    if (isNewChannel) {
      await captionSubClient.subscribe(channel);
    }
  }

  async function unsubscribeCaptionChannel(
    orgId: string,
    meetingId: string,
    socket: Socket,
  ): Promise<void> {
    const channel = pubsubChannel(orgId, meetingId);
    const localSockets = localChannelSockets.get(channel);
    if (!localSockets) return;
    localSockets.delete(socket);
    if (localSockets.size === 0) {
      localChannelSockets.delete(channel);
      await captionSubClient.unsubscribe(channel);
    }
  }

  nsp.on("connection", (socket: Socket) => {
    const { orgId, userId } = socket.data as SocketData;
    logger.info({ orgId, userId, socketId: socket.id }, "socket connected");

    socket.on("join", async (payload: JoinPayload, ack?: (res: JoinAck) => void) => {
      const meetingId = payload?.meetingId;
      if (typeof meetingId !== "string" || meetingId.length === 0) {
        ack?.({ ok: false, error: "INVALID_PAYLOAD" });
        return;
      }
      const data = socket.data as SocketData;

      // Deny-by-default: a socket may only join a meeting room within its own JWT-resolved
      // org_id. This check is the actual enforcement point; the participant-check response also
      // carries core-api's resolved internal identity — see coreApiClient.ts's header comment.
      const check = await coreApiClient.isParticipant({
        orgId,
        meetingId,
        userId,
        bearerToken: data.bearerToken,
      });
      if (!check.allowed) {
        logger.warn(
          { orgId, userId, meetingId },
          "join denied (not a participant, or cross-org attempt)",
        );
        ack?.({ ok: false, error: "FORBIDDEN" });
        return;
      }

      // Presence storage and presence-events use this resolved identity, never the raw JWT
      // claims above — see SocketData's doc comment and presenceEventsProducer.ts's UUID guard,
      // which is what makes the org fallback here safe (it can only ever cause a skip, never a
      // repeat of the original bug).
      data.presenceOrgId = check.internalOrgId ?? orgId;
      data.presenceUserId = check.internalUserId;

      await socket.join(roomName(data.presenceOrgId, meetingId));
      data.joinedMeetings.add(meetingId);
      await subscribeCaptionChannel(data.presenceOrgId, meetingId, socket);

      await heartbeat(pubClient, data.presenceOrgId, meetingId, data.presenceUserId);
      // Keep this member's heartbeat fresh for as long as the socket stays joined to this
      // meeting, so an abrupt death of THIS pod doesn't leave a permanently-live-looking ghost —
      // see presence.ts's module doc and the sweep started in start() below.
      data.heartbeatTimers.set(
        meetingId,
        setInterval(() => {
          heartbeat(pubClient, data.presenceOrgId, meetingId, data.presenceUserId).catch(
            (err: unknown) => {
              logger.error(
                { err, orgId: data.presenceOrgId, meetingId, userId: data.presenceUserId },
                "presence heartbeat failed",
              );
            },
          );
        }, heartbeatIntervalMs),
      );

      const members = await listPresence(pubClient, data.presenceOrgId, meetingId, staleAfterMs);
      nsp.to(roomName(data.presenceOrgId, meetingId)).emit("presence", members);
      ack?.({ ok: true, presence: members });

      try {
        await presenceEventsProducer.produce({
          orgId: data.presenceOrgId,
          meetingId,
          userId: data.presenceUserId,
          eventType: "join",
        });
      } catch (err) {
        logger.error(
          { err, orgId: data.presenceOrgId, meetingId, userId: data.presenceUserId },
          "failed to produce presence-events (join)",
        );
      }
    });

    socket.on("chat", async (payload: ChatPayload) => {
      const data = socket.data as SocketData;
      if (
        typeof payload?.meetingId !== "string" ||
        typeof payload?.content !== "string" ||
        typeof payload?.clientMsgId !== "string"
      ) {
        return;
      }
      if (!data.joinedMeetings.has(payload.meetingId)) {
        logger.warn(
          { orgId, userId, meetingId: payload.meetingId },
          "chat dropped: socket has not joined this meeting",
        );
        return;
      }

      // Fan out now (low latency); the durable write is a core-api call, not a local write.
      // `data.presenceOrgId` is safe to read here unconditionally: `chat` requires a prior
      // successful `join` for this exact `meetingId` on this socket (the check above), and
      // `presenceOrgId` is set during that same join before `joinedMeetings` gains the entry.
      nsp.to(roomName(data.presenceOrgId, payload.meetingId)).emit("chat", {
        meetingId: payload.meetingId,
        userId,
        content: payload.content,
        clientMsgId: payload.clientMsgId,
      });

      try {
        await coreApiClient.postChat({
          meetingId: payload.meetingId,
          userId,
          content: payload.content,
          clientMsgId: payload.clientMsgId,
          bearerToken: data.bearerToken,
        });
      } catch (err) {
        logger.error(
          { err, orgId, meetingId: payload.meetingId },
          "core-api chat persistence call failed",
        );
      }
    });

    socket.on("disconnect", () => {
      const data = socket.data as SocketData;
      for (const meetingId of data.joinedMeetings) {
        clearInterval(data.heartbeatTimers.get(meetingId));
        data.heartbeatTimers.delete(meetingId);
        const cleanup = cleanupMeetingOnDisconnect(data, meetingId, socket);
        pendingDisconnectCleanups.add(cleanup);
        void cleanup.finally(() => pendingDisconnectCleanups.delete(cleanup));
      }
    });
  });

  /**
   * Per-meeting disconnect cleanup, split into THREE INDEPENDENT steps, each with its own
   * try/catch, so a failure in one can never skip the others (fix round 1: the previous single
   * shared try/catch meant a `removeMember` failure — a transient Redis blip is exactly the
   * kind of thing that happens in production — silently skipped `unsubscribeCaptionChannel`
   * entirely. The dead socket then stayed in `localChannelSockets`'s Set forever: a per-socket
   * leak, and, once it was the last socket for that channel, an indefinitely-open Redis
   * subscription to a channel nobody is listening to anymore). Step 2 (caption unsubscribe) is
   * therefore ALWAYS attempted regardless of whether step 1 (presence) succeeded.
   */
  async function cleanupMeetingOnDisconnect(
    data: SocketData,
    meetingId: string,
    socket: Socket,
  ): Promise<void> {
    const { presenceOrgId: orgId, presenceUserId: userId } = data;

    try {
      await removeMember(pubClient, orgId, meetingId, userId);
      const members = await listPresence(pubClient, orgId, meetingId, staleAfterMs);
      nsp.to(roomName(orgId, meetingId)).emit("presence", members);
    } catch (err) {
      logger.error({ err, orgId, meetingId, userId }, "presence removal failed on disconnect");
    }

    try {
      await unsubscribeCaptionChannel(orgId, meetingId, socket);
    } catch (err) {
      logger.error({ err, orgId, meetingId }, "caption channel unsubscribe failed on disconnect");
    }

    try {
      await presenceEventsProducer.produce({ orgId, meetingId, userId, eventType: "leave" });
    } catch (err) {
      logger.error(
        { err, orgId, meetingId, userId },
        "failed to produce presence-events (leave, from disconnect)",
      );
    }
  }

  // Reconciliation sweep: reaps presence entries whose owning pod died without running its
  // `disconnect` handler (SIGKILL, OOM-kill, eviction — see presence.ts's module doc). Runs on
  // every pod independently; a meeting with no locally-connected socket on THIS pod can still have
  // its abandoned entries reaped here, which is the whole point — the pod that owned the dead
  // socket is, by definition, not around to clean up after itself.
  async function runPresenceSweep(): Promise<void> {
    const reaped = await sweepStalePresence(pubClient, staleAfterMs);
    for (const { orgId, meetingId, userId } of reaped) {
      logger.warn(
        { orgId, meetingId, userId },
        "reaped a stale presence entry (its owning pod likely died without a clean disconnect)",
      );
      const members = await listPresence(pubClient, orgId, meetingId, staleAfterMs);
      nsp.to(roomName(orgId, meetingId)).emit("presence", members);
      try {
        await presenceEventsProducer.produce({ orgId, meetingId, userId, eventType: "leave" });
      } catch (err) {
        logger.error(
          { err, orgId, meetingId, userId },
          "failed to produce presence-events (leave, from sweep)",
        );
      }
    }
  }

  let sweepTimer: NodeJS.Timeout | undefined;

  return {
    httpServer,
    io,
    nsp,
    async start() {
      await presenceEventsProducer.connect();
      await agentOutputConsumer?.start();
      sweepTimer = setInterval(() => {
        runPresenceSweep().catch((err: unknown) => logger.error({ err }, "presence sweep failed"));
      }, sweepIntervalMs);
      await new Promise<void>((resolve) => {
        if (config.wsBindHost) {
          httpServer.listen(config.port, config.wsBindHost, resolve);
        } else {
          httpServer.listen(config.port, resolve);
        }
      });
    },
    async stop() {
      clearInterval(sweepTimer);
      await agentOutputConsumer?.stop();
      await io.close();
      // `io.close()`'s own callback does not wait for each socket's async `disconnect` handler
      // to finish (Node's EventEmitter fires listeners without awaiting them) — those handlers
      // are still using `pubClient`/`captionSubClient` to clean up presence and caption
      // subscriptions. Waiting for them here (rather than disconnecting Redis out from under
      // them) is what actually distinguishes a clean shutdown from a genuine mid-operation
      // failure: with this wait, a `disconnect` handler only ever sees "Connection is closed"
      // if Redis itself is genuinely down, never as an artifact of this method's own ordering.
      //
      // A single snapshot-then-await is correct as long as every relevant `disconnect` handler
      // has already registered its cleanup into `pendingDisconnectCleanups` by the time the
      // snapshot is taken — true for how `io.close()` currently closes still-connected sockets
      // (synchronously, before its own callback fires), but that is an unstated internal
      // ordering guarantee of Socket.io's current implementation, not something this file
      // enforces. `Promise.allSettled` also yields to the event loop, which is exactly the kind
      // of window a concurrently-arriving client-initiated disconnect (as opposed to one
      // `io.close()` itself triggered) could register a NEW cleanup into after the snapshot was
      // taken. Looping until the set is genuinely empty — re-checked right before disconnecting
      // Redis, with no further await in between — removes the dependency on that internal
      // ordering guarantee entirely, rather than assuming it holds.
      while (pendingDisconnectCleanups.size > 0) {
        await Promise.allSettled([...pendingDisconnectCleanups]);
      }
      await presenceEventsProducer.disconnect();
      pubClient.disconnect();
      subClient.disconnect();
      captionSubClient.disconnect();
    },
  };
}

/** Exposed for tests that want to drive the sweep directly against a real Redis without spinning
 * up a whole App (see presence.ts for the underlying mechanics). */
export { sweepStalePresence } from "./presence";

export function loadConfigFromEnv(): AppConfig {
  return {
    port: Number(process.env.PORT ?? 8090),
    wsBindHost: process.env.WS_BIND_HOST,
    redisUrl: process.env.REDIS_URL ?? "redis://localhost:6379",
    coreApiUrl: process.env.CORE_API_URL ?? "http://localhost:8000",
    redpandaBrokers: (process.env.REDPANDA_BROKERS ?? "localhost:9092").split(","),
    schemaRegistryUrl: process.env.SCHEMA_REGISTRY_URL ?? "http://localhost:8085",
    // Optional operational tuning; unset means "use the production default" (see AppConfig doc
    // comments). Exposed via env mainly so tests can drive fast heartbeat/stale/sweep cycles
    // against a real spawned process instead of waiting out the multi-second production defaults.
    presenceHeartbeatIntervalMs: envInt("PRESENCE_HEARTBEAT_INTERVAL_MS"),
    presenceStaleAfterMs: envInt("PRESENCE_STALE_AFTER_MS"),
    presenceSweepIntervalMs: envInt("PRESENCE_SWEEP_INTERVAL_MS"),
    agentOutput: process.env.AGENT_OUTPUT_ENABLED !== "0",
  };
}

function envInt(name: string): number | undefined {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return undefined;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : undefined;
}

// Also exported so key builders stay reachable from this module for anything that only imports
// `./server`; the canonical implementation lives in libs/keys.ts.
export { presenceKey, pubsubChannel, roomName };

if (require.main === module) {
  const config = loadConfigFromEnv();
  const clerkJwksUrl = process.env.CLERK_JWKS_URL;
  if (!clerkJwksUrl) {
    throw new Error("CLERK_JWKS_URL is required (fail closed: no default JWKS source)");
  }
  const jwtVerifier = createClerkJwtVerifier({ jwksUrl: clerkJwksUrl });
  const app = createApp(config, { jwtVerifier });
  app
    .start()
    .then(() => {
      const example = pubsubChannel("<org_id>", "<meeting_id>");
      // eslint-disable-next-line no-console
      console.log(
        `realtime-gateway listening on :${config.port} (fan-out channel form: ${example})`,
      );
    })
    .catch((err) => {
      // eslint-disable-next-line no-console
      console.error("realtime-gateway failed to start", err);
      process.exit(1);
    });
}
