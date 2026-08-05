/**
 * realtime-gateway: the Socket.io WebSocket fan-out plane.
 *
 * Boots an HTTP server with `/healthz` and a Socket.io server (namespace `/v1`) wired to the
 * Redis adapter for cross-instance fan-out. Handshake authz resolves `org_id`/`user_id` from a
 * verified Clerk JWT only — never from the client payload — and fails closed. Room join is
 * deny-by-default via a `core-api` participant check. Presence lives only in Redis and is mirrored
 * to the `presence-events` topic. Chat is relayed over the socket and durably written by
 * `core-api`. The caption path is a transport seam only in this task: it relays whatever a
 * non-Socket.io publisher puts on the explicit `nexus:{org_id}:pubsub:meeting:{meeting_id}`
 * channel — nothing publishes there yet (that lands with `transcription-service` later).
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
import { createCoreApiClient, type CoreApiClient } from "./coreApiClient";
import { heartbeat, listPresence, removeMember, sweepStalePresence } from "./presence";
import { createPresenceEventsProducer, type PresenceEventsProducer } from "./presenceEventsProducer";

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
// Server -> client (broadcast to the room `meeting:{meetingId}`):
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
//   "caption" ({ meetingId, payload })  — transport only in this task; `payload` is opaque JSON
//                                          forwarded verbatim from the explicit pubsub channel.
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
   * (see `coreApiClient.ts`). `presenceUserId` is always the resolved internal `users.id` UUID.
   * `presenceOrgId` is the resolved internal `organizations.id` UUID once core-api's response
   * includes it, falling back to the Clerk `orgId` claim only while it doesn't (see
   * `coreApiClient.ts`'s CONTRACT GAP note) — this fallback keeps join/presence/room fully
   * functional either way; it can never reintroduce the original bug because
   * `presenceEventsProducer.produce()` itself refuses to emit anything that isn't UUID-shaped.
   * Initialized to `""` in the handshake middleware (never read before a successful join sets
   * it for real — `joinedMeetings` only gains entries after that point).
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
}

export interface AppDeps {
  /** Required: no default, because there is no live Clerk instance to default against. */
  jwtVerifier: ClerkJwtVerifier;
  coreApiClient?: CoreApiClient;
  presenceEventsProducer?: PresenceEventsProducer;
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
  const coreApiClient = deps.coreApiClient ?? createCoreApiClient({ baseUrl: config.coreApiUrl, logger });
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
    if (req.url === "/healthz") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "ok", service: "realtime-gateway" }));
      return;
    }
    res.writeHead(404);
    res.end();
  });

  const io = new Server(httpServer, { path: "/socket.io" });

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

  // Reference-counted local subscriptions to the explicit caption channel, one per meeting room
  // that has at least one locally-connected socket. Every gateway pod that has subscribed
  // receives the raw pub/sub message independently (that is what plain Redis pub/sub does), so
  // the handler below emits with `.local` — routing this back through the Socket.io adapter's
  // own cross-pod broadcast would deliver the same caption once per subscribed pod per client.
  const localRoomRefcounts = new Map<string, number>();

  captionSubClient.on("message", (channel: string, raw: string) => {
    let msg: { type?: string; payload?: unknown };
    try {
      msg = JSON.parse(raw);
    } catch {
      logger.warn({ channel }, "discarding malformed message on caption channel");
      return;
    }
    if (msg.type !== "caption") return;
    const match = /^nexus:(.+):pubsub:meeting:(.+)$/.exec(channel);
    if (!match) return;
    // `org_id` (match[1]) only distinguishes which explicit channel matched — the room name is
    // meeting-only (see libs/keys.ts's roomName doc comment), so it isn't needed past this point.
    const [, , meetingId] = match;
    nsp.local.to(roomName(meetingId)).emit("caption", { meetingId, payload: msg.payload });
  });

  async function subscribeCaptionChannel(orgId: string, meetingId: string): Promise<void> {
    const channel = pubsubChannel(orgId, meetingId);
    const next = (localRoomRefcounts.get(channel) ?? 0) + 1;
    localRoomRefcounts.set(channel, next);
    if (next === 1) {
      await captionSubClient.subscribe(channel);
    }
  }

  async function unsubscribeCaptionChannel(orgId: string, meetingId: string): Promise<void> {
    const channel = pubsubChannel(orgId, meetingId);
    const current = localRoomRefcounts.get(channel) ?? 0;
    if (current <= 1) {
      localRoomRefcounts.delete(channel);
      if (current === 1) {
        await captionSubClient.unsubscribe(channel);
      }
    } else {
      localRoomRefcounts.set(channel, current - 1);
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
        logger.warn({ orgId, userId, meetingId }, "join denied (not a participant, or cross-org attempt)");
        ack?.({ ok: false, error: "FORBIDDEN" });
        return;
      }

      // Presence storage and presence-events use this resolved identity, never the raw JWT
      // claims above — see SocketData's doc comment and presenceEventsProducer.ts's UUID guard,
      // which is what makes the org fallback here safe (it can only ever cause a skip, never a
      // repeat of the original bug).
      data.presenceOrgId = check.internalOrgId ?? orgId;
      data.presenceUserId = check.internalUserId;

      socket.join(roomName(meetingId));
      data.joinedMeetings.add(meetingId);
      await subscribeCaptionChannel(data.presenceOrgId, meetingId);

      await heartbeat(pubClient, data.presenceOrgId, meetingId, data.presenceUserId);
      // Keep this member's heartbeat fresh for as long as the socket stays joined to this
      // meeting, so an abrupt death of THIS pod doesn't leave a permanently-live-looking ghost —
      // see presence.ts's module doc and the sweep started in start() below.
      data.heartbeatTimers.set(
        meetingId,
        setInterval(() => {
          heartbeat(pubClient, data.presenceOrgId, meetingId, data.presenceUserId).catch((err: unknown) => {
            logger.error(
              { err, orgId: data.presenceOrgId, meetingId, userId: data.presenceUserId },
              "presence heartbeat failed",
            );
          });
        }, heartbeatIntervalMs),
      );

      const members = await listPresence(pubClient, data.presenceOrgId, meetingId, staleAfterMs);
      nsp.to(roomName(meetingId)).emit("presence", members);
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
      nsp.to(roomName(payload.meetingId)).emit("chat", {
        meetingId: payload.meetingId,
        userId,
        content: payload.content,
        clientMsgId: payload.clientMsgId,
      });

      try {
        await coreApiClient.postChat({
          orgId,
          meetingId: payload.meetingId,
          userId,
          content: payload.content,
          clientMsgId: payload.clientMsgId,
          bearerToken: data.bearerToken,
        });
      } catch (err) {
        logger.error({ err, orgId, meetingId: payload.meetingId }, "core-api chat persistence call failed");
      }
    });

    socket.on("disconnect", async () => {
      const data = socket.data as SocketData;
      for (const meetingId of data.joinedMeetings) {
        clearInterval(data.heartbeatTimers.get(meetingId));
        data.heartbeatTimers.delete(meetingId);
        try {
          await removeMember(pubClient, data.presenceOrgId, meetingId, data.presenceUserId);
          const members = await listPresence(pubClient, data.presenceOrgId, meetingId, staleAfterMs);
          nsp.to(roomName(meetingId)).emit("presence", members);
          await unsubscribeCaptionChannel(data.presenceOrgId, meetingId);
          await presenceEventsProducer.produce({
            orgId: data.presenceOrgId,
            meetingId,
            userId: data.presenceUserId,
            eventType: "leave",
          });
        } catch (err) {
          logger.error(
            { err, orgId: data.presenceOrgId, meetingId, userId: data.presenceUserId },
            "presence cleanup failed on disconnect",
          );
        }
      }
    });
  });

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
      nsp.to(roomName(meetingId)).emit("presence", members);
      try {
        await presenceEventsProducer.produce({ orgId, meetingId, userId, eventType: "leave" });
      } catch (err) {
        logger.error({ err, orgId, meetingId, userId }, "failed to produce presence-events (leave, from sweep)");
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
      await new Promise<void>((resolve) => io.close(() => resolve()));
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
    schemaRegistryUrl: process.env.SCHEMA_REGISTRY_URL ?? "http://localhost:8081",
    // Optional operational tuning; unset means "use the production default" (see AppConfig doc
    // comments). Exposed via env mainly so tests can drive fast heartbeat/stale/sweep cycles
    // against a real spawned process instead of waiting out the multi-second production defaults.
    presenceHeartbeatIntervalMs: envInt("PRESENCE_HEARTBEAT_INTERVAL_MS"),
    presenceStaleAfterMs: envInt("PRESENCE_STALE_AFTER_MS"),
    presenceSweepIntervalMs: envInt("PRESENCE_SWEEP_INTERVAL_MS"),
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
      console.log(`realtime-gateway listening on :${config.port} (fan-out channel form: ${example})`);
    })
    .catch((err) => {
      // eslint-disable-next-line no-console
      console.error("realtime-gateway failed to start", err);
      process.exit(1);
    });
}
