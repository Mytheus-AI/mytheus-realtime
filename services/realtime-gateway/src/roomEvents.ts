/**
 * Room-wide events a participant starts (polish program, Phase 0: "Shared candidate ledger",
 * "Agenda and Notes mode", "Room reconnects", and the room's raise hand, reactions and
 * "End for everyone").
 *
 * Every handler here follows the same three rules:
 *
 * 1. MEMBERSHIP FIRST. The socket must have joined this `meetingId` (the join ran core-api's
 *    deny-by-default participant check). Anything else is refused with `NOT_JOINED` and nothing is
 *    broadcast. The org is never read from a payload: the room is `roomName(presenceOrgId, ...)`,
 *    the internal org resolved at join, so a broadcast cannot cross tenants.
 *
 * 2. THE SERVER'S ROW, NOT THE CLIENT'S CLAIM. A resolution ("confirmed", "edited", "dismissed"),
 *    a host agenda change and "End for everyone" are announced only after this gateway re-reads
 *    the stored fact from core-api WITH THE ACTOR'S OWN TOKEN and finds it matches: a confirm is
 *    announced only if core-api holds it confirmed BY THIS PERSON, a dismissal for everyone only if
 *    it is dismissed and this person hosts or created the meeting, an agenda change only if the
 *    item is host-set, an end only if the meeting is ended. The broadcast carries the stored row,
 *    so a client cannot put words in it either. The write itself stays a core-api REST call
 *    (core-api remains the only writer); this is the fan-out of a fact already committed.
 *
 * 3. EPHEMERAL STATE ONLY, IN REDIS. Edit locks and raised hands are Redis hashes under the
 *    `nexus:{org_id}:` prefix (libs/keys.ts), so any pod can serve any socket and losing a pod
 *    loses nothing durable. A lock expires on its own (`LOCK_TTL_MS`, renewed while editing); a
 *    lock or a hand held by a socket that disconnects is released at once.
 *
 * Client -> server (namespace `/v1`; every one acks `{ ok: true, ... } | { ok: false, error }`):
 *   "reauth"             ({ token })                              refresh the bearer token the
 *                                                                 gateway uses on core-api calls
 *   "candidate-resolved" ({ meetingId, candidateId, outcome })    outcome: confirmed|edited|dismissed
 *   "candidate-lock"     ({ meetingId, candidateId, locked })     ack adds `holder` when refused
 *   "agenda-status"      ({ meetingId, itemId })                  after the host's PATCH succeeded
 *   "hand"               ({ meetingId, raised })
 *   "reaction"           ({ meetingId, reaction })                one of REACTIONS
 *   "end-meeting"        ({ meetingId })                          after the host's PATCH succeeded
 *
 * Server -> client, to everyone in the meeting (`{ meetingId, payload }`):
 *   "candidate-resolution" { candidateId, kind, outcome, byUserId, at, row }
 *   "candidate-lock"       { candidateId, byUserId | null, until | null }
 *   "agenda-status"        { item, byUserId, at }
 *   "hand"                 { userId, raised, at }
 *   "reaction"             { id, userId, reaction, at }
 *   "meeting-ended"        { byUserId, at }
 * and to the joining socket only, right after a successful "join":
 *   "room-state"           { locks: [{ candidateId, byUserId, until }], hands: [{ userId, at }] }
 */
import { randomUUID } from "node:crypto";

import type { Redis } from "ioredis";
import type { Logger } from "pino";
import type { Namespace, Socket } from "socket.io";

import type { ClerkJwtVerifier } from "../../../libs/auth";
import { candidateLocksKey, raisedHandsKey, roomName } from "../../../libs/keys";
import type { CoreApiClient, MeetingState } from "./coreApiClient";

export const LOCK_TTL_MS = 90_000;
/** A quiet meeting's lock and hand hashes disappear on their own after this long. */
const HASH_TTL_MS = 6 * 60 * 60 * 1000;
export const REACTION_MIN_INTERVAL_MS = 600;
export const REACTIONS = ["thumbs_up", "clap", "celebrate", "heart", "laugh", "thinking"] as const;
export type Reaction = (typeof REACTIONS)[number];

export type RoomEventError =
  | "INVALID_PAYLOAD"
  | "NOT_JOINED"
  | "NOT_VERIFIED"
  | "NOT_HOST"
  | "LOCKED"
  | "RATE_LIMITED"
  | "UNAUTHENTICATED"
  | "IDENTITY_MISMATCH"
  | "UNAVAILABLE";

export type RoomAck =
  | ({ ok: true } & Record<string, unknown>)
  | ({ ok: false; error: RoomEventError } & Record<string, unknown>);

type AckFn = (res: RoomAck) => void;

export type CandidateOutcome = "confirmed" | "edited" | "dismissed";

export interface RoomSocketData {
  orgId: string;
  userId: string;
  bearerToken: string;
  joinedMeetings: Set<string>;
  presenceOrgId: string;
  presenceUserId: string;
  lastReactionAt?: number;
}

export interface LockEntry {
  userId: string;
  socketId: string;
  until: number;
}

export interface RoomEventsDeps {
  nsp: Namespace;
  redis: Redis;
  coreApi: CoreApiClient;
  verifier: ClerkJwtVerifier;
  logger: Logger;
  now?: () => number;
}

const ID = /^[A-Za-z0-9_-]{1,80}$/;

function isId(v: unknown): v is string {
  return typeof v === "string" && ID.test(v);
}

/** The person hosts the meeting or created it: the same rule core-api applies (ADR-0038 D6). */
export function steers(meeting: MeetingState, internalUserId: string): boolean {
  return meeting.host_user_id === internalUserId || meeting.created_by === internalUserId;
}

/**
 * Whether the stored candidate supports the announcement. Pure, so it is tested on its own.
 * `actorSteers` is only consulted for a dismissal: a dismissal is announced to the room only when
 * the host (or creator) made it; anyone else's "Hide for me" never reaches the gateway at all.
 */
export function verifyResolution(
  row: { status: string; confirmed_by?: string | null },
  outcome: CandidateOutcome,
  actorUserId: string,
  actorSteers: boolean,
): "ok" | "NOT_VERIFIED" | "NOT_HOST" {
  if (outcome === "confirmed") {
    return row.status === "confirmed" && row.confirmed_by === actorUserId ? "ok" : "NOT_VERIFIED";
  }
  if (outcome === "edited") return row.status === "proposed" ? "ok" : "NOT_VERIFIED";
  if (row.status !== "dismissed") return "NOT_VERIFIED";
  return actorSteers ? "ok" : "NOT_HOST";
}

export function parseLock(raw: string | null | undefined): LockEntry | null {
  if (!raw) return null;
  try {
    const v = JSON.parse(raw) as Partial<LockEntry>;
    if (
      typeof v.userId !== "string" ||
      typeof v.socketId !== "string" ||
      typeof v.until !== "number"
    ) {
      return null;
    }
    return { userId: v.userId, socketId: v.socketId, until: v.until };
  } catch {
    return null;
  }
}

// Take or renew a lock atomically: granted when the field is empty, expired, or already this
// user's. Returns the entry now stored (the caller's on success, the holder's when refused).
const TAKE_LOCK = `
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  local ok, d = pcall(cjson.decode, cur)
  if ok and type(d) == 'table' and tonumber(d['until']) and tonumber(d['until']) > tonumber(ARGV[3])
     and d['userId'] ~= ARGV[4] then
    return cur
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return ARGV[2]
`;

// Release a lock only when it is held by this user (or, with ARGV[3] set, by this socket).
const RELEASE_LOCK = `
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if not cur then return 0 end
local ok, d = pcall(cjson.decode, cur)
if not ok or type(d) ~= 'table' then redis.call('HDEL', KEYS[1], ARGV[1]) return 1 end
if d['userId'] == ARGV[2] and (ARGV[3] == '' or d['socketId'] == ARGV[3]) then
  redis.call('HDEL', KEYS[1], ARGV[1])
  return 1
end
return 0
`;

export function createRoomEvents(deps: RoomEventsDeps) {
  const { nsp, redis, coreApi, verifier, logger } = deps;
  const now = deps.now ?? (() => Date.now());

  function room(data: RoomSocketData, meetingId: string) {
    return nsp.to(roomName(data.presenceOrgId, meetingId));
  }

  /** The common gate: a well-formed meeting id this socket has joined. */
  function joined(data: RoomSocketData, meetingId: unknown, ack: AckFn): meetingId is string {
    if (typeof meetingId !== "string" || meetingId.length === 0) {
      ack({ ok: false, error: "INVALID_PAYLOAD" });
      return false;
    }
    if (!data.joinedMeetings.has(meetingId)) {
      ack({ ok: false, error: "NOT_JOINED" });
      return false;
    }
    return true;
  }

  async function releaseLock(
    data: RoomSocketData,
    meetingId: string,
    candidateId: string,
    socketId = "",
  ): Promise<boolean> {
    const released = await redis.eval(
      RELEASE_LOCK,
      1,
      candidateLocksKey(data.presenceOrgId, meetingId),
      candidateId,
      data.presenceUserId,
      socketId,
    );
    return released === 1;
  }

  function attach(socket: Socket): void {
    const data = socket.data as RoomSocketData;
    const safeAck = (ack: unknown): AckFn =>
      typeof ack === "function" ? (ack as AckFn) : () => {};

    socket.on("reauth", async (payload: { token?: unknown }, rawAck?: unknown) => {
      const ack = safeAck(rawAck);
      const token = payload?.token;
      if (typeof token !== "string" || token.length === 0) {
        ack({ ok: false, error: "INVALID_PAYLOAD" });
        return;
      }
      try {
        const identity = await verifier.verify(token);
        // A socket's identity never changes: a token for anyone else is refused outright.
        if (identity.orgId !== data.orgId || identity.userId !== data.userId) {
          ack({ ok: false, error: "IDENTITY_MISMATCH" });
          return;
        }
        data.bearerToken = token;
        ack({ ok: true });
      } catch {
        ack({ ok: false, error: "UNAUTHENTICATED" });
      }
    });

    socket.on(
      "candidate-resolved",
      async (
        payload: { meetingId?: unknown; candidateId?: unknown; outcome?: unknown },
        rawAck?: unknown,
      ) => {
        const ack = safeAck(rawAck);
        const { meetingId, candidateId, outcome } = payload ?? {};
        if (!joined(data, meetingId, ack)) return;
        if (
          !isId(candidateId) ||
          (outcome !== "confirmed" && outcome !== "edited" && outcome !== "dismissed")
        ) {
          ack({ ok: false, error: "INVALID_PAYLOAD" });
          return;
        }
        const row = await coreApi.getCandidate({
          meetingId,
          candidateId,
          bearerToken: data.bearerToken,
        });
        if (!row) {
          ack({ ok: false, error: "NOT_VERIFIED" });
          return;
        }
        let actorSteers = false;
        if (outcome === "dismissed") {
          const meeting = await coreApi.getMeeting({ meetingId, bearerToken: data.bearerToken });
          actorSteers = meeting !== null && steers(meeting, data.presenceUserId);
        }
        const verdict = verifyResolution(row, outcome, data.presenceUserId, actorSteers);
        if (verdict !== "ok") {
          logger.warn(
            { orgId: data.presenceOrgId, meetingId, candidateId, outcome, verdict },
            "candidate resolution not announced: the stored row does not support it",
          );
          ack({ ok: false, error: verdict });
          return;
        }
        // Whoever was editing it, the card is settled (or freshly edited) now: drop any lock.
        try {
          await redis.hdel(candidateLocksKey(data.presenceOrgId, meetingId), candidateId);
        } catch (err) {
          logger.error({ err, meetingId }, "candidate lock release failed");
        }
        const at = new Date(now()).toISOString();
        room(data, meetingId).emit("candidate-resolution", {
          meetingId,
          payload: {
            candidateId,
            kind: row.kind,
            outcome,
            byUserId: data.presenceUserId,
            at,
            row: {
              status: row.status,
              content: row.content,
              rationale: row.rationale ?? null,
              assigneeId: row.assignee_id ?? null,
              dueDate: row.due_date ?? null,
              confirmedBy: row.confirmed_by ?? null,
            },
          },
        });
        room(data, meetingId).emit("candidate-lock", {
          meetingId,
          payload: { candidateId, byUserId: null, until: null },
        });
        ack({ ok: true });
      },
    );

    socket.on(
      "candidate-lock",
      async (
        payload: { meetingId?: unknown; candidateId?: unknown; locked?: unknown },
        rawAck?: unknown,
      ) => {
        const ack = safeAck(rawAck);
        const { meetingId, candidateId, locked } = payload ?? {};
        if (!joined(data, meetingId, ack)) return;
        if (!isId(candidateId) || typeof locked !== "boolean") {
          ack({ ok: false, error: "INVALID_PAYLOAD" });
          return;
        }
        try {
          if (!locked) {
            if (await releaseLock(data, meetingId, candidateId)) {
              room(data, meetingId).emit("candidate-lock", {
                meetingId,
                payload: { candidateId, byUserId: null, until: null },
              });
            }
            ack({ ok: true });
            return;
          }
          const mine: LockEntry = {
            userId: data.presenceUserId,
            socketId: socket.id,
            until: now() + LOCK_TTL_MS,
          };
          const stored = (await redis.eval(
            TAKE_LOCK,
            1,
            candidateLocksKey(data.presenceOrgId, meetingId),
            candidateId,
            JSON.stringify(mine),
            String(now()),
            data.presenceUserId,
            String(HASH_TTL_MS),
          )) as string;
          const holder = parseLock(stored);
          if (!holder || holder.userId !== data.presenceUserId) {
            ack({ ok: false, error: "LOCKED", holder: holder?.userId ?? null });
            return;
          }
          room(data, meetingId).emit("candidate-lock", {
            meetingId,
            payload: { candidateId, byUserId: data.presenceUserId, until: holder.until },
          });
          ack({ ok: true, until: holder.until });
        } catch (err) {
          logger.error({ err, meetingId, candidateId }, "candidate lock failed");
          ack({ ok: false, error: "UNAVAILABLE" });
        }
      },
    );

    socket.on(
      "agenda-status",
      async (payload: { meetingId?: unknown; itemId?: unknown }, rawAck?: unknown) => {
        const ack = safeAck(rawAck);
        const { meetingId, itemId } = payload ?? {};
        if (!joined(data, meetingId, ack)) return;
        if (!isId(itemId)) {
          ack({ ok: false, error: "INVALID_PAYLOAD" });
          return;
        }
        const meeting = await coreApi.getMeeting({ meetingId, bearerToken: data.bearerToken });
        if (!meeting) {
          ack({ ok: false, error: "NOT_VERIFIED" });
          return;
        }
        if (!steers(meeting, data.presenceUserId)) {
          ack({ ok: false, error: "NOT_HOST" });
          return;
        }
        const item = meeting.agenda.find((it) => it.id === itemId);
        if (!item || item.status_source !== "host") {
          ack({ ok: false, error: "NOT_VERIFIED" });
          return;
        }
        room(data, meetingId).emit("agenda-status", {
          meetingId,
          payload: { item, byUserId: data.presenceUserId, at: new Date(now()).toISOString() },
        });
        ack({ ok: true });
      },
    );

    socket.on(
      "hand",
      async (payload: { meetingId?: unknown; raised?: unknown }, rawAck?: unknown) => {
        const ack = safeAck(rawAck);
        const { meetingId, raised } = payload ?? {};
        if (!joined(data, meetingId, ack)) return;
        if (typeof raised !== "boolean") {
          ack({ ok: false, error: "INVALID_PAYLOAD" });
          return;
        }
        const key = raisedHandsKey(data.presenceOrgId, meetingId);
        const at = now();
        try {
          if (raised) {
            await redis.hset(key, data.presenceUserId, JSON.stringify({ socketId: socket.id, at }));
            await redis.pexpire(key, HASH_TTL_MS);
          } else {
            await redis.hdel(key, data.presenceUserId);
          }
        } catch (err) {
          logger.error({ err, meetingId }, "raise hand failed");
          ack({ ok: false, error: "UNAVAILABLE" });
          return;
        }
        room(data, meetingId).emit("hand", {
          meetingId,
          payload: { userId: data.presenceUserId, raised, at: new Date(at).toISOString() },
        });
        ack({ ok: true });
      },
    );

    socket.on(
      "reaction",
      (payload: { meetingId?: unknown; reaction?: unknown }, rawAck?: unknown) => {
        const ack = safeAck(rawAck);
        const { meetingId, reaction } = payload ?? {};
        if (!joined(data, meetingId, ack)) return;
        if (typeof reaction !== "string" || !(REACTIONS as readonly string[]).includes(reaction)) {
          ack({ ok: false, error: "INVALID_PAYLOAD" });
          return;
        }
        const t = now();
        if (
          data.lastReactionAt !== undefined &&
          t - data.lastReactionAt < REACTION_MIN_INTERVAL_MS
        ) {
          ack({ ok: false, error: "RATE_LIMITED" });
          return;
        }
        data.lastReactionAt = t;
        room(data, meetingId).emit("reaction", {
          meetingId,
          payload: {
            id: randomUUID(),
            userId: data.presenceUserId,
            reaction,
            at: new Date(t).toISOString(),
          },
        });
        ack({ ok: true });
      },
    );

    socket.on("end-meeting", async (payload: { meetingId?: unknown }, rawAck?: unknown) => {
      const ack = safeAck(rawAck);
      const meetingId = payload?.meetingId;
      if (!joined(data, meetingId, ack)) return;
      const meeting = await coreApi.getMeeting({ meetingId, bearerToken: data.bearerToken });
      if (!meeting || meeting.status !== "ended") {
        ack({ ok: false, error: "NOT_VERIFIED" });
        return;
      }
      if (!steers(meeting, data.presenceUserId)) {
        ack({ ok: false, error: "NOT_HOST" });
        return;
      }
      try {
        await redis.del(
          candidateLocksKey(data.presenceOrgId, meetingId),
          raisedHandsKey(data.presenceOrgId, meetingId),
        );
      } catch (err) {
        logger.error({ err, meetingId }, "clearing room state on end failed");
      }
      room(data, meetingId).emit("meeting-ended", {
        meetingId,
        payload: { byUserId: data.presenceUserId, at: new Date(now()).toISOString() },
      });
      ack({ ok: true });
    });
  }

  /** Right after a successful join: the live locks and raised hands, to this socket only. */
  async function sendRoomState(socket: Socket, meetingId: string): Promise<void> {
    const data = socket.data as RoomSocketData;
    try {
      const [rawLocks, rawHands] = await Promise.all([
        redis.hgetall(candidateLocksKey(data.presenceOrgId, meetingId)),
        redis.hgetall(raisedHandsKey(data.presenceOrgId, meetingId)),
      ]);
      const t = now();
      const locks = Object.entries(rawLocks)
        .map(([candidateId, raw]) => ({ candidateId, lock: parseLock(raw) }))
        .filter(
          (e): e is { candidateId: string; lock: LockEntry } => e.lock !== null && e.lock.until > t,
        )
        .map((e) => ({ candidateId: e.candidateId, byUserId: e.lock.userId, until: e.lock.until }));
      const hands = Object.entries(rawHands)
        .map(([userId, raw]) => {
          try {
            const v = JSON.parse(raw) as { at?: unknown };
            return typeof v.at === "number" ? { userId, at: new Date(v.at).toISOString() } : null;
          } catch {
            return null;
          }
        })
        .filter((h): h is { userId: string; at: string } => h !== null)
        .sort((a, b) => a.at.localeCompare(b.at));
      socket.emit("room-state", { meetingId, payload: { locks, hands } });
    } catch (err) {
      logger.error({ err, meetingId }, "room-state read failed");
    }
  }

  /**
   * On disconnect: release this socket's edit locks and lower its hand in `meetingId`, and tell
   * the room. Never throws; each step is independent.
   */
  async function releaseOnDisconnect(
    data: RoomSocketData,
    socketId: string,
    meetingId: string,
  ): Promise<void> {
    try {
      const raw = await redis.hgetall(candidateLocksKey(data.presenceOrgId, meetingId));
      for (const [candidateId, value] of Object.entries(raw)) {
        const lock = parseLock(value);
        if (lock?.socketId !== socketId) continue;
        if (await releaseLock(data, meetingId, candidateId, socketId)) {
          room(data, meetingId).emit("candidate-lock", {
            meetingId,
            payload: { candidateId, byUserId: null, until: null },
          });
        }
      }
    } catch (err) {
      logger.error({ err, meetingId }, "lock release on disconnect failed");
    }
    try {
      const key = raisedHandsKey(data.presenceOrgId, meetingId);
      const raw = await redis.hget(key, data.presenceUserId);
      if (raw && (JSON.parse(raw) as { socketId?: string }).socketId === socketId) {
        await redis.hdel(key, data.presenceUserId);
        room(data, meetingId).emit("hand", {
          meetingId,
          payload: {
            userId: data.presenceUserId,
            raised: false,
            at: new Date(now()).toISOString(),
          },
        });
      }
    } catch (err) {
      logger.error({ err, meetingId }, "hand release on disconnect failed");
    }
  }

  return { attach, sendRoomState, releaseOnDisconnect };
}
