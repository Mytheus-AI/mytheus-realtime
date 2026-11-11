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
 *   "agenda-status"      ({ meetingId, itemId } | { meetingId, itemIds, replaced? })
 *                                                                 after the host's PATCH (mark,
 *                                                                 extend) or "Start next" succeeded;
 *                                                                 `replaced: true` after the host
 *                                                                 replaced the whole agenda (PUT)
 *   "hand"               ({ meetingId, raised, userId?, reason? })
 *                                                                 your own hand; or, with `userId`
 *                                                                 of someone else, the host (or
 *                                                                 creator) lowering it: `raised`
 *                                                                 false, `reason` "lowered" or
 *                                                                 "called" (called on to speak).
 *                                                                 Your own lower may say "spoke"
 *                                                                 (lowered as you started talking)
 *   "reaction"           ({ meetingId, reaction })                one of REACTIONS
 *   "end-meeting"        ({ meetingId })                          after the host's PATCH succeeded
 *   "wrap-up"            ({ meetingId })                          after the host opened or
 *                                                                 cancelled the 60 s wrap-up
 *   "floor-attempt"      ({ meetingId, startedAt, endedAt, overUserId?, unmutedAt? })
 *                                                                 your unmute was spoken over
 *                                                                 (epoch ms); kept privately for
 *                                                                 the last word (roomSignals.ts),
 *                                                                 never broadcast
 *   "resume-point"       ({ meetingId, resumePointId })           after POST /resume-points/:id/
 *                                                                 continue or dismiss succeeded
 *
 * Server -> client, to everyone in the meeting (`{ meetingId, payload }`):
 *   "candidate-resolution" { candidateId, kind, outcome, byUserId, at, row }
 *   "candidate-lock"       { candidateId, byUserId | null, until | null }
 *   "agenda-status"        { item, items, byUserId, at, replaced? }
 *                                                          `items`: every stored row named, in
 *                                                          agenda order; `item` is the first
 *                                                          (kept for clients that read one). With
 *                                                          `replaced: true`, `items` is the whole
 *                                                          stored agenda, to be taken as it is
 *   "hand"                 { userId, raised, at, raisedAt, byUserId, reason }
 *                                                          `raisedAt`: when the hand went up (the
 *                                                          queue order; a second raise keeps it);
 *                                                          `at`: when this change happened;
 *                                                          `reason`: raised | lowered | spoke |
 *                                                          called | left
 *   "reaction"             { id, userId, reaction, at }
 *   "meeting-ended"        { byUserId, at }
 *   "wrap-up"              { state: "started" | "cancelled", startedAt, endsAt, byUserId, at }
 *   "resume-point"         { id, status: "continued" | "dismissed", byUserId, at }
 *                                                          only when core-api, read with the
 *                                                          actor's token, holds this meeting's
 *                                                          incoming point resolved
 * and to the joining socket only, right after a successful "join":
 *   "room-state"           { locks: [{ candidateId, byUserId, until }], hands: [{ userId, at }] }
 *                                                          hands oldest first: the queue
 *
 * A raised hand survives a short drop: when its socket goes, the hand is lowered only if that
 * person has not re-joined within HAND_GRACE_MS (a re-join takes the hand over, place kept).
 *
 * The last word ("After the meeting" layer 1): a hand that comes down without being called on
 * (lowered by the host, given up after 30 s, dropped on leaving, still up at "End for everyone")
 * and an unmute spoken over (`floor-attempt`) are kept privately on the meeting's room-signals
 * list (roomSignals.ts). Keeping one never blocks or fails the event that caused it.
 */
import { randomUUID } from "node:crypto";

import type { Redis } from "ioredis";
import type { Logger } from "pino";
import type { Namespace, Socket } from "socket.io";

import type { ClerkJwtVerifier } from "../../../libs/auth";
import { candidateLocksKey, raisedHandsKey, roomName } from "../../../libs/keys";
import type { CoreApiClient, MeetingState } from "./coreApiClient";
import {
  FLOOR_MAX_PER_MEETING,
  FLOOR_MIN_INTERVAL_MS,
  floorAttemptEntry,
  handEntry,
  handOutcome,
  recordSignals,
  type SignalEntry,
} from "./roomSignals";

export const LOCK_TTL_MS = 90_000;
/** A quiet meeting's lock and hand hashes disappear on their own after this long. */
const HASH_TTL_MS = 6 * 60 * 60 * 1000;
export const REACTION_MIN_INTERVAL_MS = 600;
/** How long a raised hand waits for its owner to re-join after their socket drops. */
export const HAND_GRACE_MS = 15_000;
export type HandReason = "raised" | "lowered" | "spoke" | "called" | "left";
const OWN_LOWER_REASONS: readonly string[] = ["lowered", "spoke"];
const HOST_LOWER_REASONS: readonly string[] = ["lowered", "called"];
/** The close-out wrap-up's length; the deadline is the stored start plus this. */
export const WRAP_UP_MS = 60_000;
/** "Start next" touches at most two items; a full-agenda update is still bounded (ADR-0038 D5). */
const MAX_AGENDA_IDS = 20;
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
  /** `floor-attempt` rate limit: the last one and how many per meeting, on this socket. */
  lastFloorAttemptAt?: number;
  floorAttempts?: Map<string, number>;
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
  /** Overrides HAND_GRACE_MS (tests). */
  handGraceMs?: number;
}

export interface HandEntry {
  socketId: string;
  /** When the hand went up (ms). */
  at: number;
}

export function parseHand(raw: string | null | undefined): HandEntry | null {
  if (!raw) return null;
  try {
    const v = JSON.parse(raw) as Partial<HandEntry>;
    if (typeof v.socketId !== "string" || typeof v.at !== "number") return null;
    return { socketId: v.socketId, at: v.at };
  } catch {
    return null;
  }
}

/**
 * What a "hand" request may do. Pure, so the rules are tested on their own: your own hand up or
 * down (down may say "spoke"); someone else's only down, and only by the host or creator, saying
 * "lowered" or "called". Returns the target and the reason, or the refusal.
 */
export function handIntent(
  payload: { raised?: unknown; userId?: unknown; reason?: unknown },
  actorUserId: string,
):
  | { ok: true; target: string; raised: boolean; reason: HandReason; forOther: boolean }
  | { ok: false; error: "INVALID_PAYLOAD" } {
  const { raised, userId, reason } = payload;
  if (typeof raised !== "boolean") return { ok: false, error: "INVALID_PAYLOAD" };
  if (userId !== undefined && userId !== null && !isId(userId))
    return { ok: false, error: "INVALID_PAYLOAD" };
  const target = typeof userId === "string" ? userId : actorUserId;
  const forOther = target !== actorUserId;
  if (raised) {
    if (forOther || (reason !== undefined && reason !== "raised"))
      return { ok: false, error: "INVALID_PAYLOAD" };
    return { ok: true, target, raised, reason: "raised", forOther };
  }
  const allowed = forOther ? HOST_LOWER_REASONS : OWN_LOWER_REASONS;
  if (reason !== undefined && (typeof reason !== "string" || !allowed.includes(reason))) {
    return { ok: false, error: "INVALID_PAYLOAD" };
  }
  return {
    ok: true,
    target,
    raised,
    reason: (reason as HandReason | undefined) ?? "lowered",
    forOther,
  };
}

const ID = /^[A-Za-z0-9_-]{1,80}$/;

function isId(v: unknown): v is string {
  return typeof v === "string" && ID.test(v);
}

function iso(ms: number): string {
  return new Date(ms).toISOString();
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

// Raise a hand: keep the stored raise time when the hand is already up (re-pointing it at this
// socket), else store the new one. Returns the entry now stored.
const RAISE_HAND = `
local cur = redis.call('HGET', KEYS[1], ARGV[1])
local out = ARGV[2]
if cur then
  local ok, d = pcall(cjson.decode, cur)
  if ok and type(d) == 'table' and tonumber(d['at']) then
    out = cjson.encode({ socketId = ARGV[3], at = tonumber(d['at']) })
  end
end
redis.call('HSET', KEYS[1], ARGV[1], out)
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return out
`;

// Take a hand over for a re-joined socket, keeping its raise time; nothing when it is down.
const REBIND_HAND = `
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if not cur then return 0 end
local ok, d = pcall(cjson.decode, cur)
if not ok or type(d) ~= 'table' or not tonumber(d['at']) then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode({ socketId = ARGV[2], at = tonumber(d['at']) }))
return 1
`;

// Lower a hand only if it is still held by the socket that left.
const DROP_HAND_IF = `
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if not cur then return 0 end
local ok, d = pcall(cjson.decode, cur)
if ok and type(d) == 'table' and d['socketId'] ~= ARGV[2] then return 0 end
redis.call('HDEL', KEYS[1], ARGV[1])
return 1
`;

export function createRoomEvents(deps: RoomEventsDeps) {
  const { nsp, redis, coreApi, verifier, logger } = deps;
  const now = deps.now ?? (() => Date.now());
  const handGraceMs = deps.handGraceMs ?? HAND_GRACE_MS;
  const graceTimers = new Set<ReturnType<typeof setTimeout>>();
  let stopped = false;

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

  /** Keep "didn't get in" signals for the last word; never throws, never blocks the caller. */
  async function keep(
    data: RoomSocketData,
    meetingId: string,
    entries: SignalEntry[],
  ): Promise<void> {
    try {
      await recordSignals(redis, data.presenceOrgId, meetingId, entries);
    } catch (err) {
      logger.error({ err, meetingId }, "keeping room signals failed");
    }
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
      async (
        payload: { meetingId?: unknown; itemId?: unknown; itemIds?: unknown; replaced?: unknown },
        rawAck?: unknown,
      ) => {
        const ack = safeAck(rawAck);
        const { meetingId, itemId, itemIds } = payload ?? {};
        const replaced = payload?.replaced === true;
        if (!joined(data, meetingId, ack)) return;
        // One item (a mark or an extend) or several ("Start next" finishes one and starts the
        // next): every id must be well formed, and there must be at least one.
        const ids = Array.isArray(itemIds) ? itemIds : [itemId];
        if (ids.length === 0 || ids.length > MAX_AGENDA_IDS || !ids.every(isId)) {
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
        const wanted = new Set(ids);
        const items = meeting.agenda.filter((it) => wanted.has(it.id));
        // Every named item must exist in the stored agenda and, for a status change, be host-set
        // there, or nothing is said. A replaced agenda (the host's PUT) is new rows, set by
        // nobody yet: it is announced whole, as stored.
        if (
          items.length !== wanted.size ||
          (!replaced && items.some((it) => it.status_source !== "host"))
        ) {
          ack({ ok: false, error: "NOT_VERIFIED" });
          return;
        }
        const announced = replaced
          ? [...meeting.agenda].sort((a, b) => Number(a.position ?? 0) - Number(b.position ?? 0))
          : items;
        room(data, meetingId).emit("agenda-status", {
          meetingId,
          payload: {
            item: announced[0],
            items: announced,
            byUserId: data.presenceUserId,
            at: new Date(now()).toISOString(),
            ...(replaced ? { replaced: true } : {}),
          },
        });
        ack({ ok: true });
      },
    );

    socket.on(
      "hand",
      async (
        payload: { meetingId?: unknown; raised?: unknown; userId?: unknown; reason?: unknown },
        rawAck?: unknown,
      ) => {
        const ack = safeAck(rawAck);
        const meetingId = payload?.meetingId;
        if (!joined(data, meetingId, ack)) return;
        const intent = handIntent(payload ?? {}, data.presenceUserId);
        if (!intent.ok) {
          ack({ ok: false, error: intent.error });
          return;
        }
        // Lowering someone else's hand is the host's (or creator's), as core-api decides it.
        if (intent.forOther) {
          const meeting = await coreApi.getMeeting({ meetingId, bearerToken: data.bearerToken });
          if (!meeting) {
            ack({ ok: false, error: "NOT_VERIFIED" });
            return;
          }
          if (!steers(meeting, data.presenceUserId)) {
            ack({ ok: false, error: "NOT_HOST" });
            return;
          }
        }
        const key = raisedHandsKey(data.presenceOrgId, meetingId);
        const t = now();
        let raisedAt: number;
        try {
          if (intent.raised) {
            // A second raise (another tab, a retry) keeps the place in the queue.
            const kept = (await redis.eval(
              RAISE_HAND,
              1,
              key,
              intent.target,
              JSON.stringify({ socketId: socket.id, at: t }),
              socket.id,
              String(HASH_TTL_MS),
            )) as string;
            raisedAt = parseHand(kept)?.at ?? t;
          } else {
            const prev = parseHand(await redis.hget(key, intent.target));
            const removed = await redis.hdel(key, intent.target);
            if (removed === 0) {
              // Already down: nothing to tell the room.
              ack({ ok: true, changed: false });
              return;
            }
            raisedAt = prev?.at ?? t;
          }
        } catch (err) {
          logger.error({ err, meetingId }, "raise hand failed");
          ack({ ok: false, error: "UNAVAILABLE" });
          return;
        }
        if (!intent.raised) {
          const outcome = handOutcome(intent.reason, intent.forOther, raisedAt, t);
          if (outcome) void keep(data, meetingId, [handEntry(intent.target, outcome, raisedAt, t)]);
        }
        room(data, meetingId).emit("hand", {
          meetingId,
          payload: {
            userId: intent.target,
            raised: intent.raised,
            at: iso(intent.raised ? raisedAt : t),
            raisedAt: iso(raisedAt),
            byUserId: data.presenceUserId,
            reason: intent.reason,
          },
        });
        ack({ ok: true, raisedAt: iso(raisedAt) });
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

    socket.on("wrap-up", async (payload: { meetingId?: unknown }, rawAck?: unknown) => {
      const ack = safeAck(rawAck);
      const meetingId = payload?.meetingId;
      if (!joined(data, meetingId, ack)) return;
      const meeting = await coreApi.getMeeting({ meetingId, bearerToken: data.bearerToken });
      if (!meeting) {
        ack({ ok: false, error: "NOT_VERIFIED" });
        return;
      }
      if (!steers(meeting, data.presenceUserId)) {
        ack({ ok: false, error: "NOT_HOST" });
        return;
      }
      // The stored wrap-up decides what the room hears: open (with its deadline) or closed.
      const announced = wrapUpPayload(meeting, data.presenceUserId, now());
      if (!announced) {
        ack({ ok: false, error: "NOT_VERIFIED" });
        return;
      }
      room(data, meetingId).emit("wrap-up", { meetingId, payload: announced });
      ack({ ok: true, state: announced.state });
    });

    socket.on(
      "floor-attempt",
      async (
        payload: {
          meetingId?: unknown;
          startedAt?: unknown;
          endedAt?: unknown;
          overUserId?: unknown;
          unmutedAt?: unknown;
        },
        rawAck?: unknown,
      ) => {
        const ack = safeAck(rawAck);
        const meetingId = payload?.meetingId;
        if (!joined(data, meetingId, ack)) return;
        const t = now();
        const counts = (data.floorAttempts ??= new Map<string, number>());
        if (
          (data.lastFloorAttemptAt !== undefined &&
            t - data.lastFloorAttemptAt < FLOOR_MIN_INTERVAL_MS) ||
          (counts.get(meetingId) ?? 0) >= FLOOR_MAX_PER_MEETING
        ) {
          ack({ ok: false, error: "RATE_LIMITED" });
          return;
        }
        const entry = floorAttemptEntry(payload ?? {}, data.presenceUserId, t);
        if (!entry) {
          ack({ ok: false, error: "INVALID_PAYLOAD" });
          return;
        }
        data.lastFloorAttemptAt = t;
        counts.set(meetingId, (counts.get(meetingId) ?? 0) + 1);
        try {
          await recordSignals(redis, data.presenceOrgId, meetingId, [entry]);
        } catch (err) {
          logger.error({ err, meetingId }, "keeping a floor attempt failed");
          ack({ ok: false, error: "UNAVAILABLE" });
          return;
        }
        ack({ ok: true });
      },
    );

    socket.on(
      "resume-point",
      async (payload: { meetingId?: unknown; resumePointId?: unknown }, rawAck?: unknown) => {
        const ack = safeAck(rawAck);
        const { meetingId, resumePointId } = payload ?? {};
        if (!joined(data, meetingId, ack)) return;
        if (!isId(resumePointId)) {
          ack({ ok: false, error: "INVALID_PAYLOAD" });
          return;
        }
        // The stored point decides what the room hears: only this meeting's incoming point, and
        // only once core-api holds it continued or set aside.
        const read = await coreApi.getResumePoint({ meetingId, bearerToken: data.bearerToken });
        const point = read?.incoming;
        if (
          !point ||
          point.id !== resumePointId ||
          (point.status !== "continued" && point.status !== "dismissed")
        ) {
          ack({ ok: false, error: "NOT_VERIFIED" });
          return;
        }
        room(data, meetingId).emit("resume-point", {
          meetingId,
          payload: {
            id: point.id,
            status: point.status,
            byUserId: data.presenceUserId,
            at: iso(now()),
          },
        });
        ack({ ok: true, status: point.status });
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
        const t = now();
        const up = await redis.hgetall(raisedHandsKey(data.presenceOrgId, meetingId));
        const entries = Object.entries(up).flatMap(([userId, raw]) => {
          const hand = parseHand(raw);
          return hand ? [handEntry(userId, "still_raised_at_end", hand.at, t)] : [];
        });
        await keep(data, meetingId, entries);
      } catch (err) {
        logger.error({ err, meetingId }, "reading raised hands on end failed");
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

  /** The stored wrap-up as the room hears it, or null when the meeting is over. Pure. */
  function wrapUpPayload(meeting: MeetingState, byUserId: string, at: number) {
    if (meeting.status === "ended" || meeting.status === "cancelled") return null;
    const started = meeting.wrap_up_started_at ? Date.parse(meeting.wrap_up_started_at) : NaN;
    if (Number.isNaN(started)) {
      return { state: "cancelled", startedAt: null, endsAt: null, byUserId, at: iso(at) };
    }
    return {
      state: "started",
      startedAt: iso(started),
      endsAt: iso(started + WRAP_UP_MS),
      byUserId,
      at: iso(at),
    };
  }

  /** Right after a successful join: the live locks and raised hands, to this socket only. */
  async function sendRoomState(socket: Socket, meetingId: string): Promise<void> {
    const data = socket.data as RoomSocketData;
    try {
      // Back after a drop: a hand still up is this socket's now, its place in the queue kept.
      await redis.eval(
        REBIND_HAND,
        1,
        raisedHandsKey(data.presenceOrgId, meetingId),
        data.presenceUserId,
        socket.id,
      );
    } catch (err) {
      logger.error({ err, meetingId }, "hand rebind failed");
    }
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
      const held = parseHand(await redis.hget(key, data.presenceUserId));
      if (held?.socketId === socketId && stopped) {
        // Shutting down: no timer outlives the pod; the hash's TTL tidies up.
      } else if (held?.socketId === socketId) {
        // A short drop keeps the hand: lower it only if they have not re-joined by then.
        const timer = setTimeout(() => {
          graceTimers.delete(timer);
          void lowerIfStillGone(data, socketId, meetingId);
        }, handGraceMs);
        timer.unref?.();
        graceTimers.add(timer);
      }
    } catch (err) {
      logger.error({ err, meetingId }, "hand release on disconnect failed");
    }
  }

  async function lowerIfStillGone(data: RoomSocketData, socketId: string, meetingId: string) {
    try {
      const key = raisedHandsKey(data.presenceOrgId, meetingId);
      const held = parseHand(await redis.hget(key, data.presenceUserId));
      const dropped = await redis.eval(DROP_HAND_IF, 1, key, data.presenceUserId, socketId);
      if (dropped !== 1) return;
      const t = now();
      if (held) void keep(data, meetingId, [handEntry(data.presenceUserId, "left", held.at, t)]);
      room(data, meetingId).emit("hand", {
        meetingId,
        payload: {
          userId: data.presenceUserId,
          raised: false,
          at: iso(t),
          raisedAt: iso(held?.at ?? t),
          byUserId: data.presenceUserId,
          reason: "left",
        },
      });
    } catch (err) {
      logger.error({ err, meetingId }, "hand release after grace failed");
    }
  }

  /** Stops pending hand-release timers (shutdown). */
  function stop(): void {
    stopped = true;
    for (const timer of graceTimers) clearTimeout(timer);
    graceTimers.clear();
  }

  return { attach, sendRoomState, releaseOnDisconnect, stop };
}
