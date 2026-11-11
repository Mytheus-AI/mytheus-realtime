/**
 * "After the meeting" layer 1: the "didn't get in" moments the room itself sees, kept for the
 * meeting's after-analysis (the last word).
 *
 * Two kinds, each one JSON entry on the Redis LIST `nexus:{org_id}:room-signals:{meeting_id}`
 * (`roomSignalsKey`; RPUSH, newest MAX_ENTRIES kept, TTL SIGNALS_TTL_MS):
 *
 *   spoken_over      the client saw your unmute spoken over (`floor-attempt`, below): you started
 *                    while someone else held the floor and stopped within seconds while they
 *                    went on. detail { over_user_id, spoke_ms, unmuted_at }.
 *   hand_not_called  a raised hand that came down without being called on: lowered by the host
 *                    (`lowered_by_host`), given up after waiting (`gave_up`, your own lower after
 *                    GAVE_UP_MS), dropped when you left (`left`), or still up when the meeting
 *                    ended (`still_raised_at_end`). detail { outcome, waited_ms }. A hand lowered
 *                    because you were called on (`called`) or started talking (`spoke`) is not one.
 *
 * Entries are private: nothing here is broadcast to the room. The user id is the socket's own
 * (resolved at join), never a payload's; the org is the internal org resolved at join. core-api
 * files the list into `room_signals` once the meeting has ended and the analyzer cross-checks
 * every entry against the transcript (someone who got their point in after all is not nudged).
 */
import { randomUUID } from "node:crypto";

import type { Redis } from "ioredis";

import { roomSignalsKey } from "../../../libs/keys";

export const SIGNALS_TTL_MS = 48 * 60 * 60 * 1000;
export const MAX_ENTRIES = 500;
/** A hand you lower yourself after waiting this long counts as a hand never called. */
export const GAVE_UP_MS = 30_000;
/** `floor-attempt` limits, per socket. */
export const FLOOR_MIN_INTERVAL_MS = 5_000;
export const FLOOR_MAX_PER_MEETING = 40;
export const FLOOR_MAX_AGE_MS = 120_000;
export const FLOOR_MAX_SPAN_MS = 15_000;

export type HandOutcome = "still_raised_at_end" | "lowered_by_host" | "gave_up" | "left";

export interface SignalEntry {
  v: 1;
  id: string;
  kind: "spoken_over" | "hand_not_called";
  user_id: string;
  at: number;
  end: number;
  detail: Record<string, unknown>;
}

const ID = /^[A-Za-z0-9_-]{1,80}$/;

function epochMs(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return Math.round(v);
  if (typeof v === "string") {
    const t = Date.parse(v);
    return Number.isNaN(t) ? null : t;
  }
  return null;
}

/**
 * A `floor-attempt` payload checked against the clock. Pure. `startedAt`/`endedAt` are epoch ms
 * (or ISO); `overUserId` the person who held the floor (optional); `unmutedAt` when the mic came on.
 */
export function floorAttemptEntry(
  payload: { startedAt?: unknown; endedAt?: unknown; overUserId?: unknown; unmutedAt?: unknown },
  userId: string,
  now: number,
): SignalEntry | null {
  const at = epochMs(payload.startedAt);
  const end = epochMs(payload.endedAt);
  if (at === null || end === null || end < at) return null;
  if (end - at > FLOOR_MAX_SPAN_MS) return null;
  if (at < now - FLOOR_MAX_AGE_MS || end > now + 5_000) return null;
  const over = payload.overUserId;
  if (over !== undefined && over !== null && (typeof over !== "string" || !ID.test(over)))
    return null;
  if (over === userId) return null;
  const unmuted =
    payload.unmutedAt === undefined || payload.unmutedAt === null
      ? null
      : epochMs(payload.unmutedAt);
  return {
    v: 1,
    id: randomUUID(),
    kind: "spoken_over",
    user_id: userId,
    at,
    end,
    detail: {
      over_user_id: typeof over === "string" ? over : null,
      spoke_ms: end - at,
      unmuted_at: unmuted,
    },
  };
}

/**
 * Whether a hand coming down is a hand never called, and why. Pure. `forOther`: the host lowered
 * it; `reason`: the lower's reason (`lowered`, `called`, `spoke`, `left`); `raisedAt`/`at` ms.
 */
export function handOutcome(
  reason: string,
  forOther: boolean,
  raisedAt: number,
  at: number,
): HandOutcome | null {
  if (reason === "called" || reason === "spoke") return null;
  if (reason === "left") return "left";
  if (forOther) return "lowered_by_host";
  return at - raisedAt >= GAVE_UP_MS ? "gave_up" : null;
}

export function handEntry(
  userId: string,
  outcome: HandOutcome,
  raisedAt: number,
  at: number,
): SignalEntry {
  return {
    v: 1,
    id: randomUUID(),
    kind: "hand_not_called",
    user_id: userId,
    at: raisedAt,
    end: Math.max(raisedAt, at),
    detail: { outcome, waited_ms: Math.max(0, at - raisedAt) },
  };
}

/** Append entries to the meeting's list, keep the newest MAX_ENTRIES, refresh the TTL. */
export async function recordSignals(
  redis: Redis,
  orgId: string,
  meetingId: string,
  entries: SignalEntry[],
): Promise<void> {
  if (entries.length === 0) return;
  const key = roomSignalsKey(orgId, meetingId);
  await redis
    .multi()
    .rpush(key, ...entries.map((e) => JSON.stringify(e)))
    .ltrim(key, -MAX_ENTRIES, -1)
    .pexpire(key, SIGNALS_TTL_MS)
    .exec();
}
