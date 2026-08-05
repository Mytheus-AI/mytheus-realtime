/**
 * The live presence set: `nexus:{org_id}:presence:{meeting_id}` in Redis. Ephemeral only — this
 * is the sole store; presence is never written to a durable table.
 *
 * Modeled as a Redis ZSET keyed by `user_id`, with the score being the epoch-ms of that member's
 * last heartbeat — not a plain SET. A plain SET is only ever cleaned up by the Socket.io
 * `disconnect` handler running to completion, which requires the OWNING gateway process to still
 * be alive to run it. If a pod is OOM-killed, SIGKILL'd, or evicted, `disconnect` never fires and
 * a plain-SET member leaks forever: a permanently-growing Redis set and a presence list that shows
 * ghost participants who left days ago. The heartbeat score fixes both halves of that:
 *
 *   1. `listPresence` filters by freshness at read time, so a dead pod's members stop appearing
 *      in the live list within `staleAfterMs` of its last heartbeat — no propagation delay, no
 *      dependency on any sweep having run yet.
 *   2. `sweepStalePresence`, run on an interval by every live pod (see `src/server.ts`), actually
 *      removes stale members and reports them back so the caller can broadcast the updated
 *      presence list and produce a `presence-events` "leave" record — this is what bounds the
 *      set's growth and turns the ghost into a real leave, even though the pod that owned that
 *      socket is gone and cannot run its own disconnect handler.
 *
 * A live socket calls `heartbeat()` once on join and again on an interval for as long as it stays
 * joined; `removeMember()` runs immediately on a graceful leave/disconnect. The heartbeat/sweep
 * machinery is the safety net for abnormal pod death, not the primary path for a normal close —
 * a graceful close still cleans up immediately via `removeMember()`.
 */
import type { Redis } from "ioredis";

import { presenceIndexKey, presenceKey } from "../../../libs/keys";

function indexEntry(orgId: string, meetingId: string): string {
  return `${orgId}|${meetingId}`;
}

function parseIndexEntry(entry: string): { orgId: string; meetingId: string } {
  const sep = entry.indexOf("|");
  return { orgId: entry.slice(0, sep), meetingId: entry.slice(sep + 1) };
}

/** Records (or refreshes) `userId`'s heartbeat for `meetingId`, and registers the meeting in the
 * sweep index so a reconciliation pass can find it without scanning every presence key. */
export async function heartbeat(
  redis: Redis,
  orgId: string,
  meetingId: string,
  userId: string,
  now: number = Date.now(),
): Promise<void> {
  await redis.zadd(presenceKey(orgId, meetingId), now, userId);
  await redis.sadd(presenceIndexKey(), indexEntry(orgId, meetingId));
}

/** Immediate, graceful removal (normal join/disconnect path — not the abnormal-death path). */
export async function removeMember(redis: Redis, orgId: string, meetingId: string, userId: string): Promise<void> {
  const key = presenceKey(orgId, meetingId);
  await redis.zrem(key, userId);
  const remaining = await redis.zcard(key);
  if (remaining === 0) {
    await redis.srem(presenceIndexKey(), indexEntry(orgId, meetingId));
  }
}

/** Current live members: everyone whose last heartbeat is within `staleAfterMs` of `now`. A
 * member with no recent heartbeat (its owning pod is gone) is excluded here even before any sweep
 * has physically removed it. */
export async function listPresence(
  redis: Redis,
  orgId: string,
  meetingId: string,
  staleAfterMs: number,
  now: number = Date.now(),
): Promise<string[]> {
  return redis.zrangebyscore(presenceKey(orgId, meetingId), now - staleAfterMs, "+inf");
}

export interface StalePresenceMember {
  orgId: string;
  meetingId: string;
  userId: string;
}

/**
 * Atomically reads and removes the stale members of one presence ZSET, returning exactly the
 * members THIS invocation removed. Must be a single Lua script, not two round trips (`ZRANGEBYSCORE`
 * then `ZREM`): with two round trips, two concurrent callers (e.g. two pods sweeping at roughly
 * the same moment, most likely right after a rolling deploy when many pods' sweep timers are
 * roughly phase-aligned) can both read the same stale member before either removes it, and both
 * then report it reaped — which upstream means two `presence-events` "leave" records for one real
 * transition. Redis executes an EVAL script as a single atomic command, so whichever caller's
 * script runs second sees the member already gone and returns nothing for it — no member can ever
 * be reported reaped by more than one caller. See test/presence.test.ts's concurrency test for a
 * reproduction against a real two-connection race.
 */
const SWEEP_ONE_KEY_SCRIPT = `
local stale = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if #stale > 0 then
  redis.call('ZREM', KEYS[1], unpack(stale))
end
return stale
`;

/**
 * Reaps members whose last heartbeat is older than `staleAfterMs`, across every meeting the index
 * knows has live presence. Returns the reaped members so the caller (see `src/server.ts`) can
 * broadcast an updated presence list and produce a `leave` presence-event for each — this function
 * only prunes storage, it has no knowledge of Socket.io rooms or Kafka. Safe to run concurrently
 * from multiple pods: the read-and-remove of each key is atomic (see `SWEEP_ONE_KEY_SCRIPT`), so a
 * member is reported reaped by at most one caller, never more.
 */
export async function sweepStalePresence(
  redis: Redis,
  staleAfterMs: number,
  now: number = Date.now(),
): Promise<StalePresenceMember[]> {
  const entries = await redis.smembers(presenceIndexKey());
  const cutoff = now - staleAfterMs;
  const reaped: StalePresenceMember[] = [];

  for (const entry of entries) {
    const { orgId, meetingId } = parseIndexEntry(entry);
    const key = presenceKey(orgId, meetingId);
    const stale = (await redis.eval(SWEEP_ONE_KEY_SCRIPT, 1, key, cutoff)) as string[];
    for (const userId of stale) reaped.push({ orgId, meetingId, userId });

    // Non-atomic with the script above, but harmless: SREM on an already-absent member is a
    // no-op, so two callers both observing `remaining === 0` and both calling SREM does no harm
    // (unlike the reaped-member report, there's no downstream "one record per event" consequence
    // to double-report here — it's just index bookkeeping).
    const remaining = await redis.zcard(key);
    if (remaining === 0) {
      await redis.srem(presenceIndexKey(), entry);
    }
  }
  return reaped;
}
