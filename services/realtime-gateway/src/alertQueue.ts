/**
 * The per-user agent priority queue `nexus:{org_id}:queue:agent:{user_id}` (plan 10-13 section
 * 3.7 step 3; P11 contract section 4).
 *
 * A Redis sorted set of one user's undelivered ProactiveAgent alerts. The score is
 * `priority * 1e13 + epoch ms`, so ascending order yields priority 1 (red conflict) before 2 (yellow)
 * before 3 (blue), and within one priority the oldest first. 1e13 is above any epoch ms this
 * century (1.8e12 today) and 3e13 + 1.8e12 stays far below 2^53, so the score is exact.
 *
 * Flow (server.ts wires it):
 *   1. The pod that consumed the `agent-results` record calls `enqueueAlert` (ZADD + a 24 h
 *      EXPIRE, the "session" TTL) and publishes an `agent-alert-queued` nudge on the alert's
 *      meeting channel `nexus:{org_id}:pubsub:meeting:{meeting_id}`.
 *   2. Every pod receives the nudge. A pod that holds at least one socket of that user on that
 *      channel calls `drainAlerts` for THAT meeting (one atomic Lua script, so two pods racing
 *      never pop the same member) and publishes each popped alert, in order, as an `agent-alert`
 *      message on the channel.
 *   3. Every pod delivers an `agent-alert` message to its local sockets of that user only, so a
 *      second tab on another pod gets it too.
 *   4. A user who is not in the alert's meeting: nobody drains it, the alert stays queued (even
 *      while they sit in another meeting's room). When they join the alert's meeting, the
 *      joining pod drains that meeting's alerts onto its channel. An alert is only ever
 *      delivered into its own meeting, so the room's meeting-scoped re-hydrate agrees with it;
 *      every alert is also in notification_log for the notifications list.
 *   5. Bounded: each enqueue drops members older than 24 h and keeps at most 100 (least urgent,
 *      newest dropped first); the drain skips and removes anything past 24 h.
 *
 * The member is the exact `agent-alert` payload JSON, so a replayed record re-adds the identical
 * member (ZADD is idempotent) instead of queueing a duplicate. Nothing here is durable truth:
 * notification-service's `notification_log` is, and the client re-reads it on every connect.
 */
import type { Redis } from "ioredis";

import { agentQueueKey } from "../../../libs/keys";
import type { AgentAlertPayload } from "./agentOutput";

/** The queue lives for a session: 24 h after its newest alert, then Redis reaps it. */
export const ALERT_QUEUE_TTL_SECONDS = 24 * 60 * 60;
/** An alert older than this is never delivered live (notification_log still has it). */
export const ALERT_MAX_AGE_MS = ALERT_QUEUE_TTL_SECONDS * 1000;
/** At most this many alerts wait per user; past it the least urgent, newest-queued go first. */
export const ALERT_QUEUE_CAP = 100;

const PRIORITY_BAND = 1e13;

export function alertScore(priority: 1 | 2 | 3, createdAtMs: number): number {
  const ms = Number.isFinite(createdAtMs) && createdAtMs > 0 ? Math.floor(createdAtMs) : Date.now();
  return priority * PRIORITY_BAND + ms;
}

/**
 * Removes every member older than `ALERT_MAX_AGE_MS`. The score is `priority * 1e13 + epoch ms`,
 * so "older than the cutoff" is one score range per priority band.
 */
async function purgeExpired(redis: Redis, key: string, now: number): Promise<void> {
  const cutoff = now - ALERT_MAX_AGE_MS;
  const tx = redis.multi();
  for (const p of [1, 2, 3]) {
    tx.zremrangebyscore(key, p * PRIORITY_BAND, `(${p * PRIORITY_BAND + cutoff}`);
  }
  await tx.exec();
}

export async function enqueueAlert(
  redis: Redis,
  orgId: string,
  userId: string,
  alert: AgentAlertPayload,
  now: number = Date.now(),
): Promise<void> {
  const key = agentQueueKey(orgId, userId);
  await purgeExpired(redis, key, now);
  await redis
    .multi()
    .zadd(key, alertScore(alert.priority, Date.parse(alert.createdAt)), JSON.stringify(alert))
    // Bound the queue: keep the CAP most urgent (lowest scores). Past it, a blue note is dropped
    // before any yellow, and a yellow before any red; within a priority, the newest goes first.
    .zremrangebyrank(key, ALERT_QUEUE_CAP, -1)
    .expire(key, ALERT_QUEUE_TTL_SECONDS)
    .exec();
}

/**
 * Atomically removes and returns, in ZSET order (most urgent, then oldest), the queued alerts
 * whose `meetingId` is ARGV[1]. Alerts for other meetings stay queued. A member older than the
 * cutoff (ARGV[2], epoch ms; the age is the score modulo 1e13) or one that is not a JSON object
 * is removed and not returned. Bounded by the queue cap, so one call is a small, fixed cost.
 */
const DRAIN_FOR_MEETING = `
local entries = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
local cutoff = tonumber(ARGV[2])
local out = {}
for i = 1, #entries, 2 do
  local member = entries[i]
  local ms = tonumber(entries[i + 1]) % ${PRIORITY_BAND}
  if ms < cutoff then
    redis.call('ZREM', KEYS[1], member)
  else
    local ok, decoded = pcall(cjson.decode, member)
    if not ok or type(decoded) ~= 'table' then
      redis.call('ZREM', KEYS[1], member)
    elseif decoded['meetingId'] == ARGV[1] then
      redis.call('ZREM', KEYS[1], member)
      out[#out + 1] = member
    end
  end
end
return out
`;

/**
 * Pops this user's queued alerts about `meetingId`, most urgent first. Only those: an alert is
 * delivered into its own meeting's room and nowhere else, so the room's meeting-scoped
 * re-hydrate (`GET /notifications?meeting_id=`) always agrees with what the socket showed.
 * Alerts about other meetings wait for the user to join that meeting (or expire after 24 h).
 */
export async function drainAlerts(
  redis: Redis,
  orgId: string,
  userId: string,
  meetingId: string,
  now: number = Date.now(),
): Promise<AgentAlertPayload[]> {
  const key = agentQueueKey(orgId, userId);
  const raw = (await redis.eval(
    DRAIN_FOR_MEETING,
    1,
    key,
    meetingId,
    String(now - ALERT_MAX_AGE_MS),
  )) as string[];
  const out: AgentAlertPayload[] = [];
  for (const member of raw ?? []) {
    try {
      const parsed = JSON.parse(member) as AgentAlertPayload;
      if (parsed && typeof parsed.id === "string") out.push(parsed);
    } catch {
      // the script already removed it; nothing to deliver
    }
  }
  return out;
}
