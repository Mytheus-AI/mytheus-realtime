/**
 * The per-user agent priority queue `nexus:{org_id}:queue:agent:{user_id}` (plan 10-13 section
 * 3.7 step 3; P11 contract section 4).
 *
 * A Redis sorted set of one user's undelivered ProactiveAgent alerts. The score is
 * `priority * 1e13 + epoch ms`, so ZPOPMIN yields priority 1 (red conflict) before 2 (yellow)
 * before 3 (blue), and within one priority the oldest first. 1e13 is above any epoch ms this
 * century (1.8e12 today) and 3e13 + 1.8e12 stays far below 2^53, so the score is exact.
 *
 * Flow (server.ts wires it):
 *   1. The pod that consumed the `agent-results` record calls `enqueueAlert` (ZADD + a 24 h
 *      EXPIRE, the "session" TTL) and publishes an `agent-alert-queued` nudge on the alert's
 *      meeting channel `nexus:{org_id}:pubsub:meeting:{meeting_id}`.
 *   2. Every pod receives the nudge. A pod that holds at least one socket of that user on that
 *      channel calls `drainAlerts` (ZPOPMIN, atomic: two pods racing never pop the same member)
 *      and publishes each popped alert, in order, as an `agent-alert` message on the channel.
 *   3. Every pod delivers an `agent-alert` message to its local sockets of that user only, so a
 *      second tab on another pod gets it too.
 *   4. A user with no socket anywhere: nobody drains, the alert stays queued. When that user
 *      joins a meeting room, the joining pod drains and publishes on the joined channel.
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
/** How many alerts one ZPOPMIN takes; a drain loops until the queue is empty. */
const DRAIN_BATCH = 50;

export function alertScore(priority: 1 | 2 | 3, createdAtMs: number): number {
  const ms = Number.isFinite(createdAtMs) && createdAtMs > 0 ? Math.floor(createdAtMs) : Date.now();
  return priority * 1e13 + ms;
}

export async function enqueueAlert(
  redis: Redis,
  orgId: string,
  userId: string,
  alert: AgentAlertPayload,
): Promise<void> {
  const key = agentQueueKey(orgId, userId);
  await redis
    .multi()
    .zadd(key, alertScore(alert.priority, Date.parse(alert.createdAt)), JSON.stringify(alert))
    .expire(key, ALERT_QUEUE_TTL_SECONDS)
    .exec();
}

/** Pops everything queued for this user, most urgent first. A malformed member is dropped. */
export async function drainAlerts(
  redis: Redis,
  orgId: string,
  userId: string,
): Promise<AgentAlertPayload[]> {
  const key = agentQueueKey(orgId, userId);
  const out: AgentAlertPayload[] = [];
  for (;;) {
    // [member, score, member, score, ...] in ascending score order
    const flat = await redis.zpopmin(key, DRAIN_BATCH);
    for (let i = 0; i < flat.length; i += 2) {
      try {
        const parsed = JSON.parse(flat[i]) as AgentAlertPayload;
        if (parsed && typeof parsed.id === "string") out.push(parsed);
      } catch {
        // not ours to deliver; dropping it keeps the queue draining
      }
    }
    if (flat.length < DRAIN_BATCH * 2) return out;
  }
}
