/**
 * The live presence set: `nexus:{org_id}:presence:{meeting_id}` in Redis. Ephemeral only — this
 * is the sole store; presence is never written to a durable table.
 */
import type { Redis } from "ioredis";

import { presenceKey } from "../../../libs/keys";

export async function addPresence(
  redis: Redis,
  orgId: string,
  meetingId: string,
  userId: string,
): Promise<string[]> {
  const key = presenceKey(orgId, meetingId);
  await redis.sadd(key, userId);
  return redis.smembers(key);
}

export async function removePresence(
  redis: Redis,
  orgId: string,
  meetingId: string,
  userId: string,
): Promise<string[]> {
  const key = presenceKey(orgId, meetingId);
  await redis.srem(key, userId);
  return redis.smembers(key);
}

export async function listPresence(redis: Redis, orgId: string, meetingId: string): Promise<string[]> {
  return redis.smembers(presenceKey(orgId, meetingId));
}
