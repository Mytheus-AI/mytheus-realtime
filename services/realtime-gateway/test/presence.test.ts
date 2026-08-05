/**
 * Presence set behavior against the real dev Redis (localhost:6380), not a mock — this is the
 * store presence lives in and nowhere else (ephemeral, never a durable table).
 */
import { randomUUID } from "node:crypto";

import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { presenceKey } from "../../../libs/keys";
import { addPresence, listPresence, removePresence } from "../src/presence";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6380";

describe("presence set (real Redis)", () => {
  let redis: Redis;

  beforeAll(() => {
    redis = new Redis(REDIS_URL);
  });

  afterAll(async () => {
    redis.disconnect();
  });

  it("grows on join and shrinks on leave, and is scoped to nexus:{org_id}:presence:{meeting_id}", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const key = presenceKey(orgId, meetingId);

    try {
      expect(await listPresence(redis, orgId, meetingId)).toEqual([]);

      const afterJoin1 = await addPresence(redis, orgId, meetingId, "user-1");
      expect(afterJoin1.sort()).toEqual(["user-1"]);

      const afterJoin2 = await addPresence(redis, orgId, meetingId, "user-2");
      expect(afterJoin2.sort()).toEqual(["user-1", "user-2"]);

      // the key is exactly the canonical form, not some other name
      expect(await redis.smembers(key)).toHaveLength(2);

      const afterLeave = await removePresence(redis, orgId, meetingId, "user-1");
      expect(afterLeave.sort()).toEqual(["user-2"]);

      const afterLastLeave = await removePresence(redis, orgId, meetingId, "user-2");
      expect(afterLastLeave).toEqual([]);
    } finally {
      await redis.del(key);
    }
  });
});
