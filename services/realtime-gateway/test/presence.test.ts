/**
 * Presence set behavior against the real dev Redis (localhost:6380), not a mock — this is the
 * store presence lives in and nowhere else (ephemeral, never a durable table).
 *
 * Covers the heartbeat/ZSET mechanics directly (fast, no process spawning): freshness filtering
 * on read, and the reconciliation sweep actually reaping a stale member and pruning the index once
 * a meeting's presence set is empty. The full "a whole pod dies uncleanly" scenario — proving this
 * survives a real `kill -9`, not just a graceful close — is test/presence-pod-death.test.ts.
 */
import { randomUUID } from "node:crypto";

import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { presenceIndexKey, presenceKey } from "../../../libs/keys";
import { heartbeat, listPresence, removeMember, sweepStalePresence } from "../src/presence";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6380";
const STALE_AFTER_MS = 1000;

describe("presence set (real Redis)", () => {
  let redis: Redis;

  beforeAll(() => {
    redis = new Redis(REDIS_URL);
  });

  afterAll(() => {
    redis.disconnect();
  });

  it("grows on heartbeat and shrinks on removeMember, scoped to nexus:{org_id}:presence:{meeting_id}", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const key = presenceKey(orgId, meetingId);

    try {
      expect(await listPresence(redis, orgId, meetingId, STALE_AFTER_MS)).toEqual([]);

      await heartbeat(redis, orgId, meetingId, "user-1");
      expect((await listPresence(redis, orgId, meetingId, STALE_AFTER_MS)).sort()).toEqual([
        "user-1",
      ]);

      await heartbeat(redis, orgId, meetingId, "user-2");
      expect((await listPresence(redis, orgId, meetingId, STALE_AFTER_MS)).sort()).toEqual([
        "user-1",
        "user-2",
      ]);

      // the key is exactly the canonical form, not some other name
      expect(await redis.zcard(key)).toBe(2);

      await removeMember(redis, orgId, meetingId, "user-1");
      expect((await listPresence(redis, orgId, meetingId, STALE_AFTER_MS)).sort()).toEqual([
        "user-2",
      ]);

      await removeMember(redis, orgId, meetingId, "user-2");
      expect(await listPresence(redis, orgId, meetingId, STALE_AFTER_MS)).toEqual([]);
    } finally {
      await redis.del(key);
      await redis.srem(presenceIndexKey(), `${orgId}|${meetingId}`);
    }
  });

  it("a member's own removeMember() prunes the meeting from the sweep index once empty", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const entry = `${orgId}|${meetingId}`;

    await heartbeat(redis, orgId, meetingId, "user-1");
    expect(await redis.sismember(presenceIndexKey(), entry)).toBe(1);

    await removeMember(redis, orgId, meetingId, "user-1");
    expect(await redis.sismember(presenceIndexKey(), entry)).toBe(0);
  });

  it("listPresence excludes a member whose heartbeat is older than staleAfterMs, before any sweep runs", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const key = presenceKey(orgId, meetingId);

    try {
      const oldNow = Date.now() - 10_000; // simulate a heartbeat from 10s ago
      await heartbeat(redis, orgId, meetingId, "ghost-user", oldNow);

      // fresh read-time filtering excludes it immediately — no sweep has run
      expect(await listPresence(redis, orgId, meetingId, STALE_AFTER_MS)).toEqual([]);
      // but the raw ZSET member still physically exists until something sweeps it
      expect(await redis.zcard(key)).toBe(1);
    } finally {
      await redis.del(key);
      await redis.srem(presenceIndexKey(), `${orgId}|${meetingId}`);
    }
  });

  it("sweepStalePresence reaps a stale member, prunes the index when the meeting is left empty, and leaves fresh members alone", async () => {
    const orgId = `org-${randomUUID()}`;
    const staleMeeting = `meeting-stale-${randomUUID()}`;
    const freshMeeting = `meeting-fresh-${randomUUID()}`;

    try {
      await heartbeat(redis, orgId, staleMeeting, "ghost-user", Date.now() - 10_000);
      await heartbeat(redis, orgId, freshMeeting, "live-user"); // now

      const reaped = await sweepStalePresence(redis, STALE_AFTER_MS);
      expect(reaped).toContainEqual({ orgId, meetingId: staleMeeting, userId: "ghost-user" });
      expect(reaped.some((r) => r.meetingId === freshMeeting)).toBe(false);

      // physically gone, not just filtered at read time
      expect(await redis.zcard(presenceKey(orgId, staleMeeting))).toBe(0);
      expect(await redis.sismember(presenceIndexKey(), `${orgId}|${staleMeeting}`)).toBe(0);

      // the fresh meeting is untouched and still indexed
      expect(await listPresence(redis, orgId, freshMeeting, STALE_AFTER_MS)).toEqual(["live-user"]);
      expect(await redis.sismember(presenceIndexKey(), `${orgId}|${freshMeeting}`)).toBe(1);
    } finally {
      await redis.del(presenceKey(orgId, staleMeeting), presenceKey(orgId, freshMeeting));
      await redis.srem(presenceIndexKey(), `${orgId}|${staleMeeting}`, `${orgId}|${freshMeeting}`);
    }
  });

  it("two concurrent sweeps of the same stale entry (simulating two pods) report it reaped exactly once", async () => {
    // Two separate connections, like two separate gateway pods each with their own ioredis
    // client — not two calls sharing one connection, which would trivially serialize on the wire
    // regardless of whether the underlying read-then-remove was atomic.
    const redisA = new Redis(REDIS_URL);
    const redisB = new Redis(REDIS_URL);
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const userId = "ghost-user";

    try {
      await heartbeat(redis, orgId, meetingId, userId, Date.now() - 10_000);

      const [reapedA, reapedB] = await Promise.all([
        sweepStalePresence(redisA, STALE_AFTER_MS),
        sweepStalePresence(redisB, STALE_AFTER_MS),
      ]);

      const matches = (r: { orgId: string; meetingId: string; userId: string }) =>
        r.orgId === orgId && r.meetingId === meetingId && r.userId === userId;
      const totalReports = reapedA.filter(matches).length + reapedB.filter(matches).length;

      // Without the atomic Lua script, both concurrent sweeps could read the member as stale
      // before either removed it, and both would report it — this is the bug the fix round
      // caught. With it, at most one of the two calls can ever see it.
      expect(totalReports).toBe(1);

      // and it really is gone, not just under-reported
      expect(await redis.zscore(presenceKey(orgId, meetingId), userId)).toBeNull();
    } finally {
      redisA.disconnect();
      redisB.disconnect();
      await redis.del(presenceKey(orgId, meetingId));
      await redis.srem(presenceIndexKey(), `${orgId}|${meetingId}`);
    }
  });
});
