/**
 * The last word's room signals (src/roomSignals.ts) and "pick up where we stopped"'s resume-point
 * announcement (src/roomEvents.ts): a hand that came down without being called on and an unmute
 * that was spoken over are kept privately on `nexus:{org_id}:room-signals:{meeting_id}` and never
 * broadcast; a resume point is announced only when core-api holds this meeting's incoming point
 * resolved. One real App (real Socket.io + real Redis at :6390), the fake JWKS and core-api.
 */
import { randomUUID } from "node:crypto";

import { Redis } from "ioredis";
import type { Socket as ClientSocket } from "socket.io-client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { roomSignalsKey } from "../../../libs/keys";
import {
  FLOOR_MAX_SPAN_MS,
  GAVE_UP_MS,
  floorAttemptEntry,
  handOutcome,
  type SignalEntry,
} from "../src/roomSignals";
import type { JoinAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";

type Ack = { ok: boolean; error?: string; [k: string]: unknown };

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 2000,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs)
      throw new Error("waitFor: condition not met within timeout");
    await sleep(20);
  }
}

function emitAck(socket: ClientSocket, event: string, payload: unknown): Promise<Ack> {
  return new Promise((resolve) => socket.emit(event, payload, resolve));
}

function collect(socket: ClientSocket, event: string): unknown[] {
  const seen: unknown[] = [];
  socket.on(event, (msg: unknown) => seen.push(msg));
  return seen;
}

describe("floorAttemptEntry and handOutcome (pure)", () => {
  const now = 1_800_000_000_000;
  it("keeps a recent, short attempt with who held the floor", () => {
    const e = floorAttemptEntry(
      { startedAt: now - 4000, endedAt: now - 2500, overUserId: "u-maya", unmutedAt: now - 6000 },
      "u-raj",
      now,
    );
    expect(e).toMatchObject({
      v: 1,
      kind: "spoken_over",
      user_id: "u-raj",
      at: now - 4000,
      end: now - 2500,
      detail: { over_user_id: "u-maya", spoke_ms: 1500, unmuted_at: now - 6000 },
    });
  });
  it("refuses attempts that are too old, too long, backwards, malformed or over yourself", () => {
    expect(
      floorAttemptEntry({ startedAt: now - 200_000, endedAt: now - 199_000 }, "u", now),
    ).toBeNull();
    expect(
      floorAttemptEntry(
        { startedAt: now - 30_000, endedAt: now - 30_000 + FLOOR_MAX_SPAN_MS + 1 },
        "u",
        now,
      ),
    ).toBeNull();
    expect(floorAttemptEntry({ startedAt: now - 1000, endedAt: now - 2000 }, "u", now)).toBeNull();
    expect(floorAttemptEntry({ startedAt: "soon", endedAt: now }, "u", now)).toBeNull();
    expect(
      floorAttemptEntry(
        { startedAt: now - 2000, endedAt: now - 1000, overUserId: "bad id!" },
        "u",
        now,
      ),
    ).toBeNull();
    expect(
      floorAttemptEntry({ startedAt: now - 2000, endedAt: now - 1000, overUserId: "u" }, "u", now),
    ).toBeNull();
    expect(
      floorAttemptEntry(
        { startedAt: new Date(now - 2000).toISOString(), endedAt: now - 1000 },
        "u",
        now,
      ),
    ).not.toBeNull();
  });
  it("says which lowered hands were never called on", () => {
    expect(handOutcome("called", true, 0, 60_000)).toBeNull();
    expect(handOutcome("spoke", false, 0, 60_000)).toBeNull();
    expect(handOutcome("lowered", true, 0, 1000)).toBe("lowered_by_host");
    expect(handOutcome("left", false, 0, 1000)).toBe("left");
    expect(handOutcome("lowered", false, 0, GAVE_UP_MS - 1)).toBeNull();
    expect(handOutcome("lowered", false, 0, GAVE_UP_MS)).toBe("gave_up");
  });
});

describe("room signals and the resume point", () => {
  let t: TestApp;
  let redis: Redis;

  beforeAll(async () => {
    t = await startTestApp({ handGraceMs: 200 });
    redis = new Redis(REDIS_URL);
  });

  afterAll(async () => {
    await redis.quit();
    await t.stop();
  });

  async function meeting() {
    const orgId = `org-${randomUUID()}`;
    const internalOrg = randomUUID();
    const meetingId = randomUUID();
    const maya = { clerk: `user-${randomUUID()}`, id: randomUUID() };
    const raj = { clerk: `user-${randomUUID()}`, id: randomUUID() };
    t.coreApi.allowParticipant(orgId, meetingId, maya.clerk, {
      userId: maya.id,
      orgId: internalOrg,
    });
    t.coreApi.allowParticipant(orgId, meetingId, raj.clerk, { userId: raj.id, orgId: internalOrg });
    t.coreApi.setMeeting({
      id: meetingId,
      status: "live",
      host_user_id: maya.id,
      created_by: maya.id,
      agenda: [],
    });
    const rajToken = await t.jwks.signToken({ orgId, userId: raj.clerk });
    const mayaSocket = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId, userId: maya.clerk }),
    );
    const rajSocket = await connectClient(t.baseUrl, rajToken);
    await new Promise<JoinAck>((r) => mayaSocket.emit("join", { meetingId }, r));
    await new Promise<JoinAck>((r) => rajSocket.emit("join", { meetingId }, r));
    const key = roomSignalsKey(internalOrg, meetingId);
    const signals = async (): Promise<SignalEntry[]> =>
      (await redis.lrange(key, 0, -1)).map((s) => JSON.parse(s) as SignalEntry);
    return {
      orgId,
      internalOrg,
      meetingId,
      maya,
      raj,
      mayaSocket,
      rajSocket,
      rajToken,
      key,
      signals,
    };
  }

  it("keeps an unmute spoken over privately, rate-limited, never broadcast", async () => {
    const m = await meeting();
    const heard = collect(m.mayaSocket, "floor-attempt");
    const now = Date.now();
    const ok = await emitAck(m.rajSocket, "floor-attempt", {
      meetingId: m.meetingId,
      startedAt: now - 3000,
      endedAt: now - 1800,
      overUserId: m.maya.id,
      unmutedAt: now - 5000,
    });
    expect(ok).toEqual({ ok: true });
    const kept = await m.signals();
    expect(kept).toHaveLength(1);
    expect(kept[0]).toMatchObject({
      kind: "spoken_over",
      user_id: m.raj.id,
      detail: { over_user_id: m.maya.id },
    });
    expect(await redis.pttl(m.key)).toBeGreaterThan(47 * 3600 * 1000);
    expect(
      await emitAck(m.rajSocket, "floor-attempt", {
        meetingId: m.meetingId,
        startedAt: now - 1000,
        endedAt: now - 500,
      }),
    ).toEqual({ ok: false, error: "RATE_LIMITED" });
    await sleep(80);
    expect(heard).toEqual([]);
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("refuses a malformed attempt and one for a meeting the socket has not joined", async () => {
    const m = await meeting();
    expect(
      await emitAck(m.rajSocket, "floor-attempt", {
        meetingId: m.meetingId,
        startedAt: "x",
        endedAt: 1,
      }),
    ).toEqual({
      ok: false,
      error: "INVALID_PAYLOAD",
    });
    expect(
      await emitAck(m.rajSocket, "floor-attempt", {
        meetingId: randomUUID(),
        startedAt: Date.now() - 1000,
        endedAt: Date.now(),
      }),
    ).toEqual({ ok: false, error: "NOT_JOINED" });
    expect(await m.signals()).toEqual([]);
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("keeps a hand the host lowered and one still up at the end, never one that was called on", async () => {
    const m = await meeting();
    await emitAck(m.rajSocket, "hand", { meetingId: m.meetingId, raised: true });
    await emitAck(m.mayaSocket, "hand", {
      meetingId: m.meetingId,
      raised: false,
      userId: m.raj.id,
      reason: "called",
    });
    expect(await m.signals()).toEqual([]);
    await emitAck(m.rajSocket, "hand", { meetingId: m.meetingId, raised: true });
    await emitAck(m.mayaSocket, "hand", {
      meetingId: m.meetingId,
      raised: false,
      userId: m.raj.id,
      reason: "lowered",
    });
    await waitFor(async () => (await m.signals()).length === 1);
    expect((await m.signals())[0]).toMatchObject({
      kind: "hand_not_called",
      user_id: m.raj.id,
      detail: { outcome: "lowered_by_host" },
    });
    await emitAck(m.rajSocket, "hand", { meetingId: m.meetingId, raised: true });
    t.coreApi.setMeeting({
      id: m.meetingId,
      status: "ended",
      host_user_id: m.maya.id,
      created_by: m.maya.id,
      agenda: [],
    });
    expect(await emitAck(m.mayaSocket, "end-meeting", { meetingId: m.meetingId })).toEqual({
      ok: true,
    });
    const kept = await m.signals();
    expect(kept.map((s) => (s.detail as { outcome: string }).outcome)).toEqual([
      "lowered_by_host",
      "still_raised_at_end",
    ]);
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("keeps a hand that came down because its owner left", async () => {
    const m = await meeting();
    await emitAck(m.rajSocket, "hand", { meetingId: m.meetingId, raised: true });
    m.rajSocket.close();
    await waitFor(async () => (await m.signals()).length === 1, 3000);
    expect((await m.signals())[0]).toMatchObject({
      user_id: m.raj.id,
      detail: { outcome: "left" },
    });
    m.mayaSocket.close();
  });

  it("announces a resumed thread only when core-api holds it resolved for this meeting", async () => {
    const m = await meeting();
    const onRaj = collect(m.rajSocket, "resume-point");
    const pointId = randomUUID();
    t.coreApi.setResumePoint(m.meetingId, { id: pointId, status: "open" });
    expect(
      await emitAck(m.mayaSocket, "resume-point", {
        meetingId: m.meetingId,
        resumePointId: pointId,
      }),
    ).toEqual({
      ok: false,
      error: "NOT_VERIFIED",
    });
    t.coreApi.setResumePoint(m.meetingId, { id: pointId, status: "continued" });
    expect(
      await emitAck(m.mayaSocket, "resume-point", {
        meetingId: m.meetingId,
        resumePointId: randomUUID(),
      }),
    ).toEqual({ ok: false, error: "NOT_VERIFIED" });
    expect(
      await emitAck(m.mayaSocket, "resume-point", {
        meetingId: m.meetingId,
        resumePointId: "bad id!",
      }),
    ).toEqual({
      ok: false,
      error: "INVALID_PAYLOAD",
    });
    expect(
      await emitAck(m.mayaSocket, "resume-point", {
        meetingId: m.meetingId,
        resumePointId: pointId,
      }),
    ).toEqual({
      ok: true,
      status: "continued",
    });
    await waitFor(() => onRaj.length === 1);
    expect(onRaj[0]).toMatchObject({
      meetingId: m.meetingId,
      payload: { id: pointId, status: "continued", byUserId: m.maya.id },
    });
    m.mayaSocket.close();
    m.rajSocket.close();
  });
});
