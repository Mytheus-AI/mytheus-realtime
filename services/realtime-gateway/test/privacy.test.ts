/**
 * Off the record and live caption translation in the room (src/privacy.ts, agentOutput.ts).
 * One real App (real Socket.io + real Redis at :6390), the fake JWKS and the fake core-api; a
 * separate Redis client plays core-api (the flag and the "off-record" publish) and the agent
 * runtime (a routed caption translation).
 */
import { randomUUID } from "node:crypto";

import { Redis } from "ioredis";
import type { Socket as ClientSocket } from "socket.io-client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { captionLangsKey, offRecordKey, pubsubChannel } from "../../../libs/keys";
import { captionTranslationRecord, routeAgentRecord } from "../src/agentOutput";
import { offRecordFromFlag, sanitizeOffRecord } from "../src/privacy";
import type { JoinAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";

type Ack = { ok: boolean; error?: string; [k: string]: unknown };
const NIL = "00000000-0000-0000-0000-000000000000";
const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: condition not met");
    await sleep(20);
  }
}

function emitAck(socket: ClientSocket, event: string, payload: unknown): Promise<Ack> {
  return new Promise((resolve) => socket.emit(event, payload, resolve));
}

function collect(
  socket: ClientSocket,
  event: string,
): { meetingId: string; payload: Record<string, unknown> }[] {
  const seen: { meetingId: string; payload: Record<string, unknown> }[] = [];
  socket.on(event, (msg: { meetingId: string; payload: Record<string, unknown> }) =>
    seen.push(msg),
  );
  return seen;
}

describe("pure helpers", () => {
  it("reads the flag core-api writes, and a missing flag as on the record", () => {
    const now = "2026-11-11T12:00:00.000Z";
    expect(offRecordFromFlag(null, now).active).toBe(false);
    const on = offRecordFromFlag(
      JSON.stringify({ segment_id: "s1", started_at: "2026-11-11T12:04:00Z", started_by: "u1" }),
      now,
    );
    expect(on).toMatchObject({ active: true, segmentId: "s1", startedBy: "u1" });
    expect(offRecordFromFlag("not json", now).active).toBe(true);
  });

  it("forwards only the known fields of core-api's payload", () => {
    const p = sanitizeOffRecord({ active: true, segmentId: "s", extra: "x", totalMs: 5 }, "t");
    expect(p).toMatchObject({ active: true, segmentId: "s", totalMs: 5, at: "t" });
    expect(p).not.toHaveProperty("extra");
    expect(sanitizeOffRecord({ active: "yes" }, "t")).toBeNull();
  });

  it("routes a caption translation, never as a token delta", () => {
    const orgId = randomUUID();
    const meetingId = randomUUID();
    const chunkId = randomUUID();
    const value = {
      header: { org_id: orgId },
      meeting_id: meetingId,
      user_id: NIL,
      window_id: `caption:${chunkId}`,
      seq: 0,
      delta: "Cerramos el presupuesto.",
      done: true,
      kind: "caption_translation",
      chunk_id: chunkId,
      language: "es",
      status: "translated",
    };
    expect(routeAgentRecord("agent-stream", value)).toEqual({
      channel: pubsubChannel(orgId, meetingId),
      message: {
        type: "caption-translation",
        payload: {
          chunkId,
          language: "es",
          status: "translated",
          text: "Cerramos el presupuesto.",
        },
      },
    });
    expect(captionTranslationRecord({ ...value, status: "budget_denied" })).toMatchObject({
      status: "budget_denied",
      text: "",
    });
    expect(captionTranslationRecord({ ...value, language: "klingon" })).toBeNull();
    expect(captionTranslationRecord({ ...value, delta: " " })).toBeNull();
    // A plain delta (kind delta, or a record from before kind existed) is unchanged.
    const delta = { ...value, user_id: randomUUID(), kind: "delta" };
    expect(routeAgentRecord("agent-stream", delta)?.message.type).toBe("agent-delta");
    const old: Record<string, unknown> = { ...delta };
    delete old.kind;
    expect(routeAgentRecord("agent-stream", old)?.message.type).toBe("agent-delta");
  });
});

describe("off the record and caption languages in the room", () => {
  let t: TestApp;
  let redis: Redis;

  beforeAll(async () => {
    t = await startTestApp();
    redis = new Redis(REDIS_URL);
  });

  afterAll(async () => {
    redis.disconnect();
    await t.stop();
  });

  async function meeting(opts: { offRecord?: boolean } = {}) {
    const clerkOrg = `org-${randomUUID()}`;
    const orgId = randomUUID();
    const meetingId = randomUUID();
    const maya = { clerk: `user-${randomUUID()}`, id: randomUUID() };
    const raj = { clerk: `user-${randomUUID()}`, id: randomUUID() };
    t.coreApi.allowParticipant(clerkOrg, meetingId, maya.clerk, { userId: maya.id, orgId });
    t.coreApi.allowParticipant(clerkOrg, meetingId, raj.clerk, { userId: raj.id, orgId });
    t.coreApi.setMeeting({
      id: meetingId,
      status: "live",
      host_user_id: maya.id,
      created_by: maya.id,
      agenda: [],
    });
    if (opts.offRecord) {
      await redis.set(
        offRecordKey(orgId, meetingId),
        JSON.stringify({
          segment_id: "seg-1",
          started_at: "2026-11-11T12:04:00Z",
          started_by: maya.id,
        }),
      );
    }
    const mayaSocket = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId: clerkOrg, userId: maya.clerk }),
    );
    const rajSocket = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId: clerkOrg, userId: raj.clerk }),
    );
    const mayaState = collect(mayaSocket, "off-record");
    const rajState = collect(rajSocket, "off-record");
    await new Promise<JoinAck>((r) => mayaSocket.emit("join", { meetingId }, r));
    await new Promise<JoinAck>((r) => rajSocket.emit("join", { meetingId }, r));
    return { orgId, meetingId, maya, raj, mayaSocket, rajSocket, mayaState, rajState };
  }

  it("tells a joiner whether the meeting is off the record", async () => {
    const m = await meeting({ offRecord: true });
    await waitFor(() => m.rajState.length > 0);
    expect(m.rajState[0]).toMatchObject({
      meetingId: m.meetingId,
      payload: { active: true, segmentId: "seg-1", startedBy: m.maya.id },
    });
    const n = await meeting();
    await waitFor(() => n.rajState.length > 0);
    expect(n.rajState[0].payload.active).toBe(false);
    for (const s of [m.mayaSocket, m.rajSocket, n.mayaSocket, n.rajSocket]) s.close();
  });

  it("forwards core-api's off-record broadcast to everyone in the meeting", async () => {
    const m = await meeting();
    await waitFor(() => m.rajState.length > 0 && m.mayaState.length > 0);
    await redis.publish(
      pubsubChannel(m.orgId, m.meetingId),
      JSON.stringify({
        type: "off-record",
        meetingId: "ignored-the-channel-decides",
        payload: { active: true, segmentId: "s2", startedBy: m.maya.id, injected: "<b>" },
      }),
    );
    await waitFor(() => m.rajState.length > 1 && m.mayaState.length > 1);
    expect(m.rajState[1].meetingId).toBe(m.meetingId);
    expect(m.rajState[1].payload).toMatchObject({ active: true, segmentId: "s2" });
    expect(m.rajState[1].payload).not.toHaveProperty("injected");
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("carries a participant's request to the room, rate limited, and the host's answer", async () => {
    const m = await meeting();
    const onMaya = collect(m.mayaSocket, "off-record-request");
    const answers = collect(m.rajSocket, "off-record-answer");
    expect(await emitAck(m.rajSocket, "off-record-request", { meetingId: m.meetingId })).toEqual({
      ok: true,
    });
    await waitFor(() => onMaya.length === 1);
    expect(onMaya[0].payload.userId).toBe(m.raj.id);
    expect(await emitAck(m.rajSocket, "off-record-request", { meetingId: m.meetingId })).toEqual({
      ok: false,
      error: "RATE_LIMITED",
    });
    // Only the host (or creator) answers for the room.
    expect(
      await emitAck(m.rajSocket, "off-record-answer", { meetingId: m.meetingId, userId: m.raj.id }),
    ).toEqual({ ok: false, error: "NOT_HOST" });
    expect(
      await emitAck(m.mayaSocket, "off-record-answer", {
        meetingId: m.meetingId,
        userId: m.raj.id,
      }),
    ).toEqual({ ok: true });
    await waitFor(() => answers.length === 1);
    expect(answers[0].payload).toMatchObject({
      userId: m.raj.id,
      answer: "declined",
      byUserId: m.maya.id,
    });
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("refuses a request while the meeting is already off the record, and from non-members", async () => {
    const m = await meeting({ offRecord: true });
    expect(await emitAck(m.rajSocket, "off-record-request", { meetingId: m.meetingId })).toEqual({
      ok: false,
      error: "ALREADY_OFF_RECORD",
    });
    expect(await emitAck(m.rajSocket, "off-record-request", { meetingId: randomUUID() })).toEqual({
      ok: false,
      error: "NOT_JOINED",
    });
    expect(
      await emitAck(m.rajSocket, "caption-language", { meetingId: randomUUID(), language: "es" }),
    ).toEqual({ ok: false, error: "NOT_JOINED" });
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("keeps each viewer's caption language in Redis and delivers translations only to them", async () => {
    const m = await meeting();
    const key = captionLangsKey(m.orgId, m.meetingId);
    expect(
      await emitAck(m.rajSocket, "caption-language", { meetingId: m.meetingId, language: "es" }),
    ).toEqual({ ok: true, language: "es" });
    expect(
      await emitAck(m.mayaSocket, "caption-language", { meetingId: m.meetingId, language: "xx" }),
    ).toEqual({ ok: false, error: "INVALID_PAYLOAD" });
    expect(await redis.hvals(key)).toEqual(["es"]);
    expect(await redis.pttl(key)).toBeGreaterThan(0);

    const onRaj = collect(m.rajSocket, "caption-translation");
    const onMaya = collect(m.mayaSocket, "caption-translation");
    const chunkId = randomUUID();
    for (const language of ["fr", "es"]) {
      await redis.publish(
        pubsubChannel(m.orgId, m.meetingId),
        JSON.stringify({
          type: "caption-translation",
          payload: { chunkId, language, status: "translated", text: `t-${language}` },
        }),
      );
    }
    await waitFor(() => onRaj.length === 1);
    await sleep(100);
    expect(onRaj.map((e) => e.payload.language)).toEqual(["es"]);
    expect(onMaya).toEqual([]);

    // Back to the original: the field goes, nothing more arrives.
    expect(
      await emitAck(m.rajSocket, "caption-language", { meetingId: m.meetingId, language: null }),
    ).toEqual({ ok: true, language: null });
    expect(await redis.hvals(key)).toEqual([]);

    // A viewer who leaves takes their language with them.
    await emitAck(m.mayaSocket, "caption-language", { meetingId: m.meetingId, language: "de" });
    expect(await redis.hvals(key)).toEqual(["de"]);
    m.mayaSocket.close();
    const start = Date.now();
    while (Date.now() - start < 2000 && (await redis.hvals(key)).length > 0) await sleep(20);
    expect(await redis.hvals(key)).toEqual([]);
    m.rajSocket.close();
  });
});
