/**
 * In-process integration tests for the handshake, room-join authz, presence, chat relay, and
 * caption-transport paths. One real App instance (real HTTP + Socket.io + real Redis at :6380),
 * a real fake-JWKS server, and a real fake-core-api server — only the presence-events producer is
 * an in-memory double here (the real-broker assertion lives in test/presence-events-kafka.test.ts,
 * and the real cross-*process* fan-out lives in test/cross-pod-fanout.test.ts).
 */
import { randomUUID } from "node:crypto";

import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { pubsubChannel } from "../../../libs/keys";
import type { JoinAck } from "../src/server";
import { connectClient, expectConnectRejected, startTestApp, type TestApp } from "./helpers/testApp";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6380";

describe("realtime-gateway handshake + room join + presence + chat + captions", () => {
  let t: TestApp;

  beforeAll(async () => {
    t = await startTestApp();
  });

  afterAll(async () => {
    await t.stop();
  });

  it("rejects a handshake with no token", async () => {
    await expect(expectConnectRejected(t.baseUrl)).resolves.toBeInstanceOf(Error);
  });

  it("rejects a handshake with a malformed token", async () => {
    await expect(expectConnectRejected(t.baseUrl, "garbage")).resolves.toBeInstanceOf(Error);
  });

  it("accepts a validly signed token", async () => {
    const token = await t.jwks.signToken({ orgId: "org-a", userId: "user-1" });
    const socket = await connectClient(t.baseUrl, token);
    expect(socket.connected).toBe(true);
    socket.close();
  });

  it("joins a meeting the caller is a participant of, broadcasts presence, and produces a join presence-event", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const userId = `user-${randomUUID()}`;
    t.coreApi.allowParticipant(orgId, meetingId, userId);

    const token = await t.jwks.signToken({ orgId, userId });
    const socket = await connectClient(t.baseUrl, token);

    const presenceEvents: string[][] = [];
    socket.on("presence", (members: string[]) => presenceEvents.push(members));

    const ack = await new Promise<JoinAck>((resolve) => socket.emit("join", { meetingId }, resolve));

    expect(ack).toEqual({ ok: true, presence: [userId] });
    expect(presenceEvents).toEqual([[userId]]);
    expect(t.presenceEventsProducer.calls).toContainEqual({ orgId, meetingId, userId, eventType: "join" });

    socket.close();
  });

  it("denies a join for a socket whose JWT-resolved org_id does not own the meeting, even given that org's real meeting_id", async () => {
    const orgA = `org-a-${randomUUID()}`;
    const orgB = `org-b-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`; // belongs to org A
    const userA = `user-a-${randomUUID()}`;
    const userB = `user-b-${randomUUID()}`;
    t.coreApi.allowParticipant(orgA, meetingId, userA); // org A's own user IS a participant
    // org B's user is deliberately NOT registered as a participant of org A's meeting.

    const tokenB = await t.jwks.signToken({ orgId: orgB, userId: userB });
    const socketB = await connectClient(t.baseUrl, tokenB);

    const ack = await new Promise<JoinAck>((resolve) => socketB.emit("join", { meetingId }, resolve));
    expect(ack).toEqual({ ok: false, error: "FORBIDDEN" });

    // deny-by-default must mean no side effects: no presence entry, no presence-events record
    const redis = new Redis(REDIS_URL);
    try {
      const { presenceKey } = await import("../../../libs/keys");
      expect(await redis.smembers(presenceKey(orgB, meetingId))).toEqual([]);
    } finally {
      redis.disconnect();
    }
    expect(
      t.presenceEventsProducer.calls.some((c) => c.orgId === orgB && c.meetingId === meetingId),
    ).toBe(false);

    socketB.close();
  });

  it("relays chat to the room immediately and durably writes it via core-api with the client's Idempotency-Key", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const senderId = `user-sender-${randomUUID()}`;
    const listenerId = `user-listener-${randomUUID()}`;
    t.coreApi.allowParticipant(orgId, meetingId, senderId);
    t.coreApi.allowParticipant(orgId, meetingId, listenerId);

    const senderToken = await t.jwks.signToken({ orgId, userId: senderId });
    const listenerToken = await t.jwks.signToken({ orgId, userId: listenerId });
    const sender = await connectClient(t.baseUrl, senderToken);
    const listener = await connectClient(t.baseUrl, listenerToken);

    await new Promise<JoinAck>((resolve) => sender.emit("join", { meetingId }, resolve));
    await new Promise<JoinAck>((resolve) => listener.emit("join", { meetingId }, resolve));

    const received = new Promise((resolve) => listener.once("chat", resolve));
    const clientMsgId = `client-msg-${randomUUID()}`;
    sender.emit("chat", { meetingId, content: "hello room", clientMsgId });

    await expect(received).resolves.toEqual({
      meetingId,
      userId: senderId,
      content: "hello room",
      clientMsgId,
    });

    // core-api durable write happens asynchronously after the emit; poll briefly for it.
    await waitFor(() => t.coreApi.chatCalls.some((c) => c.idempotencyKey === clientMsgId));
    const call = t.coreApi.chatCalls.find((c) => c.idempotencyKey === clientMsgId);
    expect(call).toEqual({
      orgId,
      meetingId,
      userId: senderId,
      content: "hello room",
      idempotencyKey: clientMsgId,
    });

    sender.close();
    listener.close();
  });

  it("drops chat from a socket that has not joined the meeting", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const userId = `user-${randomUUID()}`;
    // deliberately never call allowParticipant / join
    const token = await t.jwks.signToken({ orgId, userId });
    const socket = await connectClient(t.baseUrl, token);

    const before = t.coreApi.chatCalls.length;
    socket.emit("chat", { meetingId, content: "should be dropped", clientMsgId: randomUUID() });
    await sleep(150);
    expect(t.coreApi.chatCalls.length).toBe(before);

    socket.close();
  });

  it("relays a caption published on the explicit non-Socket.io channel to a joined local socket, and only once", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const userId = `user-${randomUUID()}`;
    t.coreApi.allowParticipant(orgId, meetingId, userId);

    const token = await t.jwks.signToken({ orgId, userId });
    const socket = await connectClient(t.baseUrl, token);
    await new Promise<JoinAck>((resolve) => socket.emit("join", { meetingId }, resolve));

    const captions: unknown[] = [];
    socket.on("caption", (payload: unknown) => captions.push(payload));

    const publisher = new Redis(REDIS_URL);
    try {
      await publisher.publish(
        pubsubChannel(orgId, meetingId),
        JSON.stringify({ type: "caption", payload: { text: "hello caption" } }),
      );
      await waitFor(() => captions.length > 0);
    } finally {
      publisher.disconnect();
    }

    expect(captions).toEqual([{ meetingId, payload: { text: "hello caption" } }]);

    socket.close();
  });

  it("shrinks presence on disconnect and produces a leave presence-event", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const stayerId = `user-stayer-${randomUUID()}`;
    const leaverId = `user-leaver-${randomUUID()}`;
    t.coreApi.allowParticipant(orgId, meetingId, stayerId);
    t.coreApi.allowParticipant(orgId, meetingId, leaverId);

    const stayerToken = await t.jwks.signToken({ orgId, userId: stayerId });
    const leaverToken = await t.jwks.signToken({ orgId, userId: leaverId });
    const stayer = await connectClient(t.baseUrl, stayerToken);
    const leaver = await connectClient(t.baseUrl, leaverToken);

    await new Promise<JoinAck>((resolve) => stayer.emit("join", { meetingId }, resolve));
    await new Promise<JoinAck>((resolve) => leaver.emit("join", { meetingId }, resolve));

    const presenceAfterLeave = new Promise<string[]>((resolve) => {
      stayer.on("presence", (members: string[]) => {
        if (!members.includes(leaverId)) resolve(members);
      });
    });

    leaver.close();

    await expect(presenceAfterLeave).resolves.toEqual([stayerId]);
    await waitFor(() =>
      t.presenceEventsProducer.calls.some(
        (c) => c.orgId === orgId && c.meetingId === meetingId && c.userId === leaverId && c.eventType === "leave",
      ),
    );

    stayer.close();
    // Let the server's own disconnect handler for `stayer` finish its Redis/produce work before
    // this test returns, so it can't race afterAll's app.stop() (which disconnects Redis).
    await sleep(100);
  });
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitFor: condition not met within timeout");
    }
    await sleep(20);
  }
}
