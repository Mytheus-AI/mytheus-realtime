/**
 * Task 8 — live captions wiring. `transcription-service`'s `captions.py` (task-7 report)
 * publishes `{"type":"caption","meetingId":...,"payload":{chunkId,speakerId,startMs,endMs,text,
 * isFinal,confidence}}` on `nexus:{org_id}:pubsub:meeting:{meeting_id}`, where `org_id` is
 * ALWAYS the resolved internal UUID (transcription-service has a database and resolves tenancy
 * for its own RLS scoping; it never sees a raw Clerk claim). This file proves the three
 * properties the brief calls out as the ones that matter for this bridge specifically:
 *
 *  1. The real payload shape round-trips, and `isFinal` (+ a stable `chunkId` across an
 *     interim's revisions) is what lets a client tell an interim (replace-in-place) from a
 *     final (append) apart — see providers/fake.py's `default_script()` on the other side,
 *     which sends exactly this shape: two interims and a final sharing one `chunkId`.
 *  2. Zero cross-tenant delivery — including the adversarial case the brief names explicitly:
 *     two different orgs whose meetings happen to share the literal same `meeting_id` string.
 *     This is not a theoretical worry: Socket.io ROOMS are keyed by `meeting_id` ALONE (Task 6,
 *     `libs/keys.ts` `roomName`), so two sockets from different orgs CAN end up in the same
 *     room if `meeting_id` ever collided. The pubsub CHANNEL is still org-scoped
 *     (`nexus:{org_id}:pubsub:meeting:{meeting_id}`), so caption delivery must be scoped to
 *     that channel, not to room membership, or a meeting_id collision becomes a transcript
 *     leak across tenants.
 *  3. No replay on reconnect — the gateway holds no caption buffer; a caption published while
 *     a client is disconnected is simply never seen by that client, ever (a reconnecting client
 *     re-hydrates finals from core-api's REST read model instead, per the brief's resolution).
 */
import { randomUUID } from "node:crypto";

import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { pubsubChannel } from "../../../libs/keys";
import type { JoinAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";
import type { Socket as ClientSocket } from "socket.io-client";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";

interface CaptionMessage {
  meetingId: string;
  payload: {
    chunkId: string;
    speakerId: string;
    startMs: number;
    endMs: number;
    text: string;
    isFinal: boolean;
    confidence: number;
  };
}

function captionPayload(
  overrides: Partial<CaptionMessage["payload"]> = {},
): CaptionMessage["payload"] {
  return {
    chunkId: randomUUID(),
    speakerId: "0",
    startMs: 0,
    endMs: 400,
    text: "hello",
    isFinal: false,
    confidence: 0.4,
    ...overrides,
  };
}

describe("live caption bridge (transcription-service -> realtime-gateway -> Socket.io client)", () => {
  let t: TestApp;
  let publisher: Redis;
  const sockets: ClientSocket[] = [];

  beforeAll(async () => {
    t = await startTestApp();
    publisher = new Redis(REDIS_URL);
  });

  afterEach(() => {
    for (const s of sockets.splice(0)) s.close();
  });

  afterAll(async () => {
    publisher.disconnect();
    await t.stop();
  });

  async function joinAs(params: {
    orgId: string;
    meetingId: string;
    userId: string;
    resolvedOrgId: string;
  }): Promise<ClientSocket> {
    t.coreApi.allowParticipant(params.orgId, params.meetingId, params.userId, {
      orgId: params.resolvedOrgId,
      userId: randomUUID(),
    });
    const token = await t.jwks.signToken({ orgId: params.orgId, userId: params.userId });
    const socket = await connectClient(t.baseUrl, token);
    sockets.push(socket);
    const ack = await new Promise<JoinAck>((resolve) =>
      socket.emit("join", { meetingId: params.meetingId }, resolve),
    );
    expect(ack.ok).toBe(true);
    return socket;
  }

  it("delivers the real transcription-service payload shape; isFinal + a shared chunkId is what distinguishes a mutable interim from its append-only final", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const userId = `user-${randomUUID()}`;
    const resolvedOrgId = randomUUID();
    const socket = await joinAs({ orgId, meetingId, userId, resolvedOrgId });

    const received: CaptionMessage[] = [];
    socket.on("caption", (msg: CaptionMessage) => received.push(msg));

    const chunkId = randomUUID();
    const interim = captionPayload({ chunkId, text: "hello", isFinal: false, confidence: 0.4 });
    const revisedInterim = captionPayload({
      chunkId,
      text: "hello there",
      endMs: 900,
      isFinal: false,
      confidence: 0.6,
    });
    const final = captionPayload({
      chunkId,
      text: "hello there, how are you",
      endMs: 1200,
      isFinal: true,
      confidence: 0.97,
    });

    const channel = pubsubChannel(resolvedOrgId, meetingId);
    await publisher.publish(
      channel,
      JSON.stringify({ type: "caption", meetingId, payload: interim }),
    );
    await waitFor(() => received.length >= 1);
    await publisher.publish(
      channel,
      JSON.stringify({ type: "caption", meetingId, payload: revisedInterim }),
    );
    await waitFor(() => received.length >= 2);
    await publisher.publish(
      channel,
      JSON.stringify({ type: "caption", meetingId, payload: final }),
    );
    await waitFor(() => received.length >= 3);

    expect(received).toEqual([
      { meetingId, payload: interim },
      { meetingId, payload: revisedInterim },
      { meetingId, payload: final },
    ]);
    // The property a client actually depends on: same chunkId across all three, isFinal only
    // true on the last one.
    expect(new Set(received.map((m) => m.payload.chunkId))).toEqual(new Set([chunkId]));
    expect(received.map((m) => m.payload.isFinal)).toEqual([false, false, true]);
  });

  it("zero cross-tenant caption delivery, even when two different orgs' meetings share the literal same meeting_id", async () => {
    // The adversarial case named explicitly in the brief. Room name is meeting-only (Task 6), so
    // this is the scenario that would leak if caption fan-out were keyed off room membership
    // instead of the org-scoped pubsub channel.
    const sharedMeetingId = `meeting-${randomUUID()}`;
    const orgA = `org-${randomUUID()}`;
    const orgB = `org-${randomUUID()}`;
    const resolvedOrgA = randomUUID();
    const resolvedOrgB = randomUUID();

    const socketA = await joinAs({
      orgId: orgA,
      meetingId: sharedMeetingId,
      userId: `user-${randomUUID()}`,
      resolvedOrgId: resolvedOrgA,
    });
    const socketB = await joinAs({
      orgId: orgB,
      meetingId: sharedMeetingId,
      userId: `user-${randomUUID()}`,
      resolvedOrgId: resolvedOrgB,
    });

    const receivedA: CaptionMessage[] = [];
    const receivedB: CaptionMessage[] = [];
    socketA.on("caption", (msg: CaptionMessage) => receivedA.push(msg));
    socketB.on("caption", (msg: CaptionMessage) => receivedB.push(msg));

    await publisher.publish(
      pubsubChannel(resolvedOrgA, sharedMeetingId),
      JSON.stringify({
        type: "caption",
        meetingId: sharedMeetingId,
        payload: captionPayload({ text: "org A only", isFinal: true }),
      }),
    );
    await waitFor(() => receivedA.length >= 1);
    // Safety window: give a wrongly-implemented cross-tenant leak time to arrive at B before
    // asserting its absence.
    await sleep(300);

    expect(receivedA).toHaveLength(1);
    expect(receivedB).toHaveLength(0);

    // And the reverse direction, on the SAME shared room, to rule out a one-directional bug.
    await publisher.publish(
      pubsubChannel(resolvedOrgB, sharedMeetingId),
      JSON.stringify({
        type: "caption",
        meetingId: sharedMeetingId,
        payload: captionPayload({ text: "org B only", isFinal: true }),
      }),
    );
    await waitFor(() => receivedB.length >= 1);
    await sleep(300);

    expect(receivedB).toHaveLength(1);
    expect(receivedA).toHaveLength(1); // unchanged
  });

  it("a client reconnecting receives no replayed captions — nothing published while disconnected is ever delivered", async () => {
    const orgId = `org-${randomUUID()}`;
    const meetingId = `meeting-${randomUUID()}`;
    const userId = `user-${randomUUID()}`;
    const resolvedOrgId = randomUUID();

    const first = await joinAs({ orgId, meetingId, userId, resolvedOrgId });
    first.close();
    await sleep(150); // let the server's disconnect cleanup (caption unsubscribe) run

    await publisher.publish(
      pubsubChannel(resolvedOrgId, meetingId),
      JSON.stringify({
        type: "caption",
        meetingId,
        payload: captionPayload({ text: "missed while disconnected", isFinal: true }),
      }),
    );
    await sleep(150);

    const second = await joinAs({ orgId, meetingId, userId, resolvedOrgId });
    const received: CaptionMessage[] = [];
    second.on("caption", (msg: CaptionMessage) => received.push(msg));
    await sleep(300);

    expect(received).toHaveLength(0);
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
