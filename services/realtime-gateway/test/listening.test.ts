/**
 * In every meeting at once (server.ts "listen", the person's channel, "listeners" and
 * "meeting-view" in the room). One real App (real Socket.io + real Redis at :6390), the fake
 * JWKS and the fake core-api; a separate Redis client plays core-api's publishes.
 */
import { randomUUID } from "node:crypto";

import { Redis } from "ioredis";
import type { Socket as ClientSocket } from "socket.io-client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { pubsubChannel, userChannel } from "../../../libs/keys";
import { sanitizeListeners, type JoinAck, type ListenAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";

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

function collect(socket: ClientSocket, event: string): Record<string, unknown>[] {
  const seen: Record<string, unknown>[] = [];
  socket.on(event, (msg: Record<string, unknown>) => seen.push(msg));
  return seen;
}

function listen(socket: ClientSocket, payload?: unknown): Promise<ListenAck> {
  return new Promise((resolve) =>
    payload === undefined
      ? socket.emit("listen", resolve)
      : socket.emit("listen", payload, resolve),
  );
}

describe("sanitizeListeners", () => {
  it("keeps the count and string ids only", () => {
    expect(sanitizeListeners({ count: 2, userIds: ["a", 3, "b"], extra: 1 })).toEqual({
      count: 2,
      userIds: ["a", "b"],
    });
    expect(sanitizeListeners({ count: -1 })).toBeNull();
    expect(sanitizeListeners({ count: "2" })).toBeNull();
    expect(sanitizeListeners(null)).toBeNull();
    expect(sanitizeListeners({ count: 0 })).toEqual({ count: 0, userIds: [] });
  });
});

describe("the person's channel and the room's listeners", () => {
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

  async function person(clerkOrg: string, orgId: string) {
    const clerk = `user-${randomUUID()}`;
    const id = randomUUID();
    t.coreApi.setIdentity(clerkOrg, clerk, { userId: id, orgId });
    const socket = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId: clerkOrg, userId: clerk }),
    );
    return { clerk, id, socket };
  }

  it("delivers an interruption only to the person who listens, on every tab", async () => {
    const clerkOrg = `org-${randomUUID()}`;
    const orgId = randomUUID();
    const sushan = await person(clerkOrg, orgId);
    const other = await person(clerkOrg, orgId);
    const secondTab = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId: clerkOrg, userId: sushan.clerk }),
    );
    expect(await listen(sushan.socket, {})).toEqual({ ok: true });
    expect(await listen(sushan.socket)).toEqual({ ok: true }); // idempotent, no payload
    expect(await listen(secondTab)).toEqual({ ok: true });
    expect(await listen(other.socket, { userId: sushan.id })).toEqual({ ok: true });
    const mine = collect(sushan.socket, "agent-interrupt");
    const tab = collect(secondTab, "agent-interrupt");
    const theirs = collect(other.socket, "agent-interrupt");
    const payload = { id: randomUUID(), title: "Pricing is being decided in the 3pm right now." };
    await redis.publish(
      userChannel(orgId, sushan.id),
      JSON.stringify({ type: "agent-interrupt", userId: sushan.id, payload }),
    );
    await waitFor(() => mine.length === 1 && tab.length === 1);
    expect(mine[0]).toEqual({ payload });
    // a body naming someone else on this channel, and an unknown type, are dropped
    await redis.publish(
      userChannel(orgId, sushan.id),
      JSON.stringify({ type: "agent-interrupt", userId: other.id, payload }),
    );
    await redis.publish(
      userChannel(orgId, sushan.id),
      JSON.stringify({ type: "caption", payload: { text: "x" } }),
    );
    const status = collect(sushan.socket, "view-status");
    await redis.publish(
      userChannel(orgId, sushan.id),
      JSON.stringify({ type: "view-status", payload: { id: "v1", status: "shared" } }),
    );
    await waitFor(() => status.length === 1);
    expect(mine).toHaveLength(1);
    expect(theirs).toHaveLength(0);
    for (const s of [sushan.socket, other.socket, secondTab]) s.close();
  });

  it("answers UNAVAILABLE when core-api cannot say who the socket is", async () => {
    const socket = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId: `org-${randomUUID()}`, userId: `user-${randomUUID()}` }),
    );
    expect(await listen(socket, {})).toEqual({ ok: false, error: "UNAVAILABLE" });
    socket.close();
  });

  it("stops delivering once the socket is gone", async () => {
    const clerkOrg = `org-${randomUUID()}`;
    const orgId = randomUUID();
    const p = await person(clerkOrg, orgId);
    expect(await listen(p.socket)).toEqual({ ok: true });
    p.socket.close();
    await sleep(100);
    const subscribers = (await redis.call(
      "PUBSUB",
      "NUMSUB",
      userChannel(orgId, p.id),
    )) as unknown[];
    expect(Number(subscribers[1])).toBe(0);
  });

  it("tells the room how many agents listen, and only the host sees a view", async () => {
    const clerkOrg = `org-${randomUUID()}`;
    const orgId = randomUUID();
    const meetingId = randomUUID();
    const host = { clerk: `user-${randomUUID()}`, id: randomUUID() };
    const guest = { clerk: `user-${randomUUID()}`, id: randomUUID() };
    t.coreApi.allowParticipant(clerkOrg, meetingId, host.clerk, { userId: host.id, orgId });
    t.coreApi.allowParticipant(clerkOrg, meetingId, guest.clerk, { userId: guest.id, orgId });
    t.coreApi.setMeeting({
      id: meetingId,
      status: "live",
      host_user_id: host.id,
      created_by: host.id,
      agenda: [],
    });
    const hostSocket = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId: clerkOrg, userId: host.clerk }),
    );
    const guestSocket = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId: clerkOrg, userId: guest.clerk }),
    );
    await new Promise<JoinAck>((r) => hostSocket.emit("join", { meetingId }, r));
    await new Promise<JoinAck>((r) => guestSocket.emit("join", { meetingId }, r));
    const hostCount = collect(hostSocket, "listeners");
    const guestCount = collect(guestSocket, "listeners");
    const hostViews = collect(hostSocket, "meeting-view");
    const guestViews = collect(guestSocket, "meeting-view");
    const channel = pubsubChannel(orgId, meetingId);
    await redis.publish(
      channel,
      JSON.stringify({
        type: "listeners",
        meetingId: "the-channel-decides",
        payload: { count: 2, userIds: ["a", "b"], leak: "x" },
      }),
    );
    await waitFor(() => hostCount.length === 1 && guestCount.length === 1);
    expect(guestCount[0]).toEqual({ meetingId, payload: { count: 2, userIds: ["a", "b"] } });
    const view = { id: randomUUID(), chat_text: "Sushan's view: keep SMB at 10%." };
    await redis.publish(
      channel,
      JSON.stringify({ type: "meeting-view", meetingId, userId: host.id, payload: view }),
    );
    await waitFor(() => hostViews.length === 1);
    await sleep(100);
    expect(hostViews[0]).toEqual({ meetingId, payload: view });
    expect(guestViews).toHaveLength(0);
    hostSocket.close();
    guestSocket.close();
  });
});
