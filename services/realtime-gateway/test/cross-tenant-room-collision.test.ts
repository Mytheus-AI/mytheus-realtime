/**
 * Fix round 2 — Critical finding: presence and chat still leaked by room membership, the exact
 * same bug already fixed once for captions, missed the first time because the caption fix
 * scoped delivery per-channel WITHOUT ever revisiting `roomName()` itself, which presence and
 * chat still use directly via `nsp.to(roomName(...))`.
 *
 * The scenario (identical shape to test/captions.test.ts's collision test): two different orgs
 * each have a meeting whose `meeting_id` happens to be the literal same string. Before this fix,
 * `roomName()` was meeting_id-only, so both orgs' sockets landed in the identical Socket.io
 * room, and `nsp.to(room).emit(...)` — the mechanism `@socket.io/redis-adapter` uses to fan
 * presence/chat out across every gateway pod — delivered to both regardless of org. The
 * join-time participant check does NOT protect against this: it verifies the JOINING socket's
 * own org owns a meeting with that id, never that no OTHER org does.
 *
 * Fixed by restoring org-scoping to `roomName()` itself (libs/keys.ts) — a single canonical
 * change that structurally covers every `nsp.to(roomName(...))` call site in server.ts, rather
 * than an org check bolted onto each of the four places that broadcast to a room.
 */
import { randomUUID } from "node:crypto";

import { afterEach, beforeAll, describe, expect, it } from "vitest";

import type { JoinAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";
import type { Socket as ClientSocket } from "socket.io-client";

describe("presence and chat: zero cross-tenant delivery, even when two orgs share the literal same meeting_id", () => {
  let t: TestApp;
  const sockets: ClientSocket[] = [];

  beforeAll(async () => {
    t = await startTestApp();
  });

  afterEach(() => {
    for (const s of sockets.splice(0)) s.close();
  });

  async function joinAs(params: {
    orgId: string;
    meetingId: string;
    userId: string;
    resolvedOrgId: string;
    resolvedUserId: string;
  }): Promise<ClientSocket> {
    t.coreApi.allowParticipant(params.orgId, params.meetingId, params.userId, {
      orgId: params.resolvedOrgId,
      userId: params.resolvedUserId,
    });
    const token = await t.jwks.signToken({ orgId: params.orgId, userId: params.userId });
    const socket = await connectClient(t.baseUrl, token);
    sockets.push(socket);
    const ack = await new Promise<JoinAck>((resolve) => socket.emit("join", { meetingId: params.meetingId }, resolve));
    expect(ack.ok).toBe(true);
    return socket;
  }

  it("org A's presence broadcast never reaches org B's socket, and vice versa", async () => {
    const sharedMeetingId = `meeting-${randomUUID()}`;
    const orgA = `org-${randomUUID()}`;
    const orgB = `org-${randomUUID()}`;
    const resolvedOrgA = randomUUID();
    const resolvedOrgB = randomUUID();

    const socketA = await joinAs({
      orgId: orgA,
      meetingId: sharedMeetingId,
      userId: `user-a1-${randomUUID()}`,
      resolvedOrgId: resolvedOrgA,
      resolvedUserId: randomUUID(),
    });
    const socketB = await joinAs({
      orgId: orgB,
      meetingId: sharedMeetingId,
      userId: `user-b1-${randomUUID()}`,
      resolvedOrgId: resolvedOrgB,
      resolvedUserId: randomUUID(),
    });

    // Attach listeners only now (after both initial joins), so every subsequent join's presence
    // broadcast is genuinely observed by the listener, not raced against it.
    const presenceEventsOnA: string[][] = [];
    const presenceEventsOnB: string[][] = [];
    socketA.on("presence", (members: string[]) => presenceEventsOnA.push(members));
    socketB.on("presence", (members: string[]) => presenceEventsOnB.push(members));

    // A second org-A member joins. If rooms were still meeting_id-only, this presence broadcast
    // (containing resolvedUserA2, an org-A internal id) would ALSO reach socket B.
    const resolvedUserA2 = randomUUID();
    await joinAs({
      orgId: orgA,
      meetingId: sharedMeetingId,
      userId: `user-a2-${randomUUID()}`,
      resolvedOrgId: resolvedOrgA,
      resolvedUserId: resolvedUserA2,
    });
    await sleep(300);

    // Sanity: A DID see this org-A join (proves the listener/broadcast path itself works,
    // so the absence check on B below isn't just "presence broadcasts don't work at all").
    expect(presenceEventsOnA.some((members) => members.includes(resolvedUserA2))).toBe(true);
    // The leak check: B must never see an org-A member's presence.
    expect(presenceEventsOnB.some((members) => members.includes(resolvedUserA2))).toBe(false);

    // Reverse direction: a second org-B member joins; A must never see it.
    const resolvedUserB2 = randomUUID();
    await joinAs({
      orgId: orgB,
      meetingId: sharedMeetingId,
      userId: `user-b2-${randomUUID()}`,
      resolvedOrgId: resolvedOrgB,
      resolvedUserId: resolvedUserB2,
    });
    await sleep(300);

    expect(presenceEventsOnB.some((members) => members.includes(resolvedUserB2))).toBe(true);
    expect(presenceEventsOnA.some((members) => members.includes(resolvedUserB2))).toBe(false);
  });

  it("org A's chat never reaches org B's socket, and vice versa", async () => {
    const sharedMeetingId = `meeting-${randomUUID()}`;
    const orgA = `org-${randomUUID()}`;
    const orgB = `org-${randomUUID()}`;
    const userA = `user-a-${randomUUID()}`;
    const userB = `user-b-${randomUUID()}`;

    const socketA = await joinAs({
      orgId: orgA,
      meetingId: sharedMeetingId,
      userId: userA,
      resolvedOrgId: randomUUID(),
      resolvedUserId: randomUUID(),
    });
    const socketB = await joinAs({
      orgId: orgB,
      meetingId: sharedMeetingId,
      userId: userB,
      resolvedOrgId: randomUUID(),
      resolvedUserId: randomUUID(),
    });

    const chatOnA: unknown[] = [];
    const chatOnB: unknown[] = [];
    socketA.on("chat", (msg: unknown) => chatOnA.push(msg));
    socketB.on("chat", (msg: unknown) => chatOnB.push(msg));

    socketA.emit("chat", { meetingId: sharedMeetingId, content: "org A only", clientMsgId: randomUUID() });
    await waitFor(() => chatOnA.length >= 1);
    await sleep(300);

    expect(chatOnA).toHaveLength(1);
    expect(chatOnB).toHaveLength(0);

    socketB.emit("chat", { meetingId: sharedMeetingId, content: "org B only", clientMsgId: randomUUID() });
    await waitFor(() => chatOnB.length >= 1);
    await sleep(300);

    expect(chatOnB).toHaveLength(1);
    expect(chatOnA).toHaveLength(1); // unchanged
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
