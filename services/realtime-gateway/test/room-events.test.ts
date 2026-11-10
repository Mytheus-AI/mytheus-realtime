/**
 * Room-wide events a participant starts (src/roomEvents.ts): the shared candidate ledger (a
 * resolution is announced only when core-api's stored row supports it, edit locks), host agenda
 * changes, "End for everyone", raise hand, reactions, token refresh ("reauth") and the chat ack.
 * One real App (real Socket.io + real Redis at :6390), the fake JWKS and the fake core-api.
 */
import { randomUUID } from "node:crypto";

import type { Socket as ClientSocket } from "socket.io-client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { verifyResolution } from "../src/roomEvents";
import type { JoinAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";

type Ack = { ok: boolean; error?: string; [k: string]: unknown };

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
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

describe("verifyResolution (pure)", () => {
  const me = "u-me";
  it("announces a confirm only when it is stored confirmed by the actor", () => {
    expect(
      verifyResolution({ status: "confirmed", confirmed_by: me }, "confirmed", me, false),
    ).toBe("ok");
    expect(
      verifyResolution({ status: "confirmed", confirmed_by: "other" }, "confirmed", me, false),
    ).toBe("NOT_VERIFIED");
    expect(verifyResolution({ status: "proposed" }, "confirmed", me, false)).toBe("NOT_VERIFIED");
  });
  it("announces an edit only while the row is still proposed", () => {
    expect(verifyResolution({ status: "proposed" }, "edited", me, false)).toBe("ok");
    expect(verifyResolution({ status: "confirmed" }, "edited", me, false)).toBe("NOT_VERIFIED");
  });
  it("announces a dismissal for everyone only from the host or creator", () => {
    expect(verifyResolution({ status: "dismissed" }, "dismissed", me, true)).toBe("ok");
    expect(verifyResolution({ status: "dismissed" }, "dismissed", me, false)).toBe("NOT_HOST");
    expect(verifyResolution({ status: "proposed" }, "dismissed", me, true)).toBe("NOT_VERIFIED");
  });
});

describe("room-wide events", () => {
  let t: TestApp;

  beforeAll(async () => {
    t = await startTestApp();
  });

  afterAll(async () => {
    await t.stop();
  });

  /** Two participants of one meeting (Maya hosts), joined, plus their internal ids. */
  async function meeting() {
    const orgId = `org-${randomUUID()}`;
    const meetingId = randomUUID();
    const maya = { clerk: `user-${randomUUID()}`, id: randomUUID() };
    const raj = { clerk: `user-${randomUUID()}`, id: randomUUID() };
    t.coreApi.allowParticipant(orgId, meetingId, maya.clerk, { userId: maya.id });
    t.coreApi.allowParticipant(orgId, meetingId, raj.clerk, { userId: raj.id });
    t.coreApi.setMeeting({
      id: meetingId,
      status: "live",
      host_user_id: maya.id,
      created_by: maya.id,
      agenda: [],
    });
    const mayaSocket = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId, userId: maya.clerk }),
    );
    const rajSocket = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId, userId: raj.clerk }),
    );
    await new Promise<JoinAck>((r) => mayaSocket.emit("join", { meetingId }, r));
    await new Promise<JoinAck>((r) => rajSocket.emit("join", { meetingId }, r));
    return { orgId, meetingId, maya, raj, mayaSocket, rajSocket };
  }

  it("refuses every room event from a socket that has not joined the meeting", async () => {
    const orgId = `org-${randomUUID()}`;
    const socket = await connectClient(t.baseUrl, await t.jwks.signToken({ orgId, userId: "u-x" }));
    const meetingId = randomUUID();
    for (const [event, payload] of [
      ["candidate-resolved", { meetingId, candidateId: randomUUID(), outcome: "confirmed" }],
      ["candidate-lock", { meetingId, candidateId: randomUUID(), locked: true }],
      ["agenda-status", { meetingId, itemId: randomUUID() }],
      ["hand", { meetingId, raised: true }],
      ["reaction", { meetingId, reaction: "clap" }],
      ["end-meeting", { meetingId }],
    ] as const) {
      expect(await emitAck(socket, event, payload)).toEqual({ ok: false, error: "NOT_JOINED" });
    }
    socket.close();
  });

  it("broadcasts a verified confirm to everyone, attributed to the person who confirmed", async () => {
    const m = await meeting();
    const candidateId = randomUUID();
    t.coreApi.setCandidate({
      id: candidateId,
      kind: "decision",
      meeting_id: m.meetingId,
      status: "confirmed",
      content: "Ship the beta on 2 December",
      rationale: "QA signed off",
      confirmed_by: m.raj.id,
    });
    const onMaya = collect(m.mayaSocket, "candidate-resolution");
    const onRaj = collect(m.rajSocket, "candidate-resolution");

    const ack = await emitAck(m.rajSocket, "candidate-resolved", {
      meetingId: m.meetingId,
      candidateId,
      outcome: "confirmed",
    });

    expect(ack).toEqual({ ok: true });
    await waitFor(() => onMaya.length === 1 && onRaj.length === 1);
    expect(onMaya[0]).toMatchObject({
      meetingId: m.meetingId,
      payload: {
        candidateId,
        kind: "decision",
        outcome: "confirmed",
        byUserId: m.raj.id,
        row: { status: "confirmed", content: "Ship the beta on 2 December", confirmedBy: m.raj.id },
      },
    });
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("does not announce a claim the stored row does not support", async () => {
    const m = await meeting();
    const candidateId = randomUUID();
    t.coreApi.setCandidate({
      id: candidateId,
      kind: "action",
      meeting_id: m.meetingId,
      status: "confirmed",
      content: "Write the notes",
      confirmed_by: m.maya.id, // Maya confirmed it, not Raj
    });
    const onMaya = collect(m.mayaSocket, "candidate-resolution");

    expect(
      await emitAck(m.rajSocket, "candidate-resolved", {
        meetingId: m.meetingId,
        candidateId,
        outcome: "confirmed",
      }),
    ).toEqual({ ok: false, error: "NOT_VERIFIED" });
    expect(
      await emitAck(m.rajSocket, "candidate-resolved", {
        meetingId: m.meetingId,
        candidateId: randomUUID(), // core-api has no such row
        outcome: "edited",
      }),
    ).toEqual({ ok: false, error: "NOT_VERIFIED" });
    await sleep(150);
    expect(onMaya).toEqual([]);
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("announces a dismissal for everyone only when the host made it", async () => {
    const m = await meeting();
    const candidateId = randomUUID();
    t.coreApi.setCandidate({
      id: candidateId,
      kind: "decision",
      meeting_id: m.meetingId,
      status: "dismissed",
      content: "Move standup",
    });
    const onRaj = collect(m.rajSocket, "candidate-resolution");

    expect(
      await emitAck(m.rajSocket, "candidate-resolved", {
        meetingId: m.meetingId,
        candidateId,
        outcome: "dismissed",
      }),
    ).toEqual({ ok: false, error: "NOT_HOST" });
    expect(
      await emitAck(m.mayaSocket, "candidate-resolved", {
        meetingId: m.meetingId,
        candidateId,
        outcome: "dismissed",
      }),
    ).toEqual({ ok: true });
    await waitFor(() => onRaj.length === 1);
    expect(onRaj[0]).toMatchObject({ payload: { outcome: "dismissed", byUserId: m.maya.id } });
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("locks a card for one editor, tells the room, and releases it when the editor leaves", async () => {
    const m = await meeting();
    const candidateId = randomUUID();
    const onRaj = collect(m.rajSocket, "candidate-lock");

    const mine = await emitAck(m.mayaSocket, "candidate-lock", {
      meetingId: m.meetingId,
      candidateId,
      locked: true,
    });
    expect(mine.ok).toBe(true);
    await waitFor(() => onRaj.length === 1);
    expect(onRaj[0]).toMatchObject({ payload: { candidateId, byUserId: m.maya.id } });

    // Raj cannot take it while Maya holds it.
    expect(
      await emitAck(m.rajSocket, "candidate-lock", {
        meetingId: m.meetingId,
        candidateId,
        locked: true,
      }),
    ).toEqual({ ok: false, error: "LOCKED", holder: m.maya.id });
    // Nor release it for her.
    await emitAck(m.rajSocket, "candidate-lock", {
      meetingId: m.meetingId,
      candidateId,
      locked: false,
    });
    expect(onRaj).toHaveLength(1);

    // A late joiner learns about the lock.
    const late = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId: m.orgId, userId: m.raj.clerk }),
    );
    const state = new Promise<{ payload: { locks: unknown[] } }>((r) => late.once("room-state", r));
    await new Promise<JoinAck>((r) => late.emit("join", { meetingId: m.meetingId }, r));
    expect((await state).payload.locks).toEqual([
      expect.objectContaining({ candidateId, byUserId: m.maya.id }),
    ]);

    m.mayaSocket.close();
    await waitFor(() => onRaj.length === 2);
    expect(onRaj[1]).toMatchObject({ payload: { candidateId, byUserId: null } });
    late.close();
    m.rajSocket.close();
  });

  it("broadcasts a host's agenda change only for a host-set item and only from the host", async () => {
    const m = await meeting();
    const hostItem = {
      id: randomUUID(),
      title: "Budget",
      status: "covered",
      status_source: "host",
      position: 0,
    };
    const agentItem = {
      id: randomUUID(),
      title: "Hiring",
      status: "covered",
      status_source: "agent",
      position: 1,
    };
    t.coreApi.setMeeting({
      id: m.meetingId,
      status: "live",
      host_user_id: m.maya.id,
      created_by: m.maya.id,
      agenda: [hostItem, agentItem],
    });
    const onRaj = collect(m.rajSocket, "agenda-status");

    expect(
      await emitAck(m.mayaSocket, "agenda-status", { meetingId: m.meetingId, itemId: hostItem.id }),
    ).toEqual({
      ok: true,
    });
    await waitFor(() => onRaj.length === 1);
    expect(onRaj[0]).toMatchObject({ payload: { item: hostItem, byUserId: m.maya.id } });

    expect(
      await emitAck(m.mayaSocket, "agenda-status", {
        meetingId: m.meetingId,
        itemId: agentItem.id,
      }),
    ).toEqual({ ok: false, error: "NOT_VERIFIED" });
    expect(
      await emitAck(m.rajSocket, "agenda-status", { meetingId: m.meetingId, itemId: hostItem.id }),
    ).toEqual({ ok: false, error: "NOT_HOST" });
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("ends the meeting for everyone only once core-api says it ended, and only for the host", async () => {
    const m = await meeting();
    const onRaj = collect(m.rajSocket, "meeting-ended");

    expect(await emitAck(m.mayaSocket, "end-meeting", { meetingId: m.meetingId })).toEqual({
      ok: false,
      error: "NOT_VERIFIED",
    });
    t.coreApi.setMeeting({
      id: m.meetingId,
      status: "ended",
      host_user_id: m.maya.id,
      created_by: m.maya.id,
      agenda: [],
    });
    expect(await emitAck(m.rajSocket, "end-meeting", { meetingId: m.meetingId })).toEqual({
      ok: false,
      error: "NOT_HOST",
    });
    expect(await emitAck(m.mayaSocket, "end-meeting", { meetingId: m.meetingId })).toEqual({
      ok: true,
    });
    await waitFor(() => onRaj.length === 1);
    expect(onRaj[0]).toMatchObject({ meetingId: m.meetingId, payload: { byUserId: m.maya.id } });
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("raises and lowers hands for the room, and lowers a hand when its socket leaves", async () => {
    const m = await meeting();
    const onMaya = collect(m.mayaSocket, "hand");

    expect(await emitAck(m.rajSocket, "hand", { meetingId: m.meetingId, raised: true })).toEqual({
      ok: true,
    });
    await waitFor(() => onMaya.length === 1);
    expect(onMaya[0]).toMatchObject({ payload: { userId: m.raj.id, raised: true } });

    m.rajSocket.close();
    await waitFor(() => onMaya.length === 2);
    expect(onMaya[1]).toMatchObject({ payload: { userId: m.raj.id, raised: false } });
    m.mayaSocket.close();
  });

  it("relays allowed reactions to the room, rate-limited per socket", async () => {
    const m = await meeting();
    const onMaya = collect(m.mayaSocket, "reaction");

    expect(
      await emitAck(m.rajSocket, "reaction", { meetingId: m.meetingId, reaction: "clap" }),
    ).toEqual({
      ok: true,
    });
    expect(
      await emitAck(m.rajSocket, "reaction", { meetingId: m.meetingId, reaction: "clap" }),
    ).toEqual({
      ok: false,
      error: "RATE_LIMITED",
    });
    expect(
      await emitAck(m.rajSocket, "reaction", { meetingId: m.meetingId, reaction: "<script>" }),
    ).toEqual({ ok: false, error: "INVALID_PAYLOAD" });
    await waitFor(() => onMaya.length === 1);
    expect(onMaya[0]).toMatchObject({ payload: { userId: m.raj.id, reaction: "clap" } });
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("never delivers a room event to another org's socket on the same meeting id", async () => {
    const m = await meeting();
    const otherOrg = `org-${randomUUID()}`;
    const stranger = `user-${randomUUID()}`;
    t.coreApi.allowParticipant(otherOrg, m.meetingId, stranger);
    const strangerSocket = await connectClient(
      t.baseUrl,
      await t.jwks.signToken({ orgId: otherOrg, userId: stranger }),
    );
    await new Promise<JoinAck>((r) => strangerSocket.emit("join", { meetingId: m.meetingId }, r));
    const leaked = [...collect(strangerSocket, "hand"), ...collect(strangerSocket, "reaction")];
    const onMaya = collect(m.mayaSocket, "hand");

    await emitAck(m.rajSocket, "hand", { meetingId: m.meetingId, raised: true });
    await waitFor(() => onMaya.length === 1);
    await sleep(100);
    expect(leaked).toEqual([]);
    strangerSocket.close();
    m.mayaSocket.close();
    m.rajSocket.close();
  });

  it("refreshes the socket's bearer token only for the same identity", async () => {
    const orgId = `org-${randomUUID()}`;
    const socket = await connectClient(t.baseUrl, await t.jwks.signToken({ orgId, userId: "u-1" }));
    expect(
      await emitAck(socket, "reauth", { token: await t.jwks.signToken({ orgId, userId: "u-1" }) }),
    ).toEqual({
      ok: true,
    });
    expect(
      await emitAck(socket, "reauth", { token: await t.jwks.signToken({ orgId, userId: "u-2" }) }),
    ).toEqual({
      ok: false,
      error: "IDENTITY_MISMATCH",
    });
    expect(await emitAck(socket, "reauth", { token: "garbage" })).toEqual({
      ok: false,
      error: "UNAUTHENTICATED",
    });
    socket.close();
  });

  it("acks chat with whether it was stored, and announces the stored row id", async () => {
    const m = await meeting();
    const saved = collect(m.mayaSocket, "chat-saved");
    const live = collect(m.mayaSocket, "chat");
    const clientMsgId = randomUUID();

    expect(
      await emitAck(m.rajSocket, "chat", {
        meetingId: m.meetingId,
        content: "  hello  ",
        clientMsgId,
      }),
    ).toEqual({ ok: true, persisted: true });
    await waitFor(() => saved.length === 1 && live.length === 1);
    expect(live[0]).toMatchObject({ content: "hello", senderId: m.raj.id, clientMsgId });
    expect(saved[0]).toMatchObject({ meetingId: m.meetingId, payload: { clientMsgId } });

    t.coreApi.failChatWith = 503;
    expect(
      await emitAck(m.rajSocket, "chat", {
        meetingId: m.meetingId,
        content: "lost?",
        clientMsgId: randomUUID(),
      }),
    ).toEqual({ ok: true, persisted: false });
    t.coreApi.failChatWith = null;

    expect(
      await emitAck(m.rajSocket, "chat", {
        meetingId: m.meetingId,
        content: "x".repeat(2001),
        clientMsgId: "a",
      }),
    ).toEqual({ ok: false, error: "INVALID_PAYLOAD" });
    m.mayaSocket.close();
    m.rajSocket.close();
  });
});

describe("room-wide events cross pods", () => {
  let a: TestApp;
  let b: TestApp;

  beforeAll(async () => {
    a = await startTestApp();
    b = await startTestApp();
  });

  afterAll(async () => {
    await a.stop();
    await b.stop();
  });

  it("a verified resolution on pod A reaches a socket on pod B", async () => {
    const orgId = `org-${randomUUID()}`;
    const internalOrg = randomUUID();
    const meetingId = randomUUID();
    const raj = { clerk: `user-${randomUUID()}`, id: randomUUID() };
    const maya = { clerk: `user-${randomUUID()}`, id: randomUUID() };
    a.coreApi.allowParticipant(orgId, meetingId, raj.clerk, { userId: raj.id, orgId: internalOrg });
    b.coreApi.allowParticipant(orgId, meetingId, maya.clerk, {
      userId: maya.id,
      orgId: internalOrg,
    });
    const candidateId = randomUUID();
    a.coreApi.setCandidate({
      id: candidateId,
      kind: "action",
      meeting_id: meetingId,
      status: "proposed",
      content: "Edited text",
    });

    const rajSocket = await connectClient(
      a.baseUrl,
      await a.jwks.signToken({ orgId, userId: raj.clerk }),
    );
    const mayaSocket = await connectClient(
      b.baseUrl,
      await b.jwks.signToken({ orgId, userId: maya.clerk }),
    );
    await new Promise<JoinAck>((r) => rajSocket.emit("join", { meetingId }, r));
    await new Promise<JoinAck>((r) => mayaSocket.emit("join", { meetingId }, r));
    const onMaya = collect(mayaSocket, "candidate-resolution");

    expect(
      await emitAck(rajSocket, "candidate-resolved", { meetingId, candidateId, outcome: "edited" }),
    ).toEqual({
      ok: true,
    });
    await waitFor(() => onMaya.length === 1);
    expect(onMaya[0]).toMatchObject({
      payload: {
        candidateId,
        outcome: "edited",
        byUserId: raj.id,
        row: { content: "Edited text" },
      },
    });
    rajSocket.close();
    mayaSocket.close();
  });
});
