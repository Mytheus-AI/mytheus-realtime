/**
 * Agent output bridge (Plan 7-9 section 3.4, mytheus-docs ADR-0030 D5).
 *
 * 1. `routeAgentRecord`: only surfaced PersonalListener notes and stream deltas are forwarded,
 *    on the org-scoped meeting channel, tagged with the owner; anything without UUID ids is
 *    skipped.
 * 2. Real Redis + real Socket.io: a note reaches its owner's socket and NOT another participant
 *    in the same meeting, and not a socket of another org whose meeting shares the literal id.
 */
import { randomUUID } from "node:crypto";

import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { Socket as ClientSocket } from "socket.io-client";

import { pubsubChannel } from "../../../libs/keys";
import { routeAgentRecord } from "../src/agentOutput";
import type { JoinAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";

function header(orgId: string) {
  return { org_id: orgId, idempotency_key: "k", event_id: randomUUID() };
}

function note(orgId: string, meetingId: string, userId: string, surfaced: boolean) {
  return {
    header: header(orgId),
    meeting_id: meetingId,
    window_id: `${meetingId}:0`,
    user_id: userId,
    agent: "PersonalListenerAgent",
    kind: "note",
    relevance_score: 0.8,
    confidence: surfaced ? 0.9 : 0.55,
    content: "- You own the migration.",
    payload_json: JSON.stringify({ surfaced }),
    source_chunk_ids: [randomUUID()],
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor(cond: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error("timed out waiting for condition");
    await sleep(20);
  }
}

describe("routeAgentRecord", () => {
  const org = randomUUID();
  const meeting = randomUUID();
  const user = randomUUID();

  it("forwards a surfaced note to its owner on the org-scoped meeting channel", () => {
    const routed = routeAgentRecord("agent-results", note(org, meeting, user, true));
    expect(routed?.channel).toBe(pubsubChannel(org, meeting));
    expect(routed?.message).toMatchObject({
      type: "agent-note",
      userId: user,
      payload: { content: "- You own the migration.", windowId: `${meeting}:0` },
    });
  });

  it("never forwards a silent-band note, another agent, or another kind", () => {
    expect(routeAgentRecord("agent-results", note(org, meeting, user, false))).toBeNull();
    expect(
      routeAgentRecord("agent-results", {
        ...note(org, meeting, user, true),
        agent: "DecisionAgent",
      }),
    ).toBeNull();
    expect(
      routeAgentRecord("agent-results", { ...note(org, meeting, user, true), kind: "suggestion" }),
    ).toBeNull();
  });

  it("forwards stream deltas and skips records without UUID ids", () => {
    const delta = {
      header: header(org),
      meeting_id: meeting,
      user_id: user,
      window_id: "w",
      seq: 3,
      delta: "You ",
      done: false,
    };
    expect(routeAgentRecord("agent-stream", delta)?.message).toEqual({
      type: "agent-delta",
      userId: user,
      payload: { windowId: "w", seq: 3, delta: "You ", done: false },
    });
    expect(routeAgentRecord("agent-stream", { ...delta, header: header("org_2abc") })).toBeNull();
    expect(routeAgentRecord("agent-stream", { ...delta, user_id: "user_2x" })).toBeNull();
    expect(routeAgentRecord("presence-events", delta)).toBeNull();
  });
});

describe("agent output delivery (real Redis, real Socket.io)", () => {
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

  async function join(
    clerkOrg: string,
    meetingId: string,
    internalOrg: string,
    internalUser: string,
  ) {
    const clerkUser = `user-${randomUUID()}`;
    t.coreApi.allowParticipant(clerkOrg, meetingId, clerkUser, {
      orgId: internalOrg,
      userId: internalUser,
    });
    const token = await t.jwks.signToken({ orgId: clerkOrg, userId: clerkUser });
    const socket = await connectClient(t.baseUrl, token);
    sockets.push(socket);
    const ack = await new Promise<JoinAck>((resolve) =>
      socket.emit("join", { meetingId }, resolve),
    );
    expect(ack.ok).toBe(true);
    return socket;
  }

  it("a note and its deltas reach only their owner, never the rest of the room or another org", async () => {
    const meetingId = randomUUID();
    const orgA = randomUUID();
    const orgB = randomUUID();
    const alice = randomUUID();
    const bob = randomUUID();
    const clerkOrgA = `org-${randomUUID()}`;

    const aliceSocket = await join(clerkOrgA, meetingId, orgA, alice);
    const bobSocket = await join(clerkOrgA, meetingId, orgA, bob);
    // another tenant whose meeting has the literal same id, and whose user has alice's id
    const otherOrgSocket = await join(`org-${randomUUID()}`, meetingId, orgB, alice);

    const got: Record<string, string[]> = { alice: [], bob: [], other: [] };
    for (const [name, s] of [
      ["alice", aliceSocket],
      ["bob", bobSocket],
      ["other", otherOrgSocket],
    ] as const) {
      s.on("agent-note", () => got[name].push("note"));
      s.on("agent-delta", () => got[name].push("delta"));
    }

    const delta = routeAgentRecord("agent-stream", {
      header: header(orgA),
      meeting_id: meetingId,
      user_id: alice,
      window_id: "w",
      seq: 0,
      delta: "- You ",
      done: false,
    });
    const final = routeAgentRecord("agent-results", note(orgA, meetingId, alice, true));
    await publisher.publish(delta!.channel, JSON.stringify(delta!.message));
    await publisher.publish(final!.channel, JSON.stringify(final!.message));

    await waitFor(() => got.alice.length >= 2);
    await sleep(300); // give a leak time to arrive before asserting its absence
    expect(got.alice).toEqual(["delta", "note"]);
    expect(got.bob).toEqual([]);
    expect(got.other).toEqual([]);
  });
});
