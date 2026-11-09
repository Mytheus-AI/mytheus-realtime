/**
 * Agent output bridge (Plan 7-9 section 3.4, mytheus-docs ADR-0030 D5).
 *
 * 1. `routeAgentRecord`: only surfaced PersonalListener notes and stream deltas are forwarded,
 *    on the org-scoped meeting channel, tagged with the owner; anything without UUID ids is
 *    skipped.
 * 2. Real Redis + real Socket.io: a note reaches its owner's socket and NOT another participant
 *    in the same meeting, and not a socket of another org whose meeting shares the literal id.
 * 3. Decision/action candidates (Phase 8 Part A section 3): only surfaced candidates with a UUID
 *    id are forwarded as "agent-candidate", and reach EVERY participant of the meeting, but not
 *    another meeting nor another org whose meeting shares the literal id.
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

function candidateRecord(
  orgId: string,
  meetingId: string,
  agent: string,
  kind: string,
  candidates: unknown,
) {
  return {
    header: header(orgId),
    meeting_id: meetingId,
    window_id: `${meetingId}:3`,
    user_id: randomUUID(),
    agent,
    kind,
    relevance_score: 0.8,
    confidence: 0.86,
    content: "Ship the beta on 2 December",
    payload_json:
      typeof candidates === "string"
        ? candidates
        : JSON.stringify({ candidates, prompt: "x@v1", model_tier: "sonnet" }),
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

  it("forwards surfaced decision candidates to the whole meeting, unknown fields stripped", () => {
    const keep = randomUUID();
    const stakeholder = randomUUID();
    const routed = routeAgentRecord(
      "agent-results",
      candidateRecord(org, meeting, "DecisionAgent", "decision_candidate", [
        {
          id: keep,
          content: "Ship the beta on 2 December",
          confidence: 0.86,
          surfaced: true,
          rationale: "QA signed off",
          stakeholder_ids: [stakeholder, 7],
          secret: "never forwarded",
        },
        { id: randomUUID(), content: "maybe", confidence: 0.5, surfaced: false },
        { id: "not-a-uuid", content: "bad id", confidence: 0.9, surfaced: true },
        { id: randomUUID(), content: 42, confidence: 0.9, surfaced: true },
      ]),
    );
    expect(routed?.channel).toBe(pubsubChannel(org, meeting));
    expect(routed?.message).toEqual({
      type: "agent-candidate",
      payload: {
        windowId: `${meeting}:3`,
        agent: "DecisionAgent",
        kind: "decision_candidate",
        candidates: [
          {
            id: keep,
            content: "Ship the beta on 2 December",
            confidence: 0.86,
            surfaced: true,
            rationale: "QA signed off",
            stakeholder_ids: [stakeholder],
          },
        ],
      },
    });
    expect(routed?.message).not.toHaveProperty("userId");
  });

  it("forwards surfaced action candidates with their action fields", () => {
    const id = randomUUID();
    const assignee = randomUUID();
    const routed = routeAgentRecord(
      "agent-results",
      candidateRecord(org, meeting, "ActionAgent", "action_candidate", [
        {
          id,
          content: "Alice drafts the release notes",
          confidence: 0.9,
          surfaced: true,
          assignee_id: assignee,
          due_date: "2026-12-02",
          depends_on: ["QA sign-off"],
        },
        {
          id: randomUUID(),
          content: "No owner yet",
          confidence: 0.75,
          surfaced: true,
          assignee_id: null,
          due_date: null,
        },
      ]),
    );
    expect(routed?.message).toEqual({
      type: "agent-candidate",
      payload: {
        windowId: `${meeting}:3`,
        agent: "ActionAgent",
        kind: "action_candidate",
        candidates: [
          {
            id,
            content: "Alice drafts the release notes",
            confidence: 0.9,
            surfaced: true,
            assignee_id: assignee,
            due_date: "2026-12-02",
            depends_on: ["QA sign-off"],
          },
          expect.objectContaining({ assignee_id: null, due_date: null, depends_on: [] }),
        ],
      },
    });
  });

  it("drops silent-only, malformed and mismatched candidate records", () => {
    const silent = [{ id: randomUUID(), content: "maybe", confidence: 0.5, surfaced: false }];
    const surfaced = [{ id: randomUUID(), content: "yes", confidence: 0.9, surfaced: true }];
    const rec = (agent: string, kind: string, c: unknown) =>
      routeAgentRecord("agent-results", candidateRecord(org, meeting, agent, kind, c));
    expect(rec("DecisionAgent", "decision_candidate", silent)).toBeNull();
    expect(rec("ActionAgent", "action_candidate", silent)).toBeNull();
    expect(rec("DecisionAgent", "decision_candidate", [])).toBeNull();
    expect(rec("DecisionAgent", "decision_candidate", "{not json")).toBeNull();
    expect(rec("DecisionAgent", "decision_candidate", "[1,2]")).toBeNull();
    expect(rec("DecisionAgent", "decision_candidate", '{"candidates": "nope"}')).toBeNull();
    expect(rec("DecisionAgent", "decision_candidate", [null, 3, "x"])).toBeNull();
    // agent/kind must pair up; other kinds stay dropped
    expect(rec("DecisionAgent", "action_candidate", surfaced)).toBeNull();
    expect(rec("ActionAgent", "decision_candidate", surfaced)).toBeNull();
    expect(rec("QuestionAgent", "suggestion", surfaced)).toBeNull();
    // only agent-results carries candidates; the tenant header must still be a UUID
    expect(
      routeAgentRecord(
        "agent-stream",
        candidateRecord(org, meeting, "DecisionAgent", "decision_candidate", surfaced),
      )?.message.type,
    ).toBe("agent-delta");
    expect(
      routeAgentRecord("agent-results", {
        ...candidateRecord(org, meeting, "DecisionAgent", "decision_candidate", surfaced),
        header: header("org_2abc"),
      }),
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

  it("a candidate reaches every participant of the meeting, never another meeting or org", async () => {
    const meetingId = randomUUID();
    const otherMeetingId = randomUUID();
    const orgA = randomUUID();
    const orgB = randomUUID();
    const clerkOrgA = `org-${randomUUID()}`;

    const aliceSocket = await join(clerkOrgA, meetingId, orgA, randomUUID());
    const bobSocket = await join(clerkOrgA, meetingId, orgA, randomUUID());
    const otherMeetingSocket = await join(clerkOrgA, otherMeetingId, orgA, randomUUID());
    // another tenant whose meeting has the literal same id
    const otherOrgSocket = await join(`org-${randomUUID()}`, meetingId, orgB, randomUUID());

    const got: Record<string, unknown[]> = { alice: [], bob: [], otherMeeting: [], otherOrg: [] };
    for (const [name, s] of [
      ["alice", aliceSocket],
      ["bob", bobSocket],
      ["otherMeeting", otherMeetingSocket],
      ["otherOrg", otherOrgSocket],
    ] as const) {
      s.on("agent-candidate", (e) => got[name].push(e));
      s.on("agent-note", () => got[name].push("note"));
    }

    const id = randomUUID();
    const routed = routeAgentRecord(
      "agent-results",
      candidateRecord(orgA, meetingId, "DecisionAgent", "decision_candidate", [
        { id, content: "Ship the beta", confidence: 0.86, surfaced: true, rationale: null },
      ]),
    );
    await publisher.publish(routed!.channel, JSON.stringify(routed!.message));

    await waitFor(() => got.alice.length >= 1 && got.bob.length >= 1);
    await sleep(300); // give a leak time to arrive before asserting its absence
    for (const name of ["alice", "bob"]) {
      expect(got[name]).toEqual([
        {
          meetingId,
          payload: {
            windowId: `${meetingId}:3`,
            agent: "DecisionAgent",
            kind: "decision_candidate",
            candidates: [
              {
                id,
                content: "Ship the beta",
                confidence: 0.86,
                surfaced: true,
                rationale: null,
                stakeholder_ids: [],
              },
            ],
          },
        },
      ]);
    }
    expect(got.otherMeeting).toEqual([]);
    expect(got.otherOrg).toEqual([]);
  });
});
