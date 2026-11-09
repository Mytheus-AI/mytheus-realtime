/**
 * QuestionAgent suggestions (Plan 7-9 section 3.10, mytheus-docs ADR-0034 D6).
 *
 * 1. `routeAgentRecord`: only surfaced suggestions with a UUID id, a question and priority 1-3
 *    are forwarded as "agent-suggestion", tagged with the owner's user id; citations keep only
 *    the four known kinds; a record with none left is skipped.
 * 2. Real Redis + real Socket.io: a suggestion reaches its OWNER's socket only, never another
 *    participant of the same meeting, never another org whose meeting shares the literal id.
 */
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";

import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { Socket as ClientSocket } from "socket.io-client";

import { pubsubChannel } from "../../../libs/keys";
import { routeAgentRecord } from "../src/agentOutput";
import type { JoinAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";

function suggestionRecord(orgId: string, meetingId: string, userId: string, suggestions: unknown) {
  return {
    header: { org_id: orgId, idempotency_key: "k", event_id: randomUUID() },
    meeting_id: meetingId,
    window_id: `${meetingId}:6`,
    user_id: userId,
    agent: "QuestionAgent",
    kind: "suggestion",
    relevance_score: 0.8,
    confidence: 0.86,
    content: "What is the budget ceiling?",
    payload_json: JSON.stringify({ suggestions, prompt: "question_agent@v1" }),
    source_chunk_ids: [],
  };
}

async function waitFor(cond: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error("timed out waiting for condition");
    await sleep(20);
  }
}

describe("routeAgentRecord: suggestions", () => {
  const org = randomUUID();
  const meeting = randomUUID();
  const user = randomUUID();

  it("forwards surfaced, well-formed suggestions to their owner only", () => {
    const shown = randomUUID();
    const routed = routeAgentRecord(
      "agent-results",
      suggestionRecord(org, meeting, user, [
        {
          id: shown,
          question: "What is the budget ceiling?",
          priority: 1,
          confidence: 0.86,
          surfaced: true,
          rationale: "You own vendor spend.",
          citations: [
            { kind: "transcript", chunk_id: "c1", meeting_id: meeting, start_ms: 1200 },
            { kind: "decision", decision_id: "d1", meeting_id: meeting },
            { kind: "web", url: "https://example.com" },
            { kind: "action", action_id: "a1", nested: { no: 1 } },
          ],
        },
        { id: randomUUID(), question: "Silent?", priority: 2, confidence: 0.5, surfaced: false },
        { id: "not-a-uuid", question: "Bad id?", priority: 2, surfaced: true },
        { id: randomUUID(), question: "Priority 4?", priority: 4, surfaced: true },
        { id: randomUUID(), question: 7, priority: 1, surfaced: true },
      ]),
    );
    expect(routed).toEqual({
      channel: pubsubChannel(org, meeting),
      message: {
        type: "agent-suggestion",
        userId: user,
        payload: {
          windowId: `${meeting}:6`,
          agent: "QuestionAgent",
          kind: "suggestion",
          suggestions: [
            {
              id: shown,
              question: "What is the budget ceiling?",
              priority: 1,
              confidence: 0.86,
              surfaced: true,
              rationale: "You own vendor spend.",
              citations: [
                { kind: "transcript", chunk_id: "c1", meeting_id: meeting, start_ms: 1200 },
                { kind: "decision", decision_id: "d1", meeting_id: meeting },
                { kind: "action", action_id: "a1" },
              ],
            },
          ],
        },
      },
    });
  });

  it("skips a record with no surfaced suggestion, or a bad payload", () => {
    const silent = [{ id: randomUUID(), question: "x?", priority: 2, surfaced: false }];
    expect(
      routeAgentRecord("agent-results", suggestionRecord(org, meeting, user, silent)),
    ).toBeNull();
    expect(routeAgentRecord("agent-results", suggestionRecord(org, meeting, user, "x"))).toBeNull();
    const notJson = { ...suggestionRecord(org, meeting, user, []), payload_json: "{nope" };
    expect(routeAgentRecord("agent-results", notJson)).toBeNull();
    const wrongAgent = { ...suggestionRecord(org, meeting, user, silent), agent: "DecisionAgent" };
    expect(routeAgentRecord("agent-results", wrongAgent)).toBeNull();
  });
});

describe("suggestion delivery (real Redis, real Socket.io)", () => {
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

  it("reaches only its owner's sockets, never the rest of the room or another org", async () => {
    const meetingId = randomUUID();
    const orgA = randomUUID();
    const orgB = randomUUID();
    const alice = randomUUID();
    const bob = randomUUID();
    const clerkOrgA = `org-${randomUUID()}`;

    const aliceSocket = await join(clerkOrgA, meetingId, orgA, alice);
    const aliceSecondTab = await join(clerkOrgA, meetingId, orgA, alice);
    const bobSocket = await join(clerkOrgA, meetingId, orgA, bob);
    // another tenant whose meeting has the literal same id, and whose user has alice's id
    const otherOrgSocket = await join(`org-${randomUUID()}`, meetingId, orgB, alice);

    const got: Record<string, unknown[]> = { alice: [], aliceTab: [], bob: [], other: [] };
    for (const [name, s] of [
      ["alice", aliceSocket],
      ["aliceTab", aliceSecondTab],
      ["bob", bobSocket],
      ["other", otherOrgSocket],
    ] as const) {
      s.on("agent-suggestion", (e) => got[name].push(e));
      s.on("agent-candidate", () => got[name].push("candidate"));
    }

    const id = randomUUID();
    const routed = routeAgentRecord(
      "agent-results",
      suggestionRecord(orgA, meetingId, alice, [
        { id, question: "Who signs?", priority: 2, confidence: 0.8, surfaced: true },
      ]),
    );
    await publisher.publish(routed!.channel, JSON.stringify(routed!.message));

    await waitFor(() => got.alice.length >= 1 && got.aliceTab.length >= 1);
    await sleep(300); // give a leak time to arrive before asserting its absence
    const expected = {
      meetingId,
      payload: {
        windowId: `${meetingId}:6`,
        agent: "QuestionAgent",
        kind: "suggestion",
        suggestions: [
          {
            id,
            question: "Who signs?",
            priority: 2,
            confidence: 0.8,
            surfaced: true,
            rationale: null,
            citations: [],
          },
        ],
      },
    };
    expect(got.alice).toEqual([expected]);
    expect(got.aliceTab).toEqual([expected]);
    expect(got.bob).toEqual([]);
    expect(got.other).toEqual([]);
  });
});
