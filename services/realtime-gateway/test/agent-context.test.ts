/**
 * ContextGapAgent context fills (Phase P10, plan 3.5; P10 contract section 7).
 *
 * 1. `routeAgentRecord`: a `context_fill` record is forwarded as "agent-context", tagged with the
 *    requester's user id, ONLY when its fill is surfaced and well formed. A null fill (a silent
 *    fall-through or a responder-side security entry), a silent fill, a bad payload, an unknown
 *    hop or responder are skipped; the collaboration log never leaves the gateway.
 * 2. Real Redis + real Socket.io: a fill reaches its requester's sockets (both tabs) only, never
 *    another participant of the same meeting, never another org whose meeting shares the id.
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
// 2026-11-10T10:00:00Z in microseconds, as the Avro header's timestamp-micros decodes
const OCCURRED_AT_MICROS = Date.UTC(2026, 10, 10, 10, 0, 0) * 1000;

function contextRecord(orgId: string, meetingId: string, userId: string, fill: unknown) {
  const collabId = randomUUID();
  return {
    header: {
      org_id: orgId,
      idempotency_key: "k",
      event_id: randomUUID(),
      occurred_at: OCCURRED_AT_MICROS,
    },
    meeting_id: meetingId,
    window_id: `${meetingId}:9`,
    user_id: userId,
    agent: "ContextGapAgent",
    kind: "context_fill",
    relevance_score: 0,
    confidence: 0.82,
    content: "",
    payload_json: JSON.stringify({
      gap: { query: "vendor decision", query_class: "decision_lineage" },
      collaborations: [
        {
          id: collabId,
          correlation_id: randomUUID(),
          hop: "peer",
          requester: userId,
          responder: `user:${randomUUID()}`,
          query: "vendor decision",
          query_class: "decision_lineage",
          target_scope: "same_team",
          result: "answered",
          success: true,
          latency_ms: 420,
        },
      ],
      fill,
      prompt: "context_gap_agent@v1",
      model_tier: "extract",
    }),
    source_chunk_ids: [],
  };
}

function surfacedFill(overrides: Record<string, unknown> = {}) {
  return {
    id: randomUUID(),
    content: "The team picked Acme as the vendor last Tuesday.",
    confidence: 0.82,
    surfaced: true,
    rationale: "Bob's agent had the decision from the prior sync.",
    hop: "peer",
    responder: `user:${randomUUID()}`,
    citations: [
      { kind: "decision", decision_id: randomUUID(), meeting_id: randomUUID() },
      { kind: "web", url: "https://example.com" },
      { kind: "transcript", chunk_id: "c1", nested: { no: 1 } },
    ],
    ...overrides,
  };
}

async function waitFor(cond: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error("timed out waiting for condition");
    await sleep(20);
  }
}

describe("routeAgentRecord: context fills", () => {
  const org = randomUUID();
  const meeting = randomUUID();
  const user = randomUUID();

  it("forwards a surfaced fill to the requester only, without the collaboration log", () => {
    const fill = surfacedFill();
    const routed = routeAgentRecord("agent-results", contextRecord(org, meeting, user, fill));
    expect(routed).toEqual({
      channel: pubsubChannel(org, meeting),
      message: {
        type: "agent-context",
        userId: user,
        payload: {
          id: fill.id,
          meetingId: meeting,
          content: fill.content,
          confidence: 0.82,
          rationale: fill.rationale,
          hop: "peer",
          responder: fill.responder,
          citations: [fill.citations[0], { kind: "transcript", chunk_id: "c1" }],
          createdAt: "2026-11-10T10:00:00.000Z",
        },
      },
    });
    expect(JSON.stringify(routed)).not.toContain("collaborations");
    expect(JSON.stringify(routed)).not.toContain("vendor decision");
  });

  it("forwards a knowledge-base fill with responder rag", () => {
    const fill = surfacedFill({ hop: "rag", responder: "rag", rationale: undefined });
    const routed = routeAgentRecord("agent-results", contextRecord(org, meeting, user, fill));
    expect(routed?.message.type).toBe("agent-context");
    expect(routed?.message.payload).toMatchObject({
      hop: "rag",
      responder: "rag",
      rationale: null,
    });
  });

  it("skips a null fill, a silent fill, a bad payload and a malformed fill", () => {
    const skip = (fill: unknown) =>
      expect(routeAgentRecord("agent-results", contextRecord(org, meeting, user, fill))).toBeNull();
    skip(null); // silent fall-through or a responder-side security entry
    skip(undefined);
    skip(surfacedFill({ surfaced: false }));
    skip(surfacedFill({ id: "not-a-uuid" }));
    skip(surfacedFill({ content: "" }));
    skip(surfacedFill({ content: 7 }));
    skip(surfacedFill({ hop: "telepathy" }));
    skip(surfacedFill({ responder: "alice@example.com" }));
    skip([surfacedFill()]);
    const notJson = { ...contextRecord(org, meeting, user, surfacedFill()), payload_json: "{nope" };
    expect(routeAgentRecord("agent-results", notJson)).toBeNull();
    const wrongKind = { ...contextRecord(org, meeting, user, surfacedFill()), kind: "note" };
    expect(routeAgentRecord("agent-results", wrongKind)).toBeNull();
  });

  it("falls back to now when the header has no usable timestamp", () => {
    const record = contextRecord(org, meeting, user, surfacedFill());
    const before = Date.now();
    const routed = routeAgentRecord("agent-results", {
      ...record,
      header: { ...record.header, occurred_at: "yesterday" },
    });
    const createdAt = (routed?.message.payload as { createdAt: string }).createdAt;
    expect(Date.parse(createdAt)).toBeGreaterThanOrEqual(before - 1);
  });
});

describe("context fill delivery (real Redis, real Socket.io)", () => {
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

  it("reaches only the requester's sockets, never the rest of the room or another org", async () => {
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
      s.on("agent-context", (e) => got[name].push(e));
      s.on("agent-suggestion", () => got[name].push("suggestion"));
      s.on("agent-candidate", () => got[name].push("candidate"));
    }

    const fill = surfacedFill({ responder: `user:${bob}`, citations: [] });
    const routed = routeAgentRecord("agent-results", contextRecord(orgA, meetingId, alice, fill));
    await publisher.publish(routed!.channel, JSON.stringify(routed!.message));

    await waitFor(() => got.alice.length >= 1 && got.aliceTab.length >= 1);
    await sleep(300); // give a leak time to arrive before asserting its absence
    const expected = {
      meetingId,
      payload: {
        id: fill.id,
        meetingId,
        content: fill.content,
        confidence: 0.82,
        rationale: fill.rationale,
        hop: "peer",
        responder: `user:${bob}`,
        citations: [],
        createdAt: "2026-11-10T10:00:00.000Z",
      },
    };
    expect(got.alice).toEqual([expected]);
    expect(got.aliceTab).toEqual([expected]);
    expect(got.bob).toEqual([]);
    expect(got.other).toEqual([]);
  });
});
