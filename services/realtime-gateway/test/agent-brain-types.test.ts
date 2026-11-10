/**
 * The ADR-0039 additions to the agent validators (mytheus-docs ADR-0039 D2, C5, C7):
 *
 * 1. ProactiveAgent alert types "already_decided" and "topic_watch" are accepted with their C7
 *    fields, rejected without the required ones or when they name another meeting, and leave the
 *    P11 alert shape untouched.
 * 2. The MeetingAgent nudge type "proxy_update" is accepted (always naming its item, content up
 *    to 2000 chars, the proxied participant passed through) and still host-only.
 * 3. Real Redis: a C7 alert keeps its fields through the per-user priority queue.
 */
import { randomUUID } from "node:crypto";

import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { pubsubChannel } from "../../../libs/keys";
import { routeAgentRecord, routeAgentRecords, type AgentAlertPayload } from "../src/agentOutput";
import { drainAlerts, enqueueAlert } from "../src/alertQueue";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";
const OCCURRED_AT_MICROS = Date.UTC(2026, 10, 10, 10, 0, 0) * 1000;

const org = randomUUID();
const meeting = randomUUID();
const user = randomUUID();

function header() {
  return {
    org_id: org,
    idempotency_key: "k",
    event_id: randomUUID(),
    occurred_at: OCCURRED_AT_MICROS,
  };
}

function alertRecord(alert: unknown) {
  return {
    header: header(),
    meeting_id: meeting,
    window_id: "proactive:already_decided:x",
    user_id: user,
    agent: "ProactiveAgent",
    kind: "alert",
    relevance_score: 0.9,
    confidence: 0.9,
    content: "Already decided",
    payload_json: JSON.stringify({ alert, prompt: "proactive_already_decided@v1" }),
    source_chunk_ids: [],
  };
}

function common(alertType: string, overrides: Record<string, unknown> = {}) {
  return {
    id: randomUUID(),
    alert_type: alertType,
    priority: 2,
    title: "This was already decided",
    body: "",
    confidence: 0.9,
    surfaced: true,
    rationale: null,
    subject: { kind: "meeting", id: meeting },
    related: [],
    link: null,
    ...overrides,
  };
}

const decisionId = randomUUID();
const alreadyDecided = (overrides: Record<string, unknown> = {}) =>
  common("already_decided", {
    decision_id: decisionId,
    decision_content: "We ship the Android app before iOS.",
    decided_at: "2026-10-02T09:30:00Z",
    meeting_id: meeting,
    meeting_title: "Mobile roadmap",
    ...overrides,
  });

const topicWatch = (overrides: Record<string, unknown> = {}) =>
  common("topic_watch", {
    priority: 3,
    title: "Pricing came up in Sales sync",
    topic: "Pricing",
    meeting_id: meeting,
    meeting_title: "Sales sync",
    snippet: "We should move the starter plan to annual billing only.",
    ...overrides,
  });

const route = (alert: unknown) => routeAgentRecord("agent-results", alertRecord(alert));
const payloadOf = (alert: unknown) => route(alert)?.message.payload as AgentAlertPayload;

describe("ProactiveAgent alert types already_decided and topic_watch (ADR-0039 C7)", () => {
  it("forwards already_decided with its decision fields to the recipient only", () => {
    const a = alreadyDecided();
    expect(route(a)).toEqual({
      channel: pubsubChannel(org, meeting),
      message: {
        type: "agent-alert",
        userId: user,
        payload: {
          id: a.id,
          meetingId: meeting,
          alertType: "already_decided",
          priority: 2,
          title: a.title,
          body: "",
          confidence: 0.9,
          rationale: null,
          subject: { kind: "meeting", id: meeting },
          related: [],
          link: null,
          createdAt: "2026-11-10T10:00:00.000Z",
          decisionId,
          decisionContent: "We ship the Android app before iOS.",
          decidedAt: "2026-10-02T09:30:00Z",
          meetingTitle: "Mobile roadmap",
        },
      },
    });
  });

  it("forwards topic_watch with topic, snippet and meeting title", () => {
    const p = payloadOf(topicWatch());
    expect(p.alertType).toBe("topic_watch");
    expect(p.priority).toBe(3);
    expect({ topic: p.topic, snippet: p.snippet, meetingTitle: p.meetingTitle }).toEqual({
      topic: "Pricing",
      snippet: "We should move the starter plan to annual billing only.",
      meetingTitle: "Sales sync",
    });
    expect(p).not.toHaveProperty("decisionId");
  });

  it("fills optional fields with empty values and clamps long ones", () => {
    const ad = payloadOf(
      alreadyDecided({
        decided_at: undefined,
        meeting_title: undefined,
        meeting_id: undefined,
        decision_content: "d".repeat(900),
      }),
    );
    expect(ad.decidedAt).toBeNull();
    expect(ad.meetingTitle).toBe("");
    expect(ad.decisionContent).toHaveLength(500);
    const tw = payloadOf(topicWatch({ snippet: "s".repeat(300), topic: "t".repeat(120) }));
    expect(tw.snippet).toHaveLength(200);
    expect(tw.topic).toHaveLength(80);
    expect(payloadOf(topicWatch({ snippet: null })).snippet).toBe("");
  });

  it("rejects already_decided without a UUID decision_id or a decision_content", () => {
    expect(route(alreadyDecided({ decision_id: undefined }))).toBeNull();
    expect(route(alreadyDecided({ decision_id: "d-1" }))).toBeNull();
    expect(route(alreadyDecided({ decision_content: undefined }))).toBeNull();
    expect(route(alreadyDecided({ decision_content: "   " }))).toBeNull();
    expect(route(alreadyDecided({ decision_content: 7 }))).toBeNull();
  });

  it("rejects topic_watch without a topic", () => {
    expect(route(topicWatch({ topic: undefined }))).toBeNull();
    expect(route(topicWatch({ topic: "  " }))).toBeNull();
    expect(route(topicWatch({ topic: ["Pricing"] }))).toBeNull();
  });

  it("rejects either type when it names another meeting, and still enforces the common rules", () => {
    for (const make of [alreadyDecided, topicWatch]) {
      expect(route(make({ meeting_id: randomUUID() }))).toBeNull();
      expect(route(make({ meeting_id: 42 }))).toBeNull();
      expect(route(make({ meeting_id: meeting.toUpperCase() }))).not.toBeNull();
      expect(route(make({ surfaced: false }))).toBeNull();
      expect(route(make({ priority: 0 }))).toBeNull();
      expect(route(make({ title: "" }))).toBeNull();
    }
  });

  it("leaves the P11 types' payload exactly as before (no new keys)", () => {
    const p = payloadOf(common("conflict", { priority: 1, decision_id: decisionId, topic: "x" }));
    expect(Object.keys(p).sort()).toEqual(
      [
        "alertType",
        "body",
        "confidence",
        "createdAt",
        "id",
        "link",
        "meetingId",
        "priority",
        "rationale",
        "related",
        "subject",
        "title",
      ].sort(),
    );
  });
});

describe("MeetingAgent nudge type proxy_update (ADR-0039 C5)", () => {
  const host = randomUUID();
  const itemId = randomUUID();
  const proxied = randomUUID();
  const record = (nudge: unknown) => ({
    header: header(),
    meeting_id: meeting,
    window_id: `${meeting}:4`,
    user_id: host,
    agent: "MeetingAgent",
    kind: "meeting_update",
    relevance_score: 0,
    confidence: 0.9,
    content: "",
    payload_json: JSON.stringify({ updates: [], nudge, surfaced: true }),
    source_chunk_ids: [],
  });
  const proxyNudge = (overrides: Record<string, unknown> = {}) => ({
    id: randomUUID(),
    type: "proxy_update",
    item_id: itemId,
    content: "Priya sent her agent. Her update: the vendor contract is signed.",
    user_id: proxied,
    ...overrides,
  });
  const routeNudge = (nudge: unknown) => routeAgentRecords("agent-results", record(nudge));

  it("forwards it to the host only, with the item and the proxied participant", () => {
    const n = proxyNudge();
    expect(routeNudge(n)).toEqual([
      {
        channel: pubsubChannel(org, meeting),
        message: {
          type: "agent-nudge",
          userId: host,
          payload: { id: n.id, type: "proxy_update", itemId, content: n.content, userId: proxied },
        },
      },
    ]);
  });

  it("keeps an update up to 2000 chars (other nudges stay at 200)", () => {
    const [long] = routeNudge(proxyNudge({ content: "u".repeat(2500) }));
    expect((long.message.payload as { content: string }).content).toHaveLength(2000);
    const [other] = routeNudge(proxyNudge({ type: "over_time", content: "o".repeat(2500) }));
    expect((other.message.payload as { content: string }).content).toHaveLength(200);
    expect(other.message.payload).not.toHaveProperty("userId");
  });

  it("omits a missing or malformed proxied participant rather than dropping the nudge", () => {
    for (const userId of [undefined, null, "priya"]) {
      const [routed] = routeNudge(proxyNudge({ user_id: userId }));
      expect(routed.message.type).toBe("agent-nudge");
      expect(routed.message.payload).not.toHaveProperty("userId");
    }
  });

  it("rejects a proxy_update without an item or content", () => {
    expect(routeNudge(proxyNudge({ item_id: null }))).toEqual([]);
    expect(routeNudge(proxyNudge({ item_id: "item-1" }))).toEqual([]);
    expect(routeNudge(proxyNudge({ content: "  " }))).toEqual([]);
    expect(routeNudge(proxyNudge({ id: "nope" }))).toEqual([]);
    expect(routeNudge(proxyNudge({ type: "proxy" }))).toEqual([]);
  });
});

describe("C7 alerts through the priority queue (real Redis)", () => {
  let redis: Redis;
  beforeAll(() => {
    redis = new Redis(REDIS_URL);
  });
  afterAll(() => redis.disconnect());

  it("keeps the already_decided and topic_watch fields from enqueue to drain", async () => {
    const recipient = randomUUID();
    const ad = payloadOf(alreadyDecided());
    const tw = payloadOf(topicWatch());
    await enqueueAlert(redis, org, recipient, tw);
    await enqueueAlert(redis, org, recipient, ad);
    const drained = await drainAlerts(redis, org, recipient, meeting);
    expect(drained).toEqual([ad, tw]); // priority 2 before priority 3
  });
});
