/**
 * ProactiveAgent alerts and the per-user priority queue (Phase P11, plan 10-13 section 3.7; P11
 * contract sections 1 and 4).
 *
 * 1. `routeAgentRecord`: a surfaced, well formed `alert` record becomes "agent-alert" for its
 *    recipient; a silent or malformed one is skipped; links and related chips are sanitised.
 * 2. The queue itself (real Redis): score = priority * 1e13 + epoch ms, ZPOPMIN order, 24 h TTL,
 *    idempotent on replay.
 * 3. Real Redis + real Socket.io:
 *    - owner-only delivery: both of the recipient's tabs, never another participant, never
 *      another org whose meeting has the same id;
 *    - priority order: a priority-1 alert queued after two priority-3 alerts is delivered first
 *      when the user joins;
 *    - two gateway instances on one Redis: an alert produced via instance 1 reaches a socket on
 *      instance 2, with the hop measured;
 *    - the shed order under induced backpressure: deltas are dropped, captions, suggestions and
 *      alerts all arrive.
 */
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";

import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { Socket as ClientSocket } from "socket.io-client";

import { agentQueueKey, pubsubChannel } from "../../../libs/keys";
import { routeAgentRecord, type AgentAlertPayload } from "../src/agentOutput";
import {
  ALERT_MAX_AGE_MS,
  ALERT_QUEUE_CAP,
  ALERT_QUEUE_TTL_SECONDS,
  alertScore,
  drainAlerts,
  enqueueAlert,
} from "../src/alertQueue";
import { registry } from "../src/metrics";
import type { JoinAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";
const OCCURRED_AT_MICROS = Date.UTC(2026, 10, 10, 10, 0, 0) * 1000;

type Priority = 1 | 2 | 3;
const TYPE_FOR: Record<Priority, string> = {
  1: "conflict",
  2: "workload",
  3: "missing_stakeholder",
};

function rawAlert(overrides: Record<string, unknown> = {}) {
  return {
    id: randomUUID(),
    alert_type: "conflict",
    priority: 1,
    title: "This conflicts with the Q3 vendor decision",
    body: "The team just chose Beta Corp; on Oct 2 the team chose Acme for the same contract.",
    confidence: 0.86,
    surfaced: true,
    rationale: "Both decisions name a single vendor for the Q3 support contract.",
    subject: { kind: "decision", id: randomUUID() },
    related: [{ kind: "decision", id: randomUUID(), label: "Acme chosen for Q3 support" }],
    link: "/decisions/" + randomUUID(),
    ...overrides,
  };
}

function alertRecord(orgId: string, meetingId: string, userId: string, alert: unknown) {
  return {
    header: {
      org_id: orgId,
      idempotency_key: "k",
      event_id: randomUUID(),
      occurred_at: OCCURRED_AT_MICROS,
    },
    meeting_id: meetingId,
    window_id: "proactive:conflict:x",
    user_id: userId,
    agent: "ProactiveAgent",
    kind: "alert",
    relevance_score: 0.86,
    confidence: 0.86,
    content: "This conflicts with the Q3 vendor decision",
    payload_json: JSON.stringify({ alert, prompt: "proactive_conflict@v1", model_tier: "reason" }),
    source_chunk_ids: [],
  };
}

function payload(
  meetingId: string,
  priority: Priority,
  createdAt: string,
  title?: string,
): AgentAlertPayload {
  return {
    id: randomUUID(),
    meetingId,
    alertType: TYPE_FOR[priority] as AgentAlertPayload["alertType"],
    priority,
    title: title ?? `p${priority}`,
    body: "",
    confidence: 0.9,
    rationale: null,
    subject: null,
    related: [],
    link: null,
    createdAt,
  };
}

async function waitFor(cond: () => boolean, timeoutMs = 4000): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error("timed out waiting for condition");
    await sleep(5);
  }
}

describe("routeAgentRecord: ProactiveAgent alerts", () => {
  const org = randomUUID();
  const meeting = randomUUID();
  const user = randomUUID();

  it("forwards a surfaced alert to its recipient with the contract's socket shape", () => {
    const a = rawAlert();
    const routed = routeAgentRecord("agent-results", alertRecord(org, meeting, user, a));
    expect(routed).toEqual({
      channel: pubsubChannel(org, meeting),
      message: {
        type: "agent-alert",
        userId: user,
        payload: {
          id: a.id,
          meetingId: meeting,
          alertType: "conflict",
          priority: 1,
          title: a.title,
          body: a.body,
          confidence: 0.86,
          rationale: a.rationale,
          subject: a.subject,
          related: a.related,
          link: a.link,
          createdAt: "2026-11-10T10:00:00.000Z",
        },
      },
    });
    expect(JSON.stringify(routed)).not.toContain("proactive_conflict@v1");
  });

  it("skips a silent alert and every malformed one", () => {
    const skip = (alert: unknown) =>
      expect(routeAgentRecord("agent-results", alertRecord(org, meeting, user, alert))).toBeNull();
    skip(rawAlert({ surfaced: false }));
    skip(rawAlert({ surfaced: undefined }));
    skip(rawAlert({ id: "nope" }));
    skip(rawAlert({ alert_type: "gossip" }));
    skip(rawAlert({ priority: 4 }));
    skip(rawAlert({ priority: "1" }));
    skip(rawAlert({ title: "  " }));
    skip(null);
    skip([rawAlert()]);
    const wrongKind = { ...alertRecord(org, meeting, user, rawAlert()), kind: "note" };
    expect(routeAgentRecord("agent-results", wrongKind)).toBeNull();
  });

  it("drops unsafe links, bad related chips and a bad subject, and clamps long text", () => {
    const safe = (overrides: Record<string, unknown>) =>
      routeAgentRecord("agent-results", alertRecord(org, meeting, user, rawAlert(overrides)))
        ?.message.payload as AgentAlertPayload;
    expect(safe({ link: "https://evil.example" }).link).toBeNull();
    expect(safe({ link: "//evil.example/x" }).link).toBeNull();
    expect(safe({ link: "javascript:alert(1)" }).link).toBeNull();
    expect(safe({ link: '/meetings/x" onload="y' }).link).toBeNull();
    expect(safe({ link: null }).link).toBeNull();
    const userId = randomUUID();
    expect(
      safe({
        related: [
          { kind: "user", id: userId, label: "Priya" },
          { kind: "document", id: randomUUID(), label: "x" },
          { kind: "action", id: "nope", label: "x" },
        ],
      }).related,
    ).toEqual([{ kind: "user", id: userId, label: "Priya" }]);
    expect(safe({ subject: { kind: "planet", id: randomUUID() } }).subject).toBeNull();
    const long = safe({ title: "t".repeat(500), body: "b".repeat(900) });
    expect(long.title).toHaveLength(120);
    expect(long.body).toHaveLength(400);
  });
});

describe("the per-user priority queue (real Redis)", () => {
  let redis: Redis;
  beforeAll(() => {
    redis = new Redis(REDIS_URL);
  });
  afterAll(() => redis.disconnect());

  it("scores priority first, then age", () => {
    const t = Date.UTC(2026, 10, 10);
    expect(alertScore(1, t + 60_000)).toBeLessThan(alertScore(2, t));
    expect(alertScore(2, t + 60_000)).toBeLessThan(alertScore(3, t));
    expect(alertScore(3, t)).toBeLessThan(alertScore(3, t + 1));
    expect(Number.isSafeInteger(alertScore(3, t))).toBe(true);
  });

  // The clock the queue sees in these tests, so fixed fixture times never age past 24 h.
  const NOW = Date.parse("2026-11-10T12:00:00.000Z");

  it("pops a priority-1 queued after two priority-3 first, keeps a 24 h TTL, and is idempotent on replay", async () => {
    const org = randomUUID();
    const user = randomUUID();
    const meeting = randomUUID();
    const noteA = payload(meeting, 3, "2026-11-10T10:00:00.000Z", "note A");
    const noteB = payload(meeting, 3, "2026-11-10T10:00:01.000Z", "note B");
    const conflict = payload(meeting, 1, "2026-11-10T10:00:02.000Z", "conflict");
    await enqueueAlert(redis, org, user, noteA, NOW);
    await enqueueAlert(redis, org, user, noteB, NOW);
    await enqueueAlert(redis, org, user, conflict, NOW);
    await enqueueAlert(redis, org, user, conflict, NOW); // a replayed record

    const key = agentQueueKey(org, user);
    expect(key).toBe(`nexus:${org}:queue:agent:${user}`);
    expect(await redis.zcard(key)).toBe(3);
    const ttl = await redis.ttl(key);
    expect(ttl).toBeGreaterThan(ALERT_QUEUE_TTL_SECONDS - 10);
    expect(ttl).toBeLessThanOrEqual(ALERT_QUEUE_TTL_SECONDS);

    const drained = await drainAlerts(redis, org, user, meeting, NOW);
    expect(drained.map((a) => a.title)).toEqual(["conflict", "note A", "note B"]);
    expect(await redis.exists(key)).toBe(0);
    expect(await drainAlerts(redis, org, user, meeting, NOW)).toEqual([]);
  });

  it("drains only the alerts about the given meeting; the rest stay queued in order", async () => {
    const org = randomUUID();
    const user = randomUUID();
    const x = randomUUID();
    const y = randomUUID();
    await enqueueAlert(
      redis,
      org,
      user,
      payload(y, 1, "2026-11-10T10:00:00.000Z", "Y conflict"),
      NOW,
    );
    await enqueueAlert(redis, org, user, payload(x, 3, "2026-11-10T10:00:01.000Z", "X note"), NOW);
    await enqueueAlert(
      redis,
      org,
      user,
      payload(y, 2, "2026-11-10T10:00:02.000Z", "Y workload"),
      NOW,
    );
    await enqueueAlert(
      redis,
      org,
      user,
      payload(x, 2, "2026-11-10T10:00:03.000Z", "X workload"),
      NOW,
    );

    const inX = await drainAlerts(redis, org, user, x, NOW);
    expect(inX.map((a) => a.title)).toEqual(["X workload", "X note"]);
    expect(await redis.zcard(agentQueueKey(org, user))).toBe(2);
    const inY = await drainAlerts(redis, org, user, y, NOW);
    expect(inY.map((a) => a.title)).toEqual(["Y conflict", "Y workload"]);
    expect(await redis.exists(agentQueueKey(org, user))).toBe(0);
  });

  it("drops alerts older than 24 h on enqueue and never drains one", async () => {
    const org = randomUUID();
    const user = randomUUID();
    const meeting = randomUUID();
    const iso = (ms: number) => new Date(ms).toISOString();
    const stale = payload(meeting, 1, iso(NOW - ALERT_MAX_AGE_MS - 60_000), "stale conflict");
    const fresh = payload(meeting, 3, iso(NOW - 60_000), "fresh note");
    await enqueueAlert(redis, org, user, stale, NOW - ALERT_MAX_AGE_MS - 120_000); // fresh when queued
    expect(await redis.zcard(agentQueueKey(org, user))).toBe(1);
    // a day later, the next enqueue purges it
    await enqueueAlert(redis, org, user, fresh, NOW);
    expect(await redis.zcard(agentQueueKey(org, user))).toBe(1);
    expect((await drainAlerts(redis, org, user, meeting, NOW)).map((a) => a.title)).toEqual([
      "fresh note",
    ]);

    // and the drain itself never delivers an expired member that is still stored
    await enqueueAlert(redis, org, user, payload(meeting, 2, iso(NOW), "ages out"), NOW);
    expect(await drainAlerts(redis, org, user, meeting, NOW + ALERT_MAX_AGE_MS + 1)).toEqual([]);
    expect(await redis.exists(agentQueueKey(org, user))).toBe(0);
  });

  it("caps the queue at 100: the least urgent go first, a red conflict is never dropped for notes", async () => {
    const org = randomUUID();
    const user = randomUUID();
    const meeting = randomUUID();
    const key = agentQueueKey(org, user);
    const base = NOW - 3_600_000;
    for (let i = 0; i < ALERT_QUEUE_CAP + 20; i++) {
      await enqueueAlert(
        redis,
        org,
        user,
        payload(meeting, 3, new Date(base + i * 1000).toISOString(), `note ${i}`),
        NOW,
      );
    }
    expect(await redis.zcard(key)).toBe(ALERT_QUEUE_CAP);
    await enqueueAlert(
      redis,
      org,
      user,
      payload(meeting, 1, new Date(NOW).toISOString(), "conflict"),
      NOW,
    );
    expect(await redis.zcard(key)).toBe(ALERT_QUEUE_CAP);
    const drained = await drainAlerts(redis, org, user, meeting, NOW);
    expect(drained).toHaveLength(ALERT_QUEUE_CAP);
    expect(drained[0].title).toBe("conflict");
    expect(drained[1].title).toBe("note 0");
    expect(drained[drained.length - 1].title).toBe(`note ${ALERT_QUEUE_CAP - 2}`);
  });
});

describe("alert delivery (real Redis, real Socket.io)", () => {
  let t: TestApp;
  let t2: TestApp;
  let publisher: Redis;
  const sockets: ClientSocket[] = [];

  beforeAll(async () => {
    t = await startTestApp({ deltaShedBytes: 64 * 1024, deltaShedPackets: 16 });
    t2 = await startTestApp();
    publisher = new Redis(REDIS_URL);
  });

  afterEach(() => {
    for (const s of sockets.splice(0)) s.close();
  });

  afterAll(async () => {
    publisher.disconnect();
    await t.stop();
    await t2.stop();
  });

  async function join(
    app: TestApp,
    clerkOrg: string,
    meetingId: string,
    internalOrg: string,
    internalUser: string,
    beforeJoin?: (s: ClientSocket) => void,
  ) {
    const clerkUser = `user-${randomUUID()}`;
    app.coreApi.allowParticipant(clerkOrg, meetingId, clerkUser, {
      orgId: internalOrg,
      userId: internalUser,
    });
    const token = await app.jwks.signToken({ orgId: clerkOrg, userId: clerkUser });
    const socket = await connectClient(app.baseUrl, token);
    sockets.push(socket);
    beforeJoin?.(socket);
    const ack = await new Promise<JoinAck>((resolve) =>
      socket.emit("join", { meetingId }, resolve),
    );
    expect(ack.ok).toBe(true);
    return socket;
  }

  function routedFor(
    orgId: string,
    meetingId: string,
    userId: string,
    overrides: Record<string, unknown> = {},
  ) {
    const record = alertRecord(orgId, meetingId, userId, rawAlert(overrides));
    // a current timestamp, so the queue's 24 h cutoff never ages the fixture out
    record.header.occurred_at = Date.now() * 1000;
    const routed = routeAgentRecord("agent-results", record);
    expect(routed?.message.type).toBe("agent-alert");
    return routed!;
  }

  it("reaches only the recipient's sockets, never the rest of the room or another org", async () => {
    const meetingId = randomUUID();
    const orgA = randomUUID();
    const orgB = randomUUID();
    const alice = randomUUID();
    const bob = randomUUID();
    const clerkOrgA = `org-${randomUUID()}`;

    const got: Record<string, unknown[]> = { alice: [], aliceTab: [], bob: [], other: [] };
    const listen = (name: string) => (s: ClientSocket) =>
      s.on("agent-alert", (e) => got[name].push(e));
    await join(t, clerkOrgA, meetingId, orgA, alice, listen("alice"));
    await join(t, clerkOrgA, meetingId, orgA, alice, listen("aliceTab"));
    await join(t, clerkOrgA, meetingId, orgA, bob, listen("bob"));
    // another tenant whose meeting has the literal same id, and whose user has alice's id
    await join(t, `org-${randomUUID()}`, meetingId, orgB, alice, listen("other"));

    const routed = routedFor(orgA, meetingId, alice);
    await t.app.queueAlert(routed);

    await waitFor(() => got.alice.length >= 1 && got.aliceTab.length >= 1);
    await sleep(300); // give a leak time to arrive before asserting its absence
    const expected = { meetingId, payload: routed.message.payload };
    expect(got.alice).toEqual([expected]);
    expect(got.aliceTab).toEqual([expected]);
    expect(got.bob).toEqual([]);
    expect(got.other).toEqual([]);
    // delivered, so nothing is left waiting
    expect(await publisher.exists(agentQueueKey(orgA, alice))).toBe(0);
  });

  it("keeps an alert queued while the user has no socket, then delivers most urgent first on join", async () => {
    const meetingId = randomUUID();
    const org = randomUUID();
    const carol = randomUUID();
    const base = Date.now();
    const at = (s: number) => new Date(base + s * 1000).toISOString();
    // two priority-3 notes queued, then a priority-1 conflict queued after them
    await t.app.queueAlert({
      channel: pubsubChannel(org, meetingId),
      message: {
        type: "agent-alert",
        userId: carol,
        payload: payload(meetingId, 3, at(0), "note A"),
      },
    });
    await t.app.queueAlert({
      channel: pubsubChannel(org, meetingId),
      message: {
        type: "agent-alert",
        userId: carol,
        payload: payload(meetingId, 3, at(1), "note B"),
      },
    });
    await t.app.queueAlert({
      channel: pubsubChannel(org, meetingId),
      message: {
        type: "agent-alert",
        userId: carol,
        payload: payload(meetingId, 1, at(2), "conflict"),
      },
    });
    await sleep(100);
    expect(await publisher.zcard(agentQueueKey(org, carol))).toBe(3);

    const titles: string[] = [];
    await join(t, `org-${randomUUID()}`, meetingId, org, carol, (s) =>
      s.on("agent-alert", (e: { payload: AgentAlertPayload }) => titles.push(e.payload.title)),
    );
    await waitFor(() => titles.length >= 3);
    expect(titles).toEqual(["conflict", "note A", "note B"]);
    expect(await publisher.exists(agentQueueKey(org, carol))).toBe(0);
  });

  it("never delivers an alert into another meeting's room: an alert about Y waits while the user is in X", async () => {
    const x = randomUUID();
    const y = randomUUID();
    const org = randomUUID();
    const frank = randomUUID();
    const clerkOrg = `org-${randomUUID()}`;
    const inX: unknown[] = [];
    await join(t, clerkOrg, x, org, frank, (s) => s.on("agent-alert", (e) => inX.push(e)));

    const aboutY = routedFor(org, y, frank, { title: "About Y" });
    await t.app.queueAlert(aboutY); // nudges Y's channel, where frank has no socket
    const aboutX = routedFor(org, x, frank, {
      priority: 3,
      alert_type: "missing_stakeholder",
      title: "About X",
    });
    await t.app.queueAlert(aboutX); // nudges X's channel: drains X's alerts only

    await waitFor(() => inX.length >= 1);
    await sleep(300);
    expect(inX).toEqual([{ meetingId: x, payload: aboutX.message.payload }]);
    expect(await publisher.zcard(agentQueueKey(org, frank))).toBe(1); // Y's alert still waits

    // joining X again (a reconnect) still does not pull Y's alert in
    const again: unknown[] = [];
    await join(t, clerkOrg, x, org, frank, (s) => s.on("agent-alert", (e) => again.push(e)));
    await sleep(300);
    expect(again).toEqual([]);

    // joining Y delivers it, into Y's room
    const inY: { meetingId: string; payload: AgentAlertPayload }[] = [];
    await join(t, clerkOrg, y, org, frank, (s) =>
      s.on("agent-alert", (e: (typeof inY)[number]) => inY.push(e)),
    );
    await waitFor(() => inY.length >= 1);
    expect(inY).toEqual([{ meetingId: y, payload: aboutY.message.payload }]);
    expect(await publisher.exists(agentQueueKey(org, frank))).toBe(0);
  });

  it("crosses instances: produced via instance 1, delivered to a socket on instance 2 (hop measured)", async () => {
    const meetingId = randomUUID();
    const org = randomUUID();
    const dave = randomUUID();
    const arrivals = new Map<string, number>();
    await join(t2, `org-${randomUUID()}`, meetingId, org, dave, (s) =>
      s.on("agent-alert", (e: { payload: AgentAlertPayload }) =>
        arrivals.set(e.payload.id, performance.now()),
      ),
    );

    const hops: number[] = [];
    for (let i = 0; i < 40; i++) {
      const routed = routedFor(org, meetingId, dave, { priority: 2, alert_type: "workload" });
      const id = (routed.message.payload as AgentAlertPayload).id;
      const sent = performance.now();
      await t.app.queueAlert(routed); // instance 1: ZADD + nudge; instance 2 drains and delivers
      await waitFor(() => arrivals.has(id));
      hops.push(arrivals.get(id)! - sent);
    }
    hops.sort((a, b) => a - b);
    const p50 = hops[Math.floor(hops.length * 0.5)];
    const p95 = hops[Math.floor(hops.length * 0.95)];
    // eslint-disable-next-line no-console
    console.log(
      `[cross-instance alert hop] n=${hops.length} p50=${p50.toFixed(2)}ms p95=${p95.toFixed(2)}ms max=${hops[hops.length - 1].toFixed(2)}ms`,
    );
    expect(p95).toBeLessThan(50); // plan 3.7 acceptance: the fan-out hop < 50 ms
  });

  it("sheds agent-delta first under backpressure; captions, suggestions and alerts all arrive", async () => {
    const meetingId = randomUUID();
    const org = randomUUID();
    const erin = randomUUID();
    const channel = pubsubChannel(org, meetingId);
    const counts = { caption: 0, delta: 0, suggestion: 0, alert: 0 };
    const socket = await join(t, `org-${randomUUID()}`, meetingId, org, erin, (s) => {
      s.on("caption", () => counts.caption++);
      s.on("agent-delta", () => counts.delta++);
      s.on("agent-suggestion", () => counts.suggestion++);
      s.on("agent-alert", () => counts.alert++);
    });

    const shedBefore = await shedCount();
    // Induce backpressure: stop reading the client's TCP socket so the server's send buffer fills.
    const tcp = (
      socket.io.engine.transport as unknown as {
        ws: { _socket: { pause(): void; resume(): void } };
      }
    ).ws._socket;
    tcp.pause();

    const CAPTIONS = 80;
    const DELTAS = 80;
    const big = "x".repeat(64 * 1024);
    for (let i = 0; i < CAPTIONS; i++) {
      await publisher.publish(
        channel,
        JSON.stringify({
          type: "caption",
          payload: { chunkId: `c${i}`, text: big, isFinal: true },
        }),
      );
      await publisher.publish(
        channel,
        JSON.stringify({
          type: "agent-delta",
          userId: erin,
          payload: { windowId: `${meetingId}:0`, seq: i, delta: "tok ", done: false },
        }),
      );
    }
    await publisher.publish(
      channel,
      JSON.stringify({
        type: "agent-suggestion",
        userId: erin,
        payload: { windowId: "w", agent: "QuestionAgent", kind: "suggestion", suggestions: [] },
      }),
    );
    await t.app.queueAlert(routedFor(org, meetingId, erin));
    await sleep(500);

    tcp.resume();
    await waitFor(
      () => counts.caption === CAPTIONS && counts.alert === 1 && counts.suggestion === 1,
      15_000,
    );
    await sleep(200);
    const shed = (await shedCount()) - shedBefore;
    // eslint-disable-next-line no-console
    console.log(
      `[backpressure] captions ${counts.caption}/${CAPTIONS}, deltas ${counts.delta}/${DELTAS}, shed ${shed}`,
    );
    expect(counts.caption).toBe(CAPTIONS);
    expect(counts.suggestion).toBe(1);
    expect(counts.alert).toBe(1);
    expect(counts.delta).toBeLessThan(DELTAS);
    expect(shed).toBeGreaterThan(0);
  }, 30_000);
});

async function shedCount(): Promise<number> {
  const metric = await registry.getSingleMetric("mytheus_realtime_deltas_shed_total")?.get();
  return metric?.values[0]?.value ?? 0;
}
