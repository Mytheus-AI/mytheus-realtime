/**
 * MeetingAgent output (mytheus-docs ADR-0038 D7, D8, D9).
 *
 * 1. `routeAgentRecords`: one `meeting_update` record is split into one "agent-agenda" message per
 *    well-formed entry of `updates` (meeting-scoped: no `userId`) and at most one "agent-nudge"
 *    for the host (tagged with the record's `user_id`). Each entry is validated on its own; a
 *    record that is not `surfaced === true`, or whose payload does not parse, yields nothing.
 * 2. Decoding: a record written with a schema that carries the new `MeetingAgent` and
 *    `meeting_update` enum symbols decodes through the real `SchemaRegistry.decode` (writer
 *    schema, no reader schema), exactly as the consumer calls it, and routes.
 * 3. Real Redis + real Socket.io on two gateway pods: "agent-agenda" reaches every participant of
 *    the meeting on both pods; "agent-nudge" reaches only the host's sockets; neither leaks into
 *    another org whose meeting shares the id.
 */
import { randomUUID } from "node:crypto";
import { createServer, type Server as HttpServer } from "node:http";
import type { AddressInfo } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";

import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { Socket as ClientSocket } from "socket.io-client";

import { pubsubChannel } from "../../../libs/keys";
import { routeAgentRecord, routeAgentRecords } from "../src/agentOutput";
import type { JoinAck } from "../src/server";
import { connectClient, startTestApp, type TestApp } from "./helpers/testApp";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";

function meetingRecord(orgId: string, meetingId: string, hostId: string, payload: unknown) {
  return {
    header: {
      org_id: orgId,
      idempotency_key: `agentresult:${meetingId}:${meetingId}:4:${hostId}:MeetingAgent`,
      event_id: randomUUID(),
      occurred_at: Date.now() * 1000,
    },
    meeting_id: meetingId,
    window_id: `${meetingId}:4`,
    user_id: hostId,
    agent: "MeetingAgent",
    kind: "meeting_update",
    relevance_score: 0,
    confidence: 0.9,
    content: "",
    payload_json: typeof payload === "string" ? payload : JSON.stringify(payload),
    source_chunk_ids: [],
  };
}

function update(overrides: Record<string, unknown> = {}) {
  return {
    item_id: randomUUID(),
    status: "in_progress",
    note: "Budget review started; Alice is walking through Q3.",
    ...overrides,
  };
}

function nudge(overrides: Record<string, unknown> = {}) {
  return {
    id: randomUUID(),
    type: "over_time",
    item_id: randomUUID(),
    content: "Budget review is 5 minutes over its 10.",
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

describe("routeAgentRecords: MeetingAgent meeting_update", () => {
  const org = randomUUID();
  const meeting = randomUUID();
  const host = randomUUID();
  const channel = pubsubChannel(org, meeting);
  const route = (payload: unknown) =>
    routeAgentRecords("agent-results", meetingRecord(org, meeting, host, payload));

  it("splits updates into meeting-scoped agenda messages and the nudge into a host-only one", () => {
    const u1 = update();
    const u2 = update({ status: "covered", note: "" });
    const n = nudge();
    expect(route({ updates: [u1, u2], nudge: n, surfaced: true })).toEqual([
      {
        channel,
        message: {
          type: "agent-agenda",
          payload: { itemId: u1.item_id, status: "in_progress", note: u1.note },
        },
      },
      {
        channel,
        message: {
          type: "agent-agenda",
          payload: { itemId: u2.item_id, status: "covered", note: "" },
        },
      },
      {
        channel,
        message: {
          type: "agent-nudge",
          userId: host,
          payload: { id: n.id, type: "over_time", itemId: n.item_id, content: n.content },
        },
      },
    ]);
  });

  it("agenda messages carry no userId; only the nudge is tagged with the host", () => {
    const routed = route({ updates: [update()], nudge: nudge(), surfaced: true });
    expect(routed.map((r) => r.message.type)).toEqual(["agent-agenda", "agent-nudge"]);
    expect("userId" in routed[0].message).toBe(false);
    expect((routed[1].message as { userId: string }).userId).toBe(host);
  });

  it("forwards updates alone when the nudge is null, and a nudge alone when updates is empty", () => {
    expect(
      route({ updates: [update()], nudge: null, surfaced: true }).map((r) => r.message.type),
    ).toEqual(["agent-agenda"]);
    const offAgenda = nudge({ type: "off_agenda", item_id: null });
    const routed = route({ updates: [], nudge: offAgenda, surfaced: true });
    expect(routed).toHaveLength(1);
    expect(routed[0].message.payload).toEqual({
      id: offAgenda.id,
      type: "off_agenda",
      itemId: null,
      content: offAgenda.content,
    });
    const unaddressed = route({
      updates: [],
      nudge: nudge({ type: "unaddressed" }),
      surfaced: true,
    });
    expect(unaddressed[0].message.payload).toMatchObject({ type: "unaddressed" });
  });

  it("cuts a note to 300 chars and nudge content to 200 chars, and strips unknown fields", () => {
    const routed = route({
      updates: [update({ note: "n".repeat(400), confidence: 0.9, secret: "x" })],
      nudge: nudge({ content: "c".repeat(250), confidence: 0.8, rationale: "internal" }),
      surfaced: true,
    });
    expect((routed[0].message.payload as { note: string }).note).toBe("n".repeat(300));
    expect(Object.keys(routed[0].message.payload).sort()).toEqual(["itemId", "note", "status"]);
    expect((routed[1].message.payload as { content: string }).content).toBe("c".repeat(200));
    expect(Object.keys(routed[1].message.payload).sort()).toEqual([
      "content",
      "id",
      "itemId",
      "type",
    ]);
  });

  it("drops each bad update on its own and keeps the good ones", () => {
    const good = update();
    const bad = [
      update({ item_id: "not-a-uuid" }),
      update({ item_id: undefined }),
      update({ status: "pending" }),
      update({ status: "skipped" }),
      update({ status: undefined }),
      update({ note: undefined }),
      update({ note: null }),
      update({ note: 7 }),
      null,
      "covered",
      [good],
    ];
    const routed = route({ updates: [...bad, good], nudge: null, surfaced: true });
    expect(routed).toEqual([
      {
        channel,
        message: {
          type: "agent-agenda",
          payload: { itemId: good.item_id, status: "in_progress", note: good.note },
        },
      },
    ]);
  });

  it("keeps only the first update for an item repeated in one record", () => {
    const itemId = randomUUID();
    const routed = route({
      updates: [update({ item_id: itemId }), update({ item_id: itemId, status: "covered" })],
      nudge: null,
      surfaced: true,
    });
    expect(routed).toHaveLength(1);
    expect(routed[0].message.payload).toMatchObject({ itemId, status: "in_progress" });
  });

  it("drops a malformed nudge but still forwards the record's updates", () => {
    const badNudges = [
      nudge({ id: "nope" }),
      nudge({ id: undefined }),
      nudge({ type: "boring" }),
      nudge({ type: undefined }),
      nudge({ item_id: "item-1" }),
      nudge({ item_id: undefined }),
      nudge({ content: "" }),
      nudge({ content: "   " }),
      nudge({ content: 42 }),
      [nudge()],
      "nudge",
    ];
    for (const n of badNudges) {
      expect(
        route({ updates: [update()], nudge: n, surfaced: true }).map((r) => r.message.type),
      ).toEqual(["agent-agenda"]);
    }
  });

  it("skips the whole record unless surfaced is true or when the payload is malformed", () => {
    const body = { updates: [update()], nudge: nudge() };
    expect(route({ ...body, surfaced: false })).toEqual([]);
    expect(route({ ...body, surfaced: "true" })).toEqual([]);
    expect(route(body)).toEqual([]);
    expect(route("{not json")).toEqual([]);
    expect(route("[1, 2]")).toEqual([]);
    expect(route("null")).toEqual([]);
    expect(route({ updates: "x", nudge: "y", surfaced: true })).toEqual([]);
    expect(route({ surfaced: true })).toEqual([]);
  });

  it("skips another kind from MeetingAgent, and a record without UUID tenant, meeting or user", () => {
    const payload = { updates: [update()], nudge: nudge(), surfaced: true };
    const record = meetingRecord(org, meeting, host, payload);
    expect(routeAgentRecords("agent-results", { ...record, kind: "note" })).toEqual([]);
    expect(routeAgentRecords("agent-results", { ...record, kind: "agenda_update" })).toEqual([]);
    expect(
      routeAgentRecords("agent-results", { ...record, header: { ...record.header, org_id: "x" } }),
    ).toEqual([]);
    expect(routeAgentRecords("agent-results", { ...record, meeting_id: "m-1" })).toEqual([]);
    expect(routeAgentRecords("agent-results", { ...record, user_id: null })).toEqual([]);
  });

  it("never single-routes a MeetingAgent record; other agents route as before", () => {
    const payload = { updates: [update()], nudge: nudge(), surfaced: true };
    expect(
      routeAgentRecord("agent-results", meetingRecord(org, meeting, host, payload)),
    ).toBeNull();
    const note = {
      ...meetingRecord(org, meeting, host, { surfaced: true }),
      agent: "PersonalListenerAgent",
      kind: "note",
      content: "- you own it",
    };
    const routed = routeAgentRecords("agent-results", note);
    expect(routed).toHaveLength(1);
    expect(routed[0].message).toMatchObject({ type: "agent-note", userId: host });
  });
});

describe("decoding a record that carries the new enum symbols", () => {
  // agent-results v1 plus ADR-0038 D8's symbols, as a producer would register it. The consumer
  // has never seen this schema: it reads every record with the writer schema its id names.
  const writerSchema = {
    type: "record",
    name: "AgentResult",
    namespace: "io.nexus.events",
    fields: [
      {
        name: "header",
        type: {
          type: "record",
          name: "EventEnvelopeHeader",
          fields: [
            { name: "event_id", type: "string" },
            { name: "idempotency_key", type: "string" },
            { name: "org_id", type: "string" },
            { name: "occurred_at", type: "long" },
          ],
        },
      },
      { name: "meeting_id", type: "string" },
      { name: "window_id", type: "string" },
      { name: "user_id", type: "string" },
      {
        name: "agent",
        type: {
          type: "enum",
          name: "AgentClass",
          symbols: [
            "PersonalListenerAgent",
            "DecisionAgent",
            "ActionAgent",
            "QuestionAgent",
            "ContextGapAgent",
            "ProactiveAgent",
            "MeetingAgent",
          ],
        },
      },
      {
        name: "kind",
        type: {
          type: "enum",
          name: "AgentResultKind",
          symbols: [
            "note",
            "decision_candidate",
            "action_candidate",
            "suggestion",
            "alert",
            "context_fill",
            "meeting_update",
          ],
          default: "note",
        },
      },
      { name: "relevance_score", type: "float" },
      { name: "confidence", type: "float" },
      { name: "content", type: "string" },
      { name: "payload_json", type: "string" },
      { name: "source_chunk_ids", type: { type: "array", items: "string" } },
    ],
  };
  let server: HttpServer;
  let registry: SchemaRegistry;

  beforeAll(async () => {
    // a stand-in Schema Registry that knows only schema id 41
    server = createServer((req, res) => {
      if (req.url === "/schemas/ids/41") {
        res.writeHead(200, { "content-type": "application/vnd.schemaregistry.v1+json" });
        res.end(JSON.stringify({ schema: JSON.stringify(writerSchema) }));
        return;
      }
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error_code: 40403, message: "Schema not found" }));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as AddressInfo;
    registry = new SchemaRegistry({ host: `http://127.0.0.1:${port}` });
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("decodes MeetingAgent / meeting_update as plain strings and routes the record", async () => {
    const org = randomUUID();
    const meeting = randomUUID();
    const host = randomUUID();
    const u = update({ status: "covered" });
    const n = nudge();
    const record = meetingRecord(org, meeting, host, { updates: [u], nudge: n, surfaced: true });
    const wire = await registry.encode(41, record);
    // exactly the consumer's call: no reader schema
    const decoded = (await registry.decode(wire)) as Record<string, unknown>;
    expect(decoded.agent).toBe("MeetingAgent");
    expect(decoded.kind).toBe("meeting_update");
    const routed = routeAgentRecords("agent-results", decoded);
    expect(routed.map((r) => r.message.type)).toEqual(["agent-agenda", "agent-nudge"]);
    expect(routed[0].message.payload).toEqual({
      itemId: u.item_id,
      status: "covered",
      note: u.note,
    });
  });
});

describe("MeetingAgent delivery across two pods (real Redis, real Socket.io)", () => {
  let pod1: TestApp;
  let pod2: TestApp;
  let publisher: Redis;
  const sockets: ClientSocket[] = [];

  beforeAll(async () => {
    pod1 = await startTestApp();
    pod2 = await startTestApp();
    publisher = new Redis(REDIS_URL);
  });

  afterEach(() => {
    for (const s of sockets.splice(0)) s.close();
  });

  afterAll(async () => {
    publisher.disconnect();
    await pod1.stop();
    await pod2.stop();
  });

  async function join(
    pod: TestApp,
    clerkOrg: string,
    meetingId: string,
    internalOrg: string,
    internalUser: string,
  ) {
    const clerkUser = `user-${randomUUID()}`;
    pod.coreApi.allowParticipant(clerkOrg, meetingId, clerkUser, {
      orgId: internalOrg,
      userId: internalUser,
    });
    const token = await pod.jwks.signToken({ orgId: clerkOrg, userId: clerkUser });
    const socket = await connectClient(pod.baseUrl, token);
    sockets.push(socket);
    const ack = await new Promise<JoinAck>((resolve) =>
      socket.emit("join", { meetingId }, resolve),
    );
    expect(ack.ok).toBe(true);
    return socket;
  }

  it("sends agenda updates to every participant on every pod and the nudge to the host only", async () => {
    const meetingId = randomUUID();
    const orgA = randomUUID();
    const orgB = randomUUID();
    const host = randomUUID();
    const bob = randomUUID();
    const clerkOrgA = `org-${randomUUID()}`;

    const hostOnPod1 = await join(pod1, clerkOrgA, meetingId, orgA, host);
    const hostOnPod2 = await join(pod2, clerkOrgA, meetingId, orgA, host);
    const bobOnPod2 = await join(pod2, clerkOrgA, meetingId, orgA, bob);
    // another tenant whose meeting has the literal same id, and whose user has the host's id
    const otherOrg = await join(pod1, `org-${randomUUID()}`, meetingId, orgB, host);
    // the same org, another meeting
    const otherMeeting = await join(pod2, clerkOrgA, randomUUID(), orgA, bob);

    const got: Record<string, Array<[string, unknown]>> = {
      host1: [],
      host2: [],
      bob: [],
      otherOrg: [],
      otherMeeting: [],
    };
    for (const [name, s] of [
      ["host1", hostOnPod1],
      ["host2", hostOnPod2],
      ["bob", bobOnPod2],
      ["otherOrg", otherOrg],
      ["otherMeeting", otherMeeting],
    ] as const) {
      s.on("agent-agenda", (e) => got[name].push(["agenda", e]));
      s.on("agent-nudge", (e) => got[name].push(["nudge", e]));
    }

    const u1 = update();
    const u2 = update({ status: "covered", note: "" });
    const n = nudge({ type: "unaddressed", item_id: null });
    const record = meetingRecord(orgA, meetingId, host, {
      updates: [u1, { item_id: "bad" }, u2],
      nudge: n,
      surfaced: true,
    });
    // exactly what the consumer does with the routed messages, in order
    for (const routed of routeAgentRecords("agent-results", record)) {
      await publisher.publish(routed.channel, JSON.stringify(routed.message));
    }

    await waitFor(() => got.host1.length >= 3 && got.host2.length >= 3 && got.bob.length >= 2);
    await sleep(300); // give a leak time to arrive before asserting its absence
    const agenda1 = {
      meetingId,
      payload: { itemId: u1.item_id, status: "in_progress", note: u1.note },
    };
    const agenda2 = { meetingId, payload: { itemId: u2.item_id, status: "covered", note: "" } };
    const nudgeEvent = {
      meetingId,
      payload: { id: n.id, type: "unaddressed", itemId: null, content: n.content },
    };
    const hostExpected = [
      ["agenda", agenda1],
      ["agenda", agenda2],
      ["nudge", nudgeEvent],
    ];
    expect(got.host1).toEqual(hostExpected);
    expect(got.host2).toEqual(hostExpected);
    expect(got.bob).toEqual([
      ["agenda", agenda1],
      ["agenda", agenda2],
    ]);
    expect(got.otherOrg).toEqual([]);
    expect(got.otherMeeting).toEqual([]);
  });

  it("ignores a nudge message without a userId instead of sending it to the room", async () => {
    const meetingId = randomUUID();
    const orgId = randomUUID();
    const clerkOrg = `org-${randomUUID()}`;
    const socket = await join(pod1, clerkOrg, meetingId, orgId, randomUUID());
    const got: unknown[] = [];
    socket.on("agent-nudge", (e) => got.push(e));
    socket.on("agent-agenda", (e) => got.push(e));
    const n = nudge();
    await publisher.publish(
      pubsubChannel(orgId, meetingId),
      JSON.stringify({ type: "agent-nudge", payload: { id: n.id, content: n.content } }),
    );
    // a well-formed agenda message afterwards proves the channel is live
    const u = update();
    await publisher.publish(
      pubsubChannel(orgId, meetingId),
      JSON.stringify({
        type: "agent-agenda",
        payload: { itemId: u.item_id, status: "in_progress", note: u.note },
      }),
    );
    await waitFor(() => got.length >= 1);
    await sleep(200);
    expect(got).toEqual([
      { meetingId, payload: { itemId: u.item_id, status: "in_progress", note: u.note } },
    ]);
  });
});
