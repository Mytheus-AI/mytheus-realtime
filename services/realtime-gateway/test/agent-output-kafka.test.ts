/**
 * Agent output against the real broker: real Avro `agent-results` and `agent-stream` records on
 * Redpanda (localhost:9092, Schema Registry localhost:8085) go through the gateway's real
 * consumer and reach only the owner's socket. Complements test/agent-output.test.ts, which
 * injects on the Redis channel directly.
 */
import { randomUUID } from "node:crypto";
import type { AddressInfo } from "node:net";

import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { Kafka, Partitioners, type Producer } from "kafkajs";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Socket as ClientSocket } from "socket.io-client";

import { createClerkJwtVerifier } from "../../../libs/auth";
import { createApp, type App, type JoinAck } from "../src/server";
import { connectClient, createFakePresenceEventsProducer } from "./helpers/testApp";
import { startFakeCoreApi, type FakeCoreApi } from "./helpers/fakeCoreApi";
import { startFakeJwksServer, type FakeJwksServer } from "./helpers/fakeJwks";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";
const BROKERS = (process.env.REDPANDA_BROKERS ?? "localhost:9092").split(",");
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL ?? "http://localhost:8085";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function waitFor(cond: () => boolean, timeoutMs = 20_000): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error("timed out waiting for condition");
    await sleep(50);
  }
}

describe("agent output from the real broker to the owner's socket", () => {
  let app: App;
  let jwks: FakeJwksServer;
  let coreApi: FakeCoreApi;
  let baseUrl: string;
  let producer: Producer;
  let registry: SchemaRegistry;
  const sockets: ClientSocket[] = [];

  beforeAll(async () => {
    jwks = await startFakeJwksServer();
    coreApi = await startFakeCoreApi();
    app = createApp(
      {
        port: 0,
        redisUrl: REDIS_URL,
        coreApiUrl: coreApi.baseUrl,
        redpandaBrokers: BROKERS,
        schemaRegistryUrl: SCHEMA_REGISTRY_URL,
        agentOutput: true,
      },
      {
        jwtVerifier: createClerkJwtVerifier({ jwksUrl: jwks.jwksUrl }),
        presenceEventsProducer: createFakePresenceEventsProducer(),
      },
    );
    await app.start();
    baseUrl = `http://127.0.0.1:${(app.httpServer.address() as AddressInfo).port}`;
    registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });
    producer = new Kafka({ clientId: "agent-output-test", brokers: BROKERS }).producer({
      createPartitioner: Partitioners.DefaultPartitioner,
    });
    await producer.connect();
  }, 60_000);

  afterAll(async () => {
    for (const s of sockets) s.close();
    await producer?.disconnect();
    await app?.stop();
    await jwks?.close();
    await coreApi?.close();
  });

  async function join(clerkOrg: string, meetingId: string, orgId: string, userId: string) {
    const clerkUser = `user-${randomUUID()}`;
    coreApi.allowParticipant(clerkOrg, meetingId, clerkUser, { orgId, userId });
    const socket = await connectClient(
      baseUrl,
      await jwks.signToken({ orgId: clerkOrg, userId: clerkUser }),
    );
    sockets.push(socket);
    const ack = await new Promise<JoinAck>((resolve) =>
      socket.emit("join", { meetingId }, resolve),
    );
    expect(ack.ok).toBe(true);
    return socket;
  }

  function header(orgId: string, key: string) {
    return {
      event_id: randomUUID(),
      idempotency_key: key,
      org_id: orgId,
      schema_version: 1,
      producer: "agent-runtime-service",
      occurred_at: Date.now(),
      correlation_id: null,
      causation_id: null,
    };
  }

  it("delivers a real streamed note and its final to the owner only", async () => {
    const orgId = randomUUID();
    const meetingId = randomUUID();
    const alice = randomUUID();
    const bob = randomUUID();
    const clerkOrg = `org-${randomUUID()}`;
    const aliceSocket = await join(clerkOrg, meetingId, orgId, alice);
    const bobSocket = await join(clerkOrg, meetingId, orgId, bob);
    const got: Record<string, unknown[]> = { alice: [], bob: [] };
    aliceSocket.on("agent-delta", (e) => got.alice.push(["delta", e]));
    aliceSocket.on("agent-note", (e) => got.alice.push(["note", e]));
    bobSocket.on("agent-delta", (e) => got.bob.push(e));
    bobSocket.on("agent-note", (e) => got.bob.push(e));

    const streamId = await registry.getLatestSchemaId("agent-stream-value");
    const resultsId = await registry.getLatestSchemaId("agent-results-value");
    const windowId = `${meetingId}:0`;

    // the consumer group joins asynchronously; keep producing the delta until it is seen
    const deltaValue = await registry.encode(streamId, {
      header: header(orgId, `agentstream:${windowId}:${alice}:0`),
      meeting_id: meetingId,
      user_id: alice,
      window_id: windowId,
      seq: 0,
      delta: "- You own the migration.",
      done: false,
    });
    const deadline = Date.now() + 30_000;
    while (!got.alice.some((g) => (g as unknown[])[0] === "delta") && Date.now() < deadline) {
      await producer.send({
        topic: "agent-stream",
        messages: [{ key: `${orgId}:${meetingId}`, value: deltaValue }],
      });
      await sleep(1500);
    }

    const silent = await registry.encode(resultsId, {
      header: header(orgId, `agentresult:${meetingId}:${windowId}:${bob}:PersonalListenerAgent`),
      meeting_id: meetingId,
      window_id: windowId,
      user_id: bob,
      agent: "PersonalListenerAgent",
      kind: "note",
      relevance_score: 0.8,
      confidence: 0.55,
      content: "- silent band",
      payload_json: JSON.stringify({ surfaced: false }),
      source_chunk_ids: [randomUUID()],
    });
    const surfaced = await registry.encode(resultsId, {
      header: header(orgId, `agentresult:${meetingId}:${windowId}:${alice}:PersonalListenerAgent`),
      meeting_id: meetingId,
      window_id: windowId,
      user_id: alice,
      agent: "PersonalListenerAgent",
      kind: "note",
      relevance_score: 0.9,
      confidence: 0.92,
      content: "- You own the migration, due Friday.",
      payload_json: JSON.stringify({ surfaced: true }),
      source_chunk_ids: [randomUUID()],
    });
    await producer.send({
      topic: "agent-results",
      messages: [
        { key: orgId, value: silent },
        { key: orgId, value: surfaced },
      ],
    });

    await waitFor(() => got.alice.some((g) => (g as unknown[])[0] === "note"));
    await sleep(500);
    const note = got.alice.find((g) => (g as unknown[])[0] === "note") as [
      string,
      { meetingId: string; payload: { content: string } },
    ];
    expect(note[1].meetingId).toBe(meetingId);
    expect(note[1].payload.content).toBe("- You own the migration, due Friday.");
    expect(got.bob).toEqual([]);
  }, 90_000);
});
