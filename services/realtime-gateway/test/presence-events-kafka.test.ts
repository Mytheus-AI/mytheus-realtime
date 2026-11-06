/**
 * Acceptance check #4: presence set grows on join / shrinks on disconnect, and one
 * `presence-events` record is produced per transition, partitioned by `org_id` — asserted against
 * the real broker (Redpanda at localhost:9092, Schema Registry at localhost:8085), not a mock.
 * This test uses the real `presenceEventsProducer` (unlike test/gateway.test.ts, which uses an
 * in-memory double for speed) and drives it through the actual join/disconnect socket flow.
 */
import { randomUUID } from "node:crypto";
import type { AddressInfo } from "node:net";

import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { Kafka, type Consumer, type EachMessagePayload } from "kafkajs";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { createClerkJwtVerifier } from "../../../libs/auth";
import { createApp, type App, type JoinAck } from "../src/server";
import { createPresenceEventsProducer } from "../src/presenceEventsProducer";
import { connectClient } from "./helpers/testApp";
import { startFakeCoreApi, type FakeCoreApi } from "./helpers/fakeCoreApi";
import { startFakeJwksServer, type FakeJwksServer } from "./helpers/fakeJwks";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";
const REDPANDA_BROKERS = (process.env.REDPANDA_BROKERS ?? "localhost:9092").split(",");
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL ?? "http://localhost:8085";

interface DecodedPresenceEvent {
  header: { org_id: string; producer: string; idempotency_key: string };
  meeting_id: string;
  user_id: string;
  event_type: "join" | "leave";
}

describe("presence-events production against the real broker", () => {
  let app: App;
  let jwks: FakeJwksServer;
  let coreApi: FakeCoreApi;
  let baseUrl: string;
  let consumer: Consumer;
  const received: Array<{ event: DecodedPresenceEvent; partitionKey: string | null }> = [];

  beforeAll(async () => {
    jwks = await startFakeJwksServer();
    coreApi = await startFakeCoreApi();

    const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });
    const kafka = new Kafka({
      clientId: "presence-events-test-consumer",
      brokers: REDPANDA_BROKERS,
    });
    consumer = kafka.consumer({ groupId: `test-presence-events-${randomUUID()}` });
    await consumer.connect();
    await consumer.subscribe({ topic: "presence-events", fromBeginning: false });

    const groupJoined = new Promise<void>((resolve) => {
      consumer.on(consumer.events.GROUP_JOIN, () => resolve());
    });
    void consumer.run({
      eachMessage: async ({ message }: EachMessagePayload) => {
        if (!message.value) return;
        const event = (await registry.decode(message.value)) as DecodedPresenceEvent;
        received.push({ event, partitionKey: message.key ? message.key.toString() : null });
      },
    });
    await groupJoined;

    app = createApp(
      {
        port: 0,
        redisUrl: REDIS_URL,
        coreApiUrl: coreApi.baseUrl,
        redpandaBrokers: REDPANDA_BROKERS,
        schemaRegistryUrl: SCHEMA_REGISTRY_URL,
      },
      {
        jwtVerifier: createClerkJwtVerifier({ jwksUrl: jwks.jwksUrl }),
        presenceEventsProducer: createPresenceEventsProducer({
          brokers: REDPANDA_BROKERS,
          schemaRegistryUrl: SCHEMA_REGISTRY_URL,
        }),
      },
    );
    await app.start();
    const { port } = app.httpServer.address() as AddressInfo;
    baseUrl = `http://127.0.0.1:${port}`;
  }, 30000);

  afterAll(async () => {
    await consumer.disconnect();
    await app.stop();
    await jwks.close();
    await coreApi.close();
  }, 30000);

  it("produces one join record on join and one leave record on disconnect, both partitioned by org_id", async () => {
    const orgId = randomUUID();
    const meetingId = randomUUID();
    const userId = randomUUID();
    // Already valid UUIDs here (this test is about the Kafka contract, not about proving the
    // identity-resolution fallback — that's covered in gateway.test.ts and coreApiClient's own
    // tests), so resolve identity-mapped: what core-api "resolves" to is the same value.
    coreApi.allowParticipant(orgId, meetingId, userId, { orgId, userId });

    const token = await jwks.signToken({ orgId, userId });
    const socket = await connectClient(baseUrl, token);
    await new Promise<JoinAck>((resolve) => socket.emit("join", { meetingId }, resolve));

    await waitFor(() =>
      received.some((r) => r.event.meeting_id === meetingId && r.event.event_type === "join"),
    );
    const joinRecord = received.find(
      (r) => r.event.meeting_id === meetingId && r.event.event_type === "join",
    );
    expect(joinRecord).toBeDefined();
    expect(joinRecord!.event.header.org_id).toBe(orgId);
    expect(joinRecord!.event.user_id).toBe(userId);
    expect(joinRecord!.event.header.producer).toBe("realtime-gateway");
    expect(joinRecord!.partitionKey).toBe(orgId); // partitioned by org_id

    socket.close();

    await waitFor(() =>
      received.some((r) => r.event.meeting_id === meetingId && r.event.event_type === "leave"),
    );
    const leaveRecord = received.find(
      (r) => r.event.meeting_id === meetingId && r.event.event_type === "leave",
    );
    expect(leaveRecord).toBeDefined();
    expect(leaveRecord!.event.header.org_id).toBe(orgId);
    expect(leaveRecord!.event.user_id).toBe(userId);
    expect(leaveRecord!.partitionKey).toBe(orgId);

    // exactly one record per transition, not more
    expect(received.filter((r) => r.event.meeting_id === meetingId)).toHaveLength(2);
  }, 20000);
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, timeoutMs = 10000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitFor: condition not met within timeout");
    }
    await sleep(50);
  }
}
