/**
 * Finding 2 (fix round 1): a `presence-events` produce failure must not vanish with only a log
 * line — it routes to `presence-events.dlq`, which already exists on the broker for exactly this.
 * Uses a fake `MinimalKafkaProducer` (test/helpers/fakeKafkaProducer.ts) to make the main-topic
 * send fail deterministically, rather than needing a genuinely broken broker.
 */
import { describe, expect, it } from "vitest";

import { createPresenceEventsProducer } from "../src/presenceEventsProducer";
import { createFakeKafkaProducer, createFakeSchemaRegistry } from "./helpers/fakeKafkaProducer";

describe("presenceEventsProducer DLQ routing", () => {
  it("routes a produce failure to presence-events.dlq as a DlqEnvelope, and still rethrows the original error", async () => {
    const fakeProducer = createFakeKafkaProducer({ failTopic: "presence-events" });
    const producer = createPresenceEventsProducer({
      brokers: ["unused:9092"],
      schemaRegistryUrl: "http://unused",
      producer: fakeProducer,
      registry: createFakeSchemaRegistry(),
    });
    await producer.connect();

    const orgId = "org-1";
    await expect(
      producer.produce({ orgId, meetingId: "meeting-1", userId: "user-1", eventType: "join" }),
    ).rejects.toThrow(/simulated broker failure/);

    // nothing landed on the real topic (the fake rejected it before recording)
    expect(fakeProducer.sends.filter((s) => s.topic === "presence-events")).toHaveLength(0);

    // exactly one DLQ record landed, keyed by org_id like every other topic
    const dlqSends = fakeProducer.sends.filter((s) => s.topic === "presence-events.dlq");
    expect(dlqSends).toHaveLength(1);
    expect(dlqSends[0].key).toBe(orgId);

    const envelope = JSON.parse(dlqSends[0].value);
    expect(envelope).toMatchObject({
      original_topic: "presence-events",
      org_id: orgId,
      failure_class: "MAX_RETRIES",
      attempts: 5,
    });
    expect(envelope.idempotency_key).toMatch(/^presence:/);
    expect(envelope.error).toMatch(/simulated broker failure/);
    expect(typeof envelope.first_seen_at).toBe("number");

    // the original (pre-encode) payload is recoverable from raw_value_b64 for inspection/replay
    const rawPayload = JSON.parse(Buffer.from(envelope.raw_value_b64, "base64").toString("utf8"));
    expect(rawPayload).toMatchObject({
      meeting_id: "meeting-1",
      user_id: "user-1",
      event_type: "join",
      header: { org_id: orgId },
    });
  });

  it("does not touch the DLQ when produce succeeds", async () => {
    const fakeProducer = createFakeKafkaProducer();
    const producer = createPresenceEventsProducer({
      brokers: ["unused:9092"],
      schemaRegistryUrl: "http://unused",
      producer: fakeProducer,
      registry: createFakeSchemaRegistry(),
    });
    await producer.connect();

    await producer.produce({ orgId: "org-1", meetingId: "meeting-1", userId: "user-1", eventType: "join" });

    expect(fakeProducer.sends.filter((s) => s.topic === "presence-events")).toHaveLength(1);
    expect(fakeProducer.sends.filter((s) => s.topic === "presence-events.dlq")).toHaveLength(0);
  });

  it("logs but does not throw an unhandled error when the DLQ write itself also fails", async () => {
    // failTopic can only name one topic; simulate "everything fails" by failing the main topic
    // and wrapping a producer whose send() always throws to prove the last-resort log path runs
    // without producer.produce() itself throwing something other than the original error.
    const alwaysFailingProducer = createFakeKafkaProducer({ failTopic: "presence-events" });
    const originalSend = alwaysFailingProducer.send.bind(alwaysFailingProducer);
    alwaysFailingProducer.send = async (record) => {
      if (record.topic === "presence-events.dlq") {
        throw new Error("broker fully unreachable");
      }
      return originalSend(record);
    };

    const errors: unknown[] = [];
    const producer = createPresenceEventsProducer({
      brokers: ["unused:9092"],
      schemaRegistryUrl: "http://unused",
      producer: alwaysFailingProducer,
      registry: createFakeSchemaRegistry(),
      logger: { error: (obj) => errors.push(obj) },
    });
    await producer.connect();

    await expect(
      producer.produce({ orgId: "org-1", meetingId: "meeting-1", userId: "user-1", eventType: "join" }),
    ).rejects.toThrow(/simulated broker failure/); // the ORIGINAL error, not the DLQ error

    expect(errors.length).toBeGreaterThan(0);
  });
});
