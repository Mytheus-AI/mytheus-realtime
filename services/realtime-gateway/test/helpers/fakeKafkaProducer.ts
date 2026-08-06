/**
 * A fake `MinimalKafkaProducer` (see src/presenceEventsProducer.ts) that records every `send()`
 * call and can be configured to reject sends to a specific topic — used to deterministically
 * exercise the DLQ-routing path without needing a genuinely broken broker (which would require
 * disrupting the shared local Redpanda instance other tests also depend on).
 */
import type { MinimalKafkaProducer, MinimalSchemaRegistry } from "../../src/presenceEventsProducer";

export interface RecordedSend {
  topic: string;
  key: string | undefined;
  value: string; // decoded to utf8 for easy assertions; DLQ messages are plain JSON strings
}

export interface FakeKafkaProducer extends MinimalKafkaProducer {
  sends: RecordedSend[];
  /** After this many calls to send() for `failTopic`, start rejecting instead of recording. */
  failTopic?: string;
}

export function createFakeKafkaProducer(options: { failTopic?: string } = {}): FakeKafkaProducer {
  const sends: RecordedSend[] = [];
  return {
    sends,
    failTopic: options.failTopic,
    async connect() {},
    async disconnect() {},
    async send(record) {
      if (record.topic === options.failTopic) {
        throw new Error(`simulated broker failure producing to ${record.topic}`);
      }
      for (const message of record.messages) {
        sends.push({
          topic: record.topic,
          key: message.key,
          value: typeof message.value === "string" ? message.value : message.value.toString("utf8"),
        });
      }
      return undefined;
    },
  };
}

/** A `MinimalSchemaRegistry` that just JSON-stringifies the payload instead of doing real Avro
 * encoding — sufficient for tests that only care about produce-failure/DLQ routing, not the wire
 * format (that's covered for real in test/presence-events-kafka.test.ts). */
export function createFakeSchemaRegistry(): MinimalSchemaRegistry {
  return {
    async getLatestSchemaId() {
      return 1;
    },
    async encode(_schemaId, payload) {
      return Buffer.from(JSON.stringify(payload), "utf8");
    },
  };
}
