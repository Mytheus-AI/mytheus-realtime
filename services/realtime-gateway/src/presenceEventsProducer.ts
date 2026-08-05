/**
 * Producer for the `presence-events` topic (`PresenceEvent` Avro value, subject
 * `presence-events-value`, 12 partitions, 1h retention, key `org_id`). One record per join/leave
 * transition, consumed by `core-api` for analytics.
 *
 * No transactional outbox here (contrast `mytheus-events`' documented outbox pattern for
 * Postgres-backed producers like `core-api`): this service intentionally holds no durable store
 * at all — that is its whole design point (see `CLAUDE.md` "Ephemeral state only on real-time
 * pods"). There is no table to commit the outbox row alongside. The idempotent Kafka producer
 * (`idempotent: true`, kafkajs's built-in retry-with-backoff) is the right substitute: at least
 * once, safe to retry, and it does not require inventing a database this service should not have.
 *
 * Idempotency key: `presence-events` has no canonical key shape yet in `mytheus-events`'s
 * documented list (only 8 topics are listed there; this is not one of them). A join/leave has no
 * pre-existing domain id to key off, so a fresh id is minted once per transition and reused for
 * any retry of that same produce call: `presence:{event_id}`.
 */
import { randomUUID } from "node:crypto";

import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { Kafka, Partitioners, type Producer } from "kafkajs";

import { buildEnvelopeHeader } from "../../../libs/envelope";

export type PresenceEventType = "join" | "leave";

export interface ProducePresenceEventParams {
  orgId: string;
  meetingId: string;
  userId: string;
  eventType: PresenceEventType;
}

export interface PresenceEventsProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  produce(params: ProducePresenceEventParams): Promise<void>;
}

export interface PresenceEventsProducerOptions {
  brokers: string[];
  schemaRegistryUrl: string;
  clientId?: string;
}

const TOPIC = "presence-events";
const SUBJECT = "presence-events-value";
const PRODUCER_NAME = "realtime-gateway";

export function createPresenceEventsProducer(options: PresenceEventsProducerOptions): PresenceEventsProducer {
  const kafka = new Kafka({ clientId: options.clientId ?? "realtime-gateway", brokers: options.brokers });
  const producer: Producer = kafka.producer({
    idempotent: true,
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  const registry = new SchemaRegistry({ host: options.schemaRegistryUrl });

  let schemaId: number | undefined;
  let connected = false;

  return {
    async connect() {
      if (connected) return;
      await producer.connect();
      schemaId = await registry.getLatestSchemaId(SUBJECT);
      connected = true;
    },

    async disconnect() {
      if (!connected) return;
      await producer.disconnect();
      connected = false;
    },

    async produce({ orgId, meetingId, userId, eventType }) {
      if (!connected || schemaId === undefined) {
        throw new Error("presence-events producer used before connect()");
      }
      const eventId = randomUUID();
      const header = buildEnvelopeHeader({
        orgId,
        producer: PRODUCER_NAME,
        idempotencyKey: `presence:${eventId}`,
        eventId,
      });
      const value = await registry.encode(schemaId, {
        header,
        meeting_id: meetingId,
        user_id: userId,
        event_type: eventType,
      });
      await producer.send({
        topic: TOPIC,
        messages: [{ key: orgId, value }],
      });
    },
  };
}
