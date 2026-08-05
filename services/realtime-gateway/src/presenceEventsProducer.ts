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
 * documented list (only 8 topics are listed there; this is not one of them). A fresh id is minted
 * once per transition and reused for any retry of that same produce call: `presence:{event_id}`.
 * (This dedupes kafkajs's own internal retry of a single `send()`, not an application-level
 * double-emit of the same transition — fine for an analytics stream, but worth being honest about
 * in case this key is ever reused for something that needs stronger guarantees.)
 *
 * DLQ on produce failure: `producer.send()` already retries internally with exponential backoff
 * (a finite, explicit budget — see `PRODUCE_RETRIES`'s doc comment for why kafkajs's own default
 * for an idempotent producer, unlimited retries, is deliberately NOT used here), so by the time a
 * call to `produce()` here actually rejects, that retry budget is exhausted — this is exactly the
 * delivery contract's "MAX_RETRIES" poison case (see `mytheus-events` CLAUDE.md §"The delivery
 * contract"), and `presence-events.dlq` already exists for it. Routing the failure there
 * (as a `DlqEnvelope`, `libs/envelope.ts`) means a brief broker or Schema Registry outage leaves
 * an inspectable, replayable record instead of vanishing after a single log line. `produce()`
 * still rethrows the original error afterward so existing callers keep their own logging/handling
 * unchanged — the DLQ write is additive, not a replacement for that.
 *
 * UUID validation before producing: `header.org_id` and `payload.user_id` are typed as the Avro
 * `uuid` logical type, and `core-api`'s consumer re-asserts that at read time and DLQs anything
 * that isn't (`presence-events` previously shipped with raw Clerk ids in both fields — an opaque
 * string like `"org_2abc..."`, never a UUID — which is exactly what made every single message
 * poison; see `coreApiClient.ts`'s header comment for the full story and `server.ts` for where
 * the resolved-or-not-yet-resolved identity is threaded in). `produce()` validates both ids are
 * UUID-shaped and, if not, skips producing entirely (logs a warning, resolves normally, never
 * throws) instead of ever emitting a record the consumer's own contract test already proves will
 * be poisoned. This is the single enforcement point for every caller (join, disconnect, and the
 * presence-reconciliation sweep all call this same `produce()`), so none of them need their own
 * copy of this check.
 */
import { randomUUID } from "node:crypto";

import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { Kafka, Partitioners } from "kafkajs";

import { buildDlqEnvelope, buildEnvelopeHeader } from "../../../libs/envelope";

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

/** The subset of kafkajs's `Producer` this module actually calls — small enough for tests to
 * implement a fake directly, without pulling in a real Kafka client. */
export interface MinimalKafkaProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(record: { topic: string; messages: Array<{ key?: string; value: Buffer | string }> }): Promise<unknown>;
}

/** The subset of `@kafkajs/confluent-schema-registry`'s `SchemaRegistry` this module calls. */
export interface MinimalSchemaRegistry {
  getLatestSchemaId(subject: string): Promise<number>;
  encode(schemaId: number, payload: unknown): Promise<Buffer>;
}

export interface PresenceEventsProducerOptions {
  brokers: string[];
  schemaRegistryUrl: string;
  clientId?: string;
  /** Injectable for tests (see test/helpers/fakeKafkaProducer.ts); production always builds a
   * real kafkajs `Producer` from `brokers`/`clientId`. */
  producer?: MinimalKafkaProducer;
  /** Injectable for tests; production always builds a real `SchemaRegistry` from
   * `schemaRegistryUrl`. */
  registry?: MinimalSchemaRegistry;
  logger?: { error: (obj: unknown, msg?: string) => void; warn?: (obj: unknown, msg?: string) => void };
}

/** RFC 4122-shaped, version-agnostic (accepts anything Postgres/`crypto.randomUUID()` would
 * produce). Deliberately permissive on version/variant bits — this is a wire-format guard against
 * "not a UUID at all" (e.g. a raw Clerk id like `"org_2abc..."`), not a strict UUID validator. */
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function isUuid(value: string): boolean {
  return UUID_RE.test(value);
}

const TOPIC = "presence-events";
const DLQ_TOPIC = "presence-events.dlq";
const SUBJECT = "presence-events-value";
const PRODUCER_NAME = "realtime-gateway";
/**
 * kafkajs's actual default for an `idempotent: true` producer is unlimited retries
 * (`Number.MAX_SAFE_INTEGER`) — logged here explicitly (not left implicit) because an unlimited
 * retry budget would mean `produce()` never rejects during a real outage, which would mean DLQ
 * routing below never triggers either. Bounding it to a finite, generous count is a deliberate
 * trade-off: slightly weaker exactly-once-in-effect guarantees (kafkajs warns about this on
 * `producer.connect()` — expected, not a bug) in exchange for a produce failure actually
 * surfacing, in bounded time, to the DLQ instead of retrying forever against a broker that may
 * be down for a while. Also doubles as the truthful `attempts` value on a `MAX_RETRIES`
 * DlqEnvelope.
 */
const PRODUCE_RETRIES = 5;

export function createPresenceEventsProducer(options: PresenceEventsProducerOptions): PresenceEventsProducer {
  const kafka = new Kafka({ clientId: options.clientId ?? "realtime-gateway", brokers: options.brokers });
  const producer: MinimalKafkaProducer =
    options.producer ??
    kafka.producer({
      idempotent: true,
      createPartitioner: Partitioners.DefaultPartitioner,
      retry: { retries: PRODUCE_RETRIES },
    });
  const registry: MinimalSchemaRegistry = options.registry ?? new SchemaRegistry({ host: options.schemaRegistryUrl });

  let schemaId: number | undefined;
  let connected = false;

  async function routeToDlq(orgId: string, idempotencyKey: string, rawValueJson: unknown, err: unknown): Promise<void> {
    const dlqEnvelope = buildDlqEnvelope({
      originalTopic: TOPIC,
      orgId,
      idempotencyKey,
      consumerGroup: `producer:${PRODUCER_NAME}`,
      failureClass: "MAX_RETRIES",
      error: err instanceof Error ? err.message : String(err),
      attempts: PRODUCE_RETRIES,
      rawValueJson,
    });
    try {
      await producer.send({
        topic: DLQ_TOPIC,
        messages: [{ key: orgId, value: JSON.stringify(dlqEnvelope) }],
      });
    } catch (dlqErr) {
      // Last resort: both the primary produce and the DLQ write failed (e.g. the broker itself is
      // fully unreachable, not just this topic/schema). Nothing more this producer can do; the
      // original error is still rethrown by produce() below so the caller's own logging fires.
      options.logger?.error(
        { err: dlqErr, orgId, idempotencyKey },
        "presence-events DLQ write also failed; record is unrecoverable beyond this log line",
      );
    }
  }

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
      // Both fields are the Avro `uuid` logical type and the consumer re-asserts that at read
      // time (poisoning anything else) — see this file's header comment. Skip rather than ever
      // emit a record we already know is malformed; this is a deliberate no-op, not a failure, so
      // it does not throw and does not go through DLQ routing (there was nothing to retry).
      if (!isUuid(orgId) || !isUuid(userId)) {
        options.logger?.warn?.(
          { orgId, meetingId, userId, eventType },
          "skipping presence-events produce: org_id or user_id is not a resolved UUID yet (see coreApiClient.ts's CONTRACT GAP note)",
        );
        return;
      }
      const eventId = randomUUID();
      const idempotencyKey = `presence:${eventId}`;
      const header = buildEnvelopeHeader({
        orgId,
        producer: PRODUCER_NAME,
        idempotencyKey,
        eventId,
      });
      const payload = { header, meeting_id: meetingId, user_id: userId, event_type: eventType };

      try {
        const value = await registry.encode(schemaId, payload);
        await producer.send({ topic: TOPIC, messages: [{ key: orgId, value }] });
      } catch (err) {
        await routeToDlq(orgId, idempotencyKey, payload, err);
        throw err;
      }
    },
  };
}
