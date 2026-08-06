/**
 * Shared event envelope for the real-time tier: `EventEnvelopeHeader`, mirrored field-for-field
 * from `mytheus-events/schemas/_envelope/EventEnvelopeHeader.avsc` (frozen field order, per
 * `07-event-architecture.md` §3.1).
 *
 * Why a hand-kept mirror instead of importing the generated TypeScript client
 * (`mytheus-events/clients/typescript`): that package has no `package.json` — it is `private:
 * true` build output of `mytheus-events`' own `codegen`, never published, and this repo has no
 * workspace link into `mytheus-events` (they are separate git repos). A relative cross-repo file
 * import would only work on a machine that happens to have both repos checked out next to each
 * other at exactly this relative path, which does not hold in CI or a fresh clone. So: mirror the
 * schema here, exactly, rather than invent a payload shape or take on a fragile path dependency.
 * If the envelope schema changes, this file changes with it.
 */
import { randomUUID } from "node:crypto";

export interface EventEnvelopeHeader {
  event_id: string;
  idempotency_key: string;
  org_id: string;
  schema_version: number;
  producer: string;
  /** Microseconds since the Unix epoch (UTC) — matches the Avro `timestamp-micros` logical type. */
  occurred_at: number;
  correlation_id: string | null;
  causation_id: string | null;
}

export interface BuildEnvelopeHeaderInput {
  orgId: string;
  /** Owning service name, kebab-case, from the Service Catalog (e.g. "realtime-gateway"). */
  producer: string;
  /** Business dedupe key; stable across producer retries of the same fact. */
  idempotencyKey: string;
  schemaVersion?: number;
  correlationId?: string | null;
  causationId?: string | null;
  /** Override for tests; defaults to a fresh uuid. */
  eventId?: string;
  /** Override for tests; defaults to now, in microseconds. */
  occurredAtMicros?: number;
}

export function buildEnvelopeHeader(input: BuildEnvelopeHeaderInput): EventEnvelopeHeader {
  return {
    event_id: input.eventId ?? randomUUID(),
    idempotency_key: input.idempotencyKey,
    org_id: input.orgId,
    schema_version: input.schemaVersion ?? 1,
    producer: input.producer,
    occurred_at: input.occurredAtMicros ?? Date.now() * 1000,
    correlation_id: input.correlationId ?? null,
    causation_id: input.causationId ?? null,
  };
}

/**
 * Dead-letter wrapper for poison messages, mirrored field-for-field from
 * `mytheus-events/contracts/DlqEnvelope.avsc` (same cross-repo-import rationale as
 * `EventEnvelopeHeader` above). One shared schema, paired with every topic's `<topic>.dlq`.
 */
export interface DlqEnvelope {
  original_topic: string;
  original_partition: number;
  original_offset: number;
  org_id: string;
  idempotency_key: string;
  consumer_group: string;
  failure_class: "DESERIALIZE" | "SCHEMA_INCOMPAT" | "BUSINESS_INVALID" | "MAX_RETRIES";
  error: string;
  attempts: number;
  /** Microseconds since the Unix epoch (UTC) — matches the Avro `timestamp-micros` logical type. */
  first_seen_at: number;
  raw_value_b64: string;
}

export interface BuildDlqEnvelopeInput {
  originalTopic: string;
  orgId: string;
  idempotencyKey: string;
  /**
   * `DlqEnvelope.consumer_group` is defined for the consumer-side poison-message case (see
   * `mytheus-events` CLAUDE.md §"The delivery contract"). `mytheus-events` pairs exactly one
   * shared `DlqEnvelope` schema with every topic's `.dlq`, and adding a producer-specific DLQ
   * schema would be a canon change this task cannot make. A producer-side DLQ write (this
   * function's only caller today, for `presence-events` produce failures) therefore reuses this
   * field to name the failing producer component instead of a consumer group.
   */
  consumerGroup: string;
  failureClass: "DESERIALIZE" | "SCHEMA_INCOMPAT" | "BUSINESS_INVALID" | "MAX_RETRIES";
  error: string;
  attempts: number;
  /** The value that failed to produce, as a JSON-serializable object (pre-Avro-encode) — captured
   * this way, not as encoded bytes, because Avro encoding itself may be what failed. */
  rawValueJson: unknown;
  /** Override for tests; defaults to now, in microseconds. */
  firstSeenAtMicros?: number;
}

export function buildDlqEnvelope(input: BuildDlqEnvelopeInput): DlqEnvelope {
  return {
    original_topic: input.originalTopic,
    // Sentinels, not real values: a producer-side failure means the record was never actually
    // written, so it has no real partition/offset — those only exist once a broker assigns them.
    original_partition: -1,
    original_offset: -1,
    org_id: input.orgId,
    idempotency_key: input.idempotencyKey,
    consumer_group: input.consumerGroup,
    failure_class: input.failureClass,
    error: input.error,
    attempts: input.attempts,
    first_seen_at: input.firstSeenAtMicros ?? Date.now() * 1000,
    raw_value_b64: Buffer.from(JSON.stringify(input.rawValueJson), "utf8").toString("base64"),
  };
}
