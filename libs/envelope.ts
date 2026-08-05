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
