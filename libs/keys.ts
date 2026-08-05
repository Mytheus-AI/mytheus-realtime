/**
 * Redis key / Socket.io room builders for the real-time tier — the single canonical location.
 * Frozen forms come from `mytheus-docs/_canon/02-data-model.md` §5; never hand-build these
 * strings inline anywhere in this repo.
 *
 * This started as `services/realtime-gateway/src/keys.ts` (Plan 0 scaffold) and moved here so
 * every service in this repo shares one implementation. `transcription-service` will need the
 * presence/pubsub key shapes too once it starts bridging captions onto the explicit channel.
 */

/**
 * Every `org_id` in this file means Mytheus's own internal tenant UUID (`organizations.id`) —
 * the same thing `org_id` means everywhere else in the system (event envelopes, Postgres rows,
 * every other producer). It is NEVER Clerk's opaque org identifier (a string like
 * `"org_2abc..."`, from the JWT's `org_id` claim). This service has no database, so it cannot
 * resolve one to the other itself; see `src/coreApiClient.ts`'s header comment for how it gets
 * a resolved internal `org_id` from `core-api`, and `src/server.ts` for where that resolved
 * value — not the raw JWT claim — is threaded into every function in this file. Passing the raw
 * Clerk claim into any of these was a real, shipped bug (routed every `presence-events` record
 * to `presence-events.dlq` as poison, since the Avro schema types `org_id` as a UUID); don't
 * reintroduce it.
 */

/**
 * Verbatim canonical Socket.io cross-instance fan-out channel for a meeting:
 * `nexus:{org_id}:pubsub:meeting:{meeting_id}` (_canon/02-data-model.md §5).
 *
 * Used two ways in this repo: the `@socket.io/redis-adapter` uses its own internal Redis
 * channels for ordinary cross-pod broadcast (`nsp.to(room).emit(...)`), so this explicit name is
 * for non-Socket.io publishers that inject events into a meeting's fan-out from outside the
 * gateway process (e.g. the transcription caption bridge landing in a later task — whatever
 * publishes there must also key by the internal `org_id`, or the two sides will never match and
 * captions will silently never arrive; there is no error to catch that, so get this right up
 * front).
 */
export function pubsubChannel(orgId: string, meetingId: string): string {
  return `nexus:${orgId}:pubsub:meeting:${meetingId}`;
}

/**
 * Verbatim canonical live presence set key: `nexus:{org_id}:presence:{meeting_id}`
 * (_canon/02-data-model.md §5). Ephemeral only — Redis is the only store, never a durable table.
 */
export function presenceKey(orgId: string, meetingId: string): string {
  return `nexus:${orgId}:presence:${meetingId}`;
}

/**
 * Socket.io room name for a meeting, keyed by `meeting_id` alone — matching the canon's literal
 * "rooms keyed by `meeting_id`" (not `org_id`+`meeting_id`). `meeting_id` is a UUID minted by
 * `core-api` and never reused across tenants, so this is already tenant-safe on its own; the
 * actual authz boundary is the deny-by-default participant check on join (`src/server.ts`), not
 * the room name. An earlier version of this function also embedded `org_id` as defense in depth,
 * but that forced every caller to agree on which `org_id` REPRESENTATION to use (Clerk's claim
 * vs the resolved internal UUID — see this file's header comment), and one caller (the
 * reconciliation sweep, which reads back whatever was stored in Redis rather than a live JWT)
 * could only ever see the resolved value. A meeting-only key sidesteps that mismatch entirely
 * without weakening the actual boundary.
 */
export function roomName(meetingId: string): string {
  return `meeting:${meetingId}`;
}

/**
 * Internal bookkeeping key: a Redis SET of `{org_id}|{meeting_id}` pairs that currently have at
 * least one live presence entry. Not a tenant-facing key and not itself frozen by canon — it
 * exists purely so the presence reconciliation sweep (see `src/presence.ts`) can find every
 * meeting with live presence without a full `SCAN` over `nexus:*:presence:*`. `_internal` can
 * never collide with a real `org_id` (those are UUIDs).
 */
export function presenceIndexKey(): string {
  return "nexus:_internal:presence-index";
}
