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
 * Verbatim canonical Socket.io cross-instance fan-out channel for a meeting:
 * `nexus:{org_id}:pubsub:meeting:{meeting_id}` (_canon/02-data-model.md §5).
 *
 * Used two ways in this repo: the `@socket.io/redis-adapter` uses its own internal Redis
 * channels for ordinary cross-pod broadcast (`nsp.to(room).emit(...)`), so this explicit name is
 * for non-Socket.io publishers that inject events into a meeting's fan-out from outside the
 * gateway process (e.g. the transcription caption bridge landing in a later task).
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
 * Socket.io room name for a meeting. Not itself a frozen `nexus:` key — the canon only requires
 * "rooms keyed by `meeting_id`" — but this embeds `org_id` too as defense in depth: room
 * membership is granted only after the server-side authz check (a client can never `socket.join`
 * directly), so this scoping can't be bypassed, but keeping `org_id` in the room name means a
 * future bug that ever computed the wrong room still cannot collide across tenants.
 */
export function roomName(orgId: string, meetingId: string): string {
  return `meeting:${orgId}:${meetingId}`;
}
