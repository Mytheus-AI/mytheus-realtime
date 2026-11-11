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
 * A person's own fan-out channel: `nexus:{org_id}:pubsub:user:{user_id}` ("In every meeting at
 * once"). core-api publishes to it when the person's agent interrupts them about a live meeting
 * they are not in (`agent-interrupt`) and when a host shares or sets aside their view
 * (`view-status`). Every gateway pod holding a socket of that person that asked to `listen` is
 * subscribed; the channel, not the message body, decides who gets it. Internal UUIDs only.
 */
export function userChannel(orgId: string, userId: string): string {
  return `nexus:${orgId}:pubsub:user:${userId}`;
}

/**
 * Verbatim canonical live presence set key: `nexus:{org_id}:presence:{meeting_id}`
 * (_canon/02-data-model.md §5). Ephemeral only — Redis is the only store, never a durable table.
 */
export function presenceKey(orgId: string, meetingId: string): string {
  return `nexus:${orgId}:presence:${meetingId}`;
}

/**
 * Verbatim canonical per-user agent priority queue: `nexus:{org_id}:queue:agent:{user_id}`
 * (_canon/02-data-model.md section 5; plan 10-13 section 3.7 step 3). A Redis sorted set of the
 * user's undelivered ProactiveAgent alerts, scored so ZPOPMIN yields the most urgent (priority 1,
 * red) first and, within a priority, the oldest first. Ephemeral: the durable copy of every alert
 * is notification-service's `notification_log`.
 */
export function agentQueueKey(orgId: string, userId: string): string {
  return `nexus:${orgId}:queue:agent:${userId}`;
}

/**
 * Socket.io room name for a meeting, keyed by BOTH `org_id` and `meeting_id`.
 *
 * THIS IS A LOAD-BEARING TENANCY BOUNDARY, not defense in depth: `nsp.to(roomName(...))` is how
 * `@socket.io/redis-adapter` fans presence and chat out across every gateway pod, and that
 * fan-out reaches every socket in the named room with no further per-message check. If the room
 * were keyed by `meeting_id` alone, two sockets from DIFFERENT orgs whose meetings happened to
 * share a literal `meeting_id` would land in the identical room and receive each other's
 * presence member lists (internal user UUIDs) and chat content — a real, shipped cross-tenant
 * leak this repo hit once already for captions (a different delivery path, fixed by scoping to
 * the org-embedded pubsub channel instead of room membership) and again for presence/chat (this
 * fix). `meeting_id` being a `core-api`-minted UUID that is never reused across tenants makes a
 * collision astronomically unlikely, but "unlikely" is not "structurally impossible," and the
 * join-time participant check alone does not protect against it either: it verifies the JOINING
 * socket's own org owns a meeting with that id, not that no OTHER org does. Org-scoping the room
 * itself is what makes the leak structurally impossible rather than dependent on `meeting_id`
 * uniqueness holding.
 *
 * A PRIOR version of this function was meeting-only, on the reasoning that org-scoping "forced
 * every caller to agree on which `org_id` REPRESENTATION to use (Clerk's claim vs the resolved
 * internal UUID), and one caller (the reconciliation sweep, which reads back whatever was stored
 * in Redis rather than a live JWT) could only ever see the resolved value" — i.e. join-time and
 * sweep-time traffic risked landing in DIFFERENT rooms for the same real meeting. That risk is
 * gone: every caller in `src/server.ts` (join, chat, disconnect cleanup, and the sweep) now
 * threads through the exact same single value (`SocketData.presenceOrgId`, resolved once at
 * join and reused for everything for that socket's lifetime, including what gets written to the
 * presence index the sweep reads back from) — there is no longer a second representation for
 * this function's callers to disagree about. Restoring org-scoping here closes the cross-tenant
 * gap without reintroducing the mismatch this function used to guard against.
 */
export function roomName(orgId: string, meetingId: string): string {
  return `meeting:${orgId}:${meetingId}`;
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

/**
 * Who is editing which candidate card in a meeting right now (the shared candidate ledger's edit
 * lock): one Redis hash per meeting, field = candidate id, value = JSON `{userId, socketId,
 * until}`. Ephemeral by design (a lock expires on its own; the hash itself expires after a quiet
 * six hours), so pod loss loses nothing durable.
 */
export function candidateLocksKey(orgId: string, meetingId: string): string {
  return `nexus:${orgId}:candidate-locks:${meetingId}`;
}

/**
 * Raised hands in a meeting: one Redis hash per meeting, field = internal user id, value = JSON
 * `{socketId, at}`. Ephemeral like presence; read on join so a late joiner sees who is waiting.
 */
export function raisedHandsKey(orgId: string, meetingId: string): string {
  return `nexus:${orgId}:hands:${meetingId}`;
}

/**
 * Off the record (polish wave 3): set by core-api while a meeting is off the record (JSON
 * `{segment_id, started_at, started_by}`), deleted when it ends. transcription-service, the
 * agent runtime and the indexers enforce it; the gateway reads it on join so a socket that
 * arrives mid-segment sees "Off the record" at once.
 */
export function offRecordKey(orgId: string, meetingId: string): string {
  return `nexus:${orgId}:off-record:${meetingId}`;
}

/**
 * The caption languages viewers of a meeting chose (live translation, polish wave 3): one
 * Redis hash per meeting, field = socket id, value = an ISO 639-1 code. A hash rather than a
 * plain set so two viewers of one language count separately and one leaving does not stop the
 * other's translation; the agent runtime reads the distinct values (HVALS). Ephemeral: a socket's
 * field goes with its disconnect, and the hash expires after six quiet hours (the backstop for a
 * pod that died without running its disconnect handler).
 */
export function captionLangsKey(orgId: string, meetingId: string): string {
  return `nexus:${orgId}:caption-langs:${meetingId}`;
}
