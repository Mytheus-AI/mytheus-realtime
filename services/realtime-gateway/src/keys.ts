/**
 * Redis key/channel builders for the real-time tier. Frozen names come from
 * `mytheus-docs/_canon/02-data-model.md` — never hand-build these strings inline.
 */

/**
 * Verbatim canonical Socket.io cross-instance fan-out channel for a meeting.
 * `nexus:{org_id}:pubsub:meeting:{meeting_id}` (_canon/02-data-model.md §5).
 */
export function pubsubChannel(orgId: string, meetingId: string): string {
  return `nexus:${orgId}:pubsub:meeting:${meetingId}`;
}
