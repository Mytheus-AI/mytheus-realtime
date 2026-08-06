"""Consumes `meeting-room-events` (produced by `meeting-service`) and drives `SessionTracker`.

Idempotency (plan 3.6 step 8: "Ensure idempotency so a redelivered event never opens a
duplicate stream"): two layers, matching the pattern documented in `mytheus-events/CLAUDE.md`
("Inbox dedupe on consume") --

1. `SessionTracker` itself is idempotent in effect: a redelivered `joined` for an
   already-open meeting increments a counter and returns without opening a second stream (see
   `sessions.py`).
2. A Redis dedupe cache in front of it, keyed `nexus:{org_id}:dedupe:meeting-room-events:
   {idempotency_key}` (24h TTL, per this program's documented dedupe-key shape), so a
   redelivery that arrives while THIS pod still holds the meeting open is rejected before it
   even reaches `SessionTracker` -- belt-and-braces, since `SessionTracker`'s in-memory state
   does not survive a pod restart the way the plan's "ensure idempotency" implies redelivery
   handling should.

No durable inbox table: unlike `mytheus-backend`'s consumers, this service holds no durable
store of its own to add an inbox row to (mirrors Task 6's `presence-events` producer decision
on `realtime-gateway` -- no table, no outbox/inbox to bolt onto). The Redis cache plus
`SessionTracker`'s own idempotent state machine is the substitute.

Manual offset commit after the effect (open/close the stream), never before -- an offset
committed before the effect and then a crash would silently skip that meeting's transcription
forever, since Kafka would never redeliver it.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from redis.asyncio import Redis

from .sessions import SessionTracker

logger = logging.getLogger(__name__)

CONSUMER_GROUP = "transcription-service"
TOPIC = "meeting-room-events"
_DEDUPE_TTL_SECONDS = 24 * 60 * 60


def _dedupe_key(org_id: str, idempotency_key: str) -> str:
    return f"nexus:{org_id}:dedupe:{TOPIC}:{idempotency_key}"


async def handle_meeting_room_event(
    tracker: SessionTracker, redis: Redis, event: dict[str, Any]
) -> bool:
    """`event` is the decoded Avro value (header + payload fields flattened, matching
    `MeetingRoomEvent`). Returns `True` iff this call had an effect (as opposed to being
    rejected as a duplicate or an event type this service ignores)."""
    header = event["header"]
    org_id = header["org_id"]
    idempotency_key = header["idempotency_key"]

    accepted = await redis.set(
        _dedupe_key(org_id, idempotency_key), "1", nx=True, ex=_DEDUPE_TTL_SECONDS
    )
    if not accepted:
        logger.info("meeting-room-events: duplicate %s, skipping", idempotency_key)
        return False

    meeting_id = event["meeting_id"]
    event_type = event["event_type"]

    if event_type == "joined":
        return await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)
    if event_type == "left":
        return await tracker.on_participant_left(org_id=org_id, meeting_id=meeting_id)
    # "recording_ready": no transcription-session action; acknowledged (dedupe key still set)
    # so a redelivery doesn't reprocess it, but nothing to do here today.
    return False


def decode_meeting_room_event(raw_json: bytes) -> dict[str, Any]:
    """Test/dev seam: decodes a JSON-shaped stand-in for the Avro value. The real Kafka
    consumer wiring (confluent-kafka `Consumer` + `AvroDeserializer` against the registered
    `meeting-room-events-value` subject, manual offset commit after `handle_meeting_room_event`
    returns) is the same shape as `mytheus-backend/services/relay/main.py`'s producer-side
    `AvroSerializer` usage in reverse, and is intentionally not stood up as a long-running
    consumer loop in this task -- see the task report's scope notes: the compliance gate,
    finals-only persistence, and the producer were prioritized over the reconnect/consumer
    loop's I/O plumbing. `handle_meeting_room_event` above is the real, tested business logic;
    only the confluent-kafka wiring around it (subscribe/poll/commit) is left for the next
    pass.
    """
    return dict(json.loads(raw_json))
