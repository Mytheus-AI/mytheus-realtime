"""Publishes onto the explicit cross-service fan-out channel `realtime-gateway` bridges to
Socket.io clients, per Task 6's report (`libs/keys.ts` `pubsubChannel`,
`mytheus-realtime/services/realtime-gateway/src/server.ts`'s caption subscriber): the gateway
subscribes to `nexus:{org_id}:pubsub:meeting:{meeting_id}` and re-emits any
`{"type":"caption",...}` message it sees there as a Socket.io `caption` event with payload
`{ meetingId, payload }`, where `payload` is forwarded verbatim/opaque. This is the "also
emit as a caption" half of both the interim and the final path (plan 3.6 step 5).

Interims and finals both publish here; only finals additionally persist (`persistence.py`).
Best-effort: a Redis outage must not block persistence of a final (captions are a live UX
optimization per this repo's `CLAUDE.md`, not a system of record), so publish failures are
logged and swallowed, never raised.
"""

from __future__ import annotations

import json
import logging

from redis.asyncio import Redis

from .providers.base import Utterance

logger = logging.getLogger(__name__)


def pubsub_channel(org_id: str, meeting_id: str) -> str:
    """Verbatim canonical channel name, mirrored from `libs/keys.ts` `pubsubChannel` (Task 6)."""
    return f"nexus:{org_id}:pubsub:meeting:{meeting_id}"


async def publish_caption(
    redis: Redis,
    *,
    org_id: str,
    meeting_id: str,
    utterance: Utterance,
    text_override: str | None = None,
) -> None:
    message = {
        "type": "caption",
        "meetingId": meeting_id,
        "payload": {
            "chunkId": utterance.chunk_id,
            "speakerId": utterance.speaker_id,
            "startMs": utterance.start_ms,
            "endMs": utterance.end_ms,
            "text": text_override if text_override is not None else utterance.text,
            "isFinal": utterance.is_final,
            "confidence": utterance.confidence,
        },
    }
    try:
        await redis.publish(pubsub_channel(org_id, meeting_id), json.dumps(message))
    except Exception:  # noqa: BLE001 - best-effort; captions are never a system of record
        logger.warning(
            "caption publish failed for meeting %s (org %s); continuing without blocking "
            "persistence",
            meeting_id,
            org_id,
            exc_info=True,
        )
