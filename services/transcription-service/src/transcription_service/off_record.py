"""Off the record (polish program wave 3): while a meeting is off the record, nothing said in it
is captioned, persisted or produced.

core-api sets `nexus:{org_id}:off-record:{meeting_id}` when the host takes the meeting off the
record and deletes it when they end it (or the meeting ends). `persistence.process_utterance`,
the one place an utterance becomes a caption, a `transcript_chunks` row and a `transcript-chunks`
message, asks `OffRecordGate.active` first and drops the utterance when it is set: no interim
caption, no final caption, no row, no outbox entry. Live captions pause too, by decision: a
caption is the transcription of what was said, and "off the record" means it is not transcribed.

The flag is read at most every `CACHE_S` per meeting (an EXISTS per interim would be one Redis
round trip per partial word), so going off the record takes effect within half a second of the
host's click. A Redis error keeps the last state this process read. With no state read yet it
counts as on the record: core-api refuses to start a segment unless its own flag write succeeds,
so a Redis that cannot be read now could not have been used to go off the record either, and
dropping every transcript on a blip would lose a meeting nobody paused.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

CACHE_S = 0.5


def off_record_key(org_id: str, meeting_id: str) -> str:
    """Verbatim `nexus:{org_id}:off-record:{meeting_id}` (core-api `routes/off_record.py`)."""
    return f"nexus:{org_id}:off-record:{meeting_id}"


class OffRecordGate:
    def __init__(self, *, cache_s: float = CACHE_S, clock: Callable[[], float] = time.monotonic):
        self._cache_s = cache_s
        self._clock = clock
        self._state: dict[tuple[str, str], tuple[float, bool]] = {}

    async def active(self, redis: Any, org_id: str, meeting_id: str) -> bool:
        key = (org_id, meeting_id)
        now = self._clock()
        hit = self._state.get(key)
        if hit is not None and now < hit[0]:
            return hit[1]
        try:
            value = bool(await redis.exists(off_record_key(org_id, meeting_id)))
        except Exception:  # noqa: BLE001 - see the module docstring
            last = hit[1] if hit is not None else False
            logger.warning(
                "off-record flag unreadable for meeting %s (org %s); keeping %s",
                meeting_id,
                org_id,
                "off the record" if last else "on the record",
            )
            return last
        if len(self._state) > 10_000:
            self._state.clear()  # a bound, not an LRU: meetings come and go
        self._state[key] = (now + self._cache_s, value)
        return value

    def forget(self, org_id: str, meeting_id: str) -> None:
        self._state.pop((org_id, meeting_id), None)


GATE = OffRecordGate()
