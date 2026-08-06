"""Per-meeting Deepgram-session lifecycle (plan 3.6 step 8): open a stream on the first
`participant.joined` for a meeting, close it when the last participant leaves. "One diarized
Deepgram session per meeting_id" (plan 3.6 step 4) -- never per participant, which is the
locked 6x cost lever (D1 = 216M min/mo).

Idempotency: `meeting-room-events` is at-least-once, so a redelivered `joined` for a meeting
whose session is already open must NOT open a second stream, and a redelivered `left` must not
double-decrement the participant count below zero. `SessionTracker` is the pure, synchronous
state machine that both the real Kafka consumer (`meeting_room_consumer.py`) and tests drive
directly -- kept separate from any I/O so redelivery semantics are testable without a broker.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .audit import record_session_decision
from .db import set_current_org
from .persistence import process_utterance
from .providers.base import TranscriptionProvider, TranscriptionStream
from .tenancy import get_data_residency_region, get_pii_redaction_enabled

logger = logging.getLogger(__name__)


@dataclass
class _MeetingSession:
    participant_count: int
    stream: TranscriptionStream
    pump_task: asyncio.Task[None]


@dataclass
class SessionTracker:
    """Keyed by `meeting_id` only (not `(org_id, meeting_id)`): `meeting_id` is already
    globally unique (UUID) and the provider/session boundary has no cross-tenant fan-out risk
    the way a Socket.io room name does -- see Task 6's `roomName` decision for the contrasting
    case where defense-in-depth on the key mattered."""

    provider: TranscriptionProvider
    session_factory: async_sessionmaker[AsyncSession]
    redis: Redis | None
    _sessions: dict[str, _MeetingSession] = field(default_factory=dict)

    async def on_participant_joined(self, *, org_id: str, meeting_id: str) -> bool:
        """Returns `True` iff this call opened a new stream (idempotency-observable for
        tests); `False` if a session for this meeting was already open (redelivery, or a
        second/third participant joining a meeting already being transcribed)."""
        existing = self._sessions.get(meeting_id)
        if existing is not None:
            existing.participant_count += 1
            return False

        async with self.session_factory() as session:
            async with session.begin():
                await set_current_org(session, org_id)
                region = await get_data_residency_region(session, org_id)
                pii_redaction_enabled = await get_pii_redaction_enabled(session, org_id)
                # Compliance gate acceptance check: "an audit-log entry records the
                # redaction-toggle and any cross-border decision" — same transaction as the
                # reads above, so the audit entry and the decision it records can never drift.
                await record_session_decision(
                    session,
                    org_id=org_id,
                    meeting_id=meeting_id,
                    region=region,
                    pii_redaction_enabled=pii_redaction_enabled,
                )

        stream = await self.provider.open_stream(
            org_id=org_id, meeting_id=meeting_id, region=region
        )
        task = asyncio.create_task(
            self._pump(
                stream,
                org_id=org_id,
                meeting_id=meeting_id,
                pii_redaction_enabled=pii_redaction_enabled,
            )
        )
        self._sessions[meeting_id] = _MeetingSession(
            participant_count=1, stream=stream, pump_task=task
        )
        return True

    async def on_participant_left(self, *, org_id: str, meeting_id: str) -> bool:
        """Returns `True` iff this call closed the stream (last leave); `False` if the
        meeting has no tracked session (redelivery of `left` after close, or `left` for a
        meeting this pod never saw `joined` for -- both must be safe no-ops, never a crash)."""
        existing = self._sessions.get(meeting_id)
        if existing is None:
            return False
        existing.participant_count -= 1
        if existing.participant_count > 0:
            return False
        del self._sessions[meeting_id]
        await existing.stream.close()
        existing.pump_task.cancel()
        return True

    async def _pump(
        self,
        stream: TranscriptionStream,
        *,
        org_id: str,
        meeting_id: str,
        pii_redaction_enabled: bool,
    ) -> None:
        """Drains `stream.utterances()` into `persistence.process_utterance` for the life of
        the stream. Errors are logged, never allowed to crash the event loop silently -- a
        stalled pump would look like a live meeting producing no captions and no chunks."""
        try:
            async for utterance in stream.utterances():
                await process_utterance(
                    session_factory=self.session_factory,
                    redis=self.redis,
                    org_id=org_id,
                    meeting_id=meeting_id,
                    utterance=utterance,
                    pii_redaction_enabled=pii_redaction_enabled,
                )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a pump failure must not crash the process
            logger.exception(
                "transcription pump failed for meeting %s (org %s)", meeting_id, org_id
            )
