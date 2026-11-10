"""Meeting sessions and the per-participant transcription streams inside them.

Two levels, deliberately separate:

1. The MEETING session (`_MeetingSession`, keyed by `meeting_id`): meeting-level state only --
   the `org_id` it belongs to (cross-org refusal), the tenant's `data_residency.region` and
   `pii_redaction_enabled` (read once, under RLS, with the compliance audit entry written in
   the same transaction), and the captions room (`nexus:{org_id}:pubsub:meeting:{meeting_id}`,
   addressed through `persistence.process_utterance`). It holds NO provider stream.

2. PARTICIPANT streams (`ParticipantLease`, keyed by `(org_id, meeting_id,
   participant_identity)`): the audio sender (`meeting-service`) opens one `/internal/audio`
   connection per participant audio track, and each connection gets its own provider stream
   (`ReconnectingStream` over the selected provider, `diarize=False` -- one speaker per track)
   and its own pump. Every utterance a participant's stream produces is attributed to that
   connection's `participant_identity` (a `users.id`) by setting `Utterance.speaker_id` to it
   before persistence. That reuses the existing `speaker_id` column / Avro field and the
   existing downstream `meeting_speakers` `(org_id, meeting_id, speaker_id)` mapping path; no
   schema changes. A reconnect by the SAME participant supersedes only that participant's
   old connection (its `superseded` event is set and its handler closes 4409); other
   participants are untouched.

   Cost note (recorded, not hidden): this is one vendor stream per participant track, not the
   "one diarized Deepgram session per meeting" this repo's CLAUDE.md describes. It follows
   the sender's actual per-track shape, as decided by the lead; mixing tracks server-side
   would be the alternative to restore per-meeting cost.

Lifetime: a meeting session opens on the first participant attach (or on a
`meeting-room-events` `joined` hint from the consumer) and closes when its LAST participant
connection detaches. The consumer's `joined`/`left` remain idempotent hints: `left` closes a
session only when no hint and no participant connection keeps it open.

Every open/close of a meeting session, and every participant attach/detach, runs under a
ref-counted per-meeting lock, so concurrent connects for one meeting can neither open two
meeting sessions nor race a close.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .audit import record_session_decision
from .db import set_current_org
from .persistence import process_utterance
from .providers.base import TranscriptionProvider, TranscriptionStream
from .providers.resilient import ReconnectingStream
from .tenancy import get_data_residency_region, get_pii_redaction_enabled

logger = logging.getLogger(__name__)

# How long closing a participant stream waits for its pump to drain the provider's last
# results (Deepgram flushes the pending final after CloseStream) before cancelling it.
_PUMP_DRAIN_TIMEOUT_SECONDS = 2.0


class TenantMismatchError(Exception):
    """A meeting's open session belongs to a different `org_id` than the caller named."""


@dataclass
class ParticipantLease:
    """One `/internal/audio` connection's participant stream."""

    org_id: str
    meeting_id: str
    participant_identity: str
    connection_id: str
    stream: ReconnectingStream
    pump_task: asyncio.Task[None] | None = None
    # Where this participant's stream sits on the meeting timeline, in ms: added to every time
    # the stream reports (see `meeting_timeline`).
    meeting_offset_ms: int = 0
    superseded: asyncio.Event = field(default_factory=asyncio.Event)
    # Set only for a diarized stream that carries several people (the capture companion's tab
    # channel, `capture_in.py`): each provider speaker index becomes `{prefix}{index + 1}`
    # instead of the connection's single participant identity.
    speaker_prefix: str | None = None


def speaker_for(lease: ParticipantLease, provider_speaker: str) -> str:
    """Who an utterance is attributed to. A single-speaker connection (one participant track,
    or the capture mic) is its participant identity, a `users.id`. A diarized multi-speaker
    connection maps Deepgram's per-stream speaker index ("0", "1", ...) to a stable, readable,
    non-UUID label (`1`, `2`, ... with an empty prefix) that the downstream `meeting_speakers`
    mapping can name, and that the agent runtime never mistakes for a user (it only adds UUID
    speakers to its roster)."""
    if lease.speaker_prefix is None:
        return lease.participant_identity
    raw = provider_speaker.strip()
    label = str(int(raw) + 1) if raw.isdigit() else (raw or "1")
    return f"{lease.speaker_prefix}{label}"[:256]


def meeting_timeline(lease: ParticipantLease, start_ms: int, end_ms: int) -> tuple[int, int]:
    """Place one utterance on the meeting's timeline.

    Each participant has their own provider stream, whose clock starts at zero when that
    participant's audio connects (`ReconnectingStream` already keeps it continuous across
    reconnects, from the audio actually sent). Without an offset everyone's lines sat near
    00:00. The offset is fixed when the participant joins: how far into the meeting that was.
    """
    return start_ms + lease.meeting_offset_ms, end_ms + lease.meeting_offset_ms


@dataclass
class _MeetingSession:
    org_id: str
    region: str
    pii_redaction_enabled: bool
    hint_count: int = 0
    participants: dict[str, ParticipantLease] = field(default_factory=dict)
    started_at: float = field(default_factory=time.monotonic)
    # The transcript time this session continues from: the meeting's last saved line, so a
    # meeting everyone left and rejoined does not restart at 00:00.
    base_ms: int = 0


@dataclass
class SessionTracker:
    provider: TranscriptionProvider
    session_factory: async_sessionmaker[AsyncSession]
    redis: Redis | None
    _sessions: dict[str, _MeetingSession] = field(default_factory=dict)
    # meeting_id -> (lock, number of holders + waiters). Ref-counted so the entry is dropped
    # once nobody holds or waits on it: a plain dict of locks would grow by one entry per
    # meeting ever seen, and dropping an entry someone is still waiting on would let a second
    # caller mint a fresh lock and race the first.
    _locks: dict[str, tuple[asyncio.Lock, int]] = field(default_factory=dict)

    @asynccontextmanager
    async def _lock_for(self, meeting_id: str) -> AsyncIterator[None]:
        lock, refs = self._locks.get(meeting_id, (asyncio.Lock(), 0))
        self._locks[meeting_id] = (lock, refs + 1)
        try:
            async with lock:
                yield
        finally:
            lock, refs = self._locks[meeting_id]
            if refs <= 1:
                del self._locks[meeting_id]
            else:
                self._locks[meeting_id] = (lock, refs - 1)

    # ------------------------------------------------------------------ introspection

    def has_session(self, meeting_id: str) -> bool:
        return meeting_id in self._sessions

    def participant_count(self, meeting_id: str) -> int:
        session = self._sessions.get(meeting_id)
        return 0 if session is None else len(session.participants)

    @property
    def active_session_count(self) -> int:
        return len(self._sessions)

    # ------------------------------------------------------------------ consumer hints

    async def on_participant_joined(self, *, org_id: str, meeting_id: str) -> bool:
        """`meeting-room-events` `joined` hint. Returns `True` iff this call opened the
        meeting session; `False` if it was already open (redelivery or a later participant).
        Opens no provider stream: streams belong to participant audio connections."""
        async with self._lock_for(meeting_id):
            existing = self._sessions.get(meeting_id)
            if existing is not None:
                if existing.org_id != org_id:
                    logger.error(
                        "meeting-room-events joined for meeting %s names a different org than "
                        "its open session; ignored",
                        meeting_id,
                    )
                    return False
                existing.hint_count += 1
                return False
            session = await self._open_meeting(org_id=org_id, meeting_id=meeting_id)
            session.hint_count = 1
            return True

    async def on_participant_left(self, *, org_id: str, meeting_id: str) -> bool:
        """`left` hint. Returns `True` iff this call closed the meeting session (no hint and
        no participant connection left). Safe no-op for an unknown meeting or another org."""
        async with self._lock_for(meeting_id):
            existing = self._sessions.get(meeting_id)
            if existing is None or existing.org_id != org_id:
                return False
            existing.hint_count = max(0, existing.hint_count - 1)
            if existing.hint_count > 0 or existing.participants:
                return False
            await self._close_meeting(meeting_id)
            return True

    # ------------------------------------------------------------------ participant streams

    async def attach_participant(
        self,
        *,
        org_id: str,
        meeting_id: str,
        participant_identity: str,
        diarize: bool = False,
        speaker_prefix: str | None = None,
    ) -> ParticipantLease:
        """Opens (or reuses) the meeting session and opens a NEW provider stream for this
        participant connection. `diarize`/`speaker_prefix` are for a connection that carries
        several speakers (see `speaker_for`); the default is one speaker per connection. A
        previous connection for the same participant is superseded and its stream closed.
        Raises `TenantMismatchError` on an org mismatch; lets `ComplianceGateClosedError` or
        any provider open failure propagate (and closes a meeting session it opened just for
        this attach)."""
        async with self._lock_for(meeting_id):
            meeting = self._sessions.get(meeting_id)
            opened_here = False
            if meeting is None:
                meeting = await self._open_meeting(org_id=org_id, meeting_id=meeting_id)
                opened_here = True
            elif meeting.org_id != org_id:
                raise TenantMismatchError(
                    f"meeting {meeting_id} has an open session for a different org"
                )

            previous = meeting.participants.pop(participant_identity, None)
            if previous is not None:
                previous.superseded.set()
                await self._stop_participant(previous)

            region = meeting.region
            # The participant who opens the meeting is its zero (plus where an earlier
            # session's transcript ended); anyone joining later starts that far in.
            offset_ms = meeting.base_ms + (
                0 if opened_here else int((time.monotonic() - meeting.started_at) * 1000)
            )

            async def _open_inner() -> TranscriptionStream:
                # Region pin applies to the first connection and to every reconnect.
                return await self.provider.open_stream(
                    org_id=org_id, meeting_id=meeting_id, region=region, diarize=diarize
                )

            try:
                stream = await ReconnectingStream.open(_open_inner)
            except BaseException:
                if opened_here or (not meeting.participants and meeting.hint_count == 0):
                    await self._close_meeting(meeting_id)
                raise

            lease = ParticipantLease(
                org_id=org_id,
                meeting_id=meeting_id,
                participant_identity=participant_identity,
                connection_id=str(uuid.uuid4()),
                stream=stream,
                meeting_offset_ms=offset_ms,
                speaker_prefix=speaker_prefix if diarize else None,
            )
            lease.pump_task = asyncio.create_task(
                self._pump(lease, pii_redaction_enabled=meeting.pii_redaction_enabled)
            )
            meeting.participants[participant_identity] = lease
            return lease

    async def detach_participant(self, lease: ParticipantLease) -> bool:
        """Ends `lease`'s stream. Returns `True` iff this was the meeting's last participant
        connection and the meeting session closed. A superseded lease is already stopped; its
        detach is a no-op."""
        async with self._lock_for(lease.meeting_id):
            meeting = self._sessions.get(lease.meeting_id)
            if meeting is None or meeting.participants.get(lease.participant_identity) is not lease:
                return False
            del meeting.participants[lease.participant_identity]
            await self._stop_participant(lease)
            if meeting.participants:
                return False
            await self._close_meeting(lease.meeting_id)
            return True

    async def close_all(self) -> None:
        """Shutdown: stop every participant stream and close every meeting session."""
        for meeting_id in list(self._sessions):
            async with self._lock_for(meeting_id):
                meeting = self._sessions.get(meeting_id)
                if meeting is None:
                    continue
                for lease in list(meeting.participants.values()):
                    await self._stop_participant(lease)
                meeting.participants.clear()
                await self._close_meeting(meeting_id)

    # ------------------------------------------------------------------ internals

    async def _open_meeting(self, *, org_id: str, meeting_id: str) -> _MeetingSession:
        async with self.session_factory() as session:
            async with session.begin():
                await set_current_org(session, org_id)
                region = await get_data_residency_region(session, org_id)
                pii_redaction_enabled = await get_pii_redaction_enabled(session, org_id)
                base_ms = int(
                    (
                        await session.execute(
                            text(
                                "SELECT COALESCE(MAX(end_ms), 0) FROM transcript_chunks "
                                "WHERE meeting_id = :m"
                            ),
                            {"m": meeting_id},
                        )
                    ).scalar_one()
                )
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
        meeting = _MeetingSession(
            org_id=org_id,
            region=region,
            pii_redaction_enabled=pii_redaction_enabled,
            base_ms=base_ms,
        )
        self._sessions[meeting_id] = meeting
        return meeting

    async def _close_meeting(self, meeting_id: str) -> None:
        meeting = self._sessions.pop(meeting_id, None)
        if meeting is None:
            return
        for lease in list(meeting.participants.values()):
            await self._stop_participant(lease)

    @staticmethod
    async def _stop_participant(lease: ParticipantLease) -> None:
        await lease.stream.close()
        task = lease.pump_task
        if task is None or task.done():
            return
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=_PUMP_DRAIN_TIMEOUT_SECONDS)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(BaseException):
                await task

    async def _pump(self, lease: ParticipantLease, *, pii_redaction_enabled: bool) -> None:
        """Drains one participant stream into `persistence.process_utterance`, strictly in
        order, attributing every utterance to the connection's participant. Errors are
        logged, never allowed to crash the event loop silently."""
        try:
            async for utterance in lease.stream.utterances():
                # Silence and breath come back as results with no words. Saving them filled
                # transcripts with blank lines; captioning them showed a name with no text.
                if not utterance.text.strip():
                    continue
                start_ms, end_ms = meeting_timeline(lease, utterance.start_ms, utterance.end_ms)
                await process_utterance(
                    session_factory=self.session_factory,
                    redis=self.redis,
                    org_id=lease.org_id,
                    meeting_id=lease.meeting_id,
                    utterance=replace(
                        utterance,
                        speaker_id=speaker_for(lease, utterance.speaker_id),
                        start_ms=start_ms,
                        end_ms=end_ms,
                    ),
                    pii_redaction_enabled=pii_redaction_enabled,
                )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a pump failure must not crash the process
            logger.exception(
                "transcription pump failed for meeting %s (org %s, connection %s)",
                lease.meeting_id,
                lease.org_id,
                lease.connection_id,
            )
