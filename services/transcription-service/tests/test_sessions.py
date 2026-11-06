"""`SessionTracker` against real Postgres: one meeting session per `meeting_id` (opened by the
first `joined` hint or participant attach, closed when the last participant connection ends
or, with no connection, the last `left` hint), and one provider stream per participant
connection whose utterances persist attributed to that participant. A redelivered
`joined`/`left` must never open a second meeting session or double-close.

Drives `FakeProvider`, so this is real pipeline wiring (participant stream -> utterance pump
-> `persistence.process_utterance` -> real Postgres), just not a real Kafka consumer loop or
WebSocket around it (`tests/test_internal_audio.py` covers the WebSocket over fakes).
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transcription_service.providers.base import Utterance
from transcription_service.providers.fake import FakeProvider
from transcription_service.sessions import SessionTracker


@pytest.mark.asyncio
async def test_second_join_does_not_open_a_second_meeting_session(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    provider = FakeProvider(script=[])
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)

    opened_first = await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)
    opened_second = await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)

    assert opened_first is True
    assert opened_second is False, "a second (or redelivered) join must not reopen the session"
    assert provider.opens == [], "hints open no provider stream; participant connections do"


@pytest.mark.asyncio
async def test_hinted_session_closes_only_on_last_leave(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    tracker = SessionTracker(
        provider=FakeProvider(script=[]), session_factory=session_factory, redis=redis_client
    )

    await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)
    await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)  # 2 participants

    closed_first = await tracker.on_participant_left(org_id=org_id, meeting_id=meeting_id)
    assert closed_first is False, "one leave with a second participant still present must not close"

    closed_second = await tracker.on_participant_left(org_id=org_id, meeting_id=meeting_id)
    assert closed_second is True, "the last leave must close the session"

    redelivered_leave = await tracker.on_participant_left(org_id=org_id, meeting_id=meeting_id)
    assert redelivered_leave is False, "a redelivered leave after close must be a safe no-op"


@pytest.mark.asyncio
async def test_participant_finals_persist_exactly_once_attributed_to_the_participant(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    chunk_id = str(uuid.uuid4())
    participant = str(uuid.uuid4())
    final = Utterance(
        chunk_id=chunk_id,
        speaker_id="0",
        start_ms=0,
        end_ms=500,
        text="pumped final",
        is_final=True,
        confidence=0.9,
    )
    provider = FakeProvider(script=[final])
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)

    lease = await tracker.attach_participant(
        org_id=org_id, meeting_id=meeting_id, participant_identity=participant
    )
    await asyncio.sleep(0.2)  # let the background pump drain the (short) scripted stream
    assert await tracker.detach_participant(lease) is True, "last connection closes the meeting"

    async with session_factory() as session:
        rows = (
            await session.execute(
                text("SELECT speaker_id FROM transcript_chunks WHERE id = :id"), {"id": chunk_id}
            )
        ).all()
    assert [r[0] for r in rows] == [participant]


@pytest.mark.asyncio
async def test_default_script_persists_through_the_real_pipeline(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    """Fix round 1, item 1: `FakeProvider()` with NO script override (what
    `select_provider()` returns under the dev flag) driven unmodified through a participant
    stream's real background pump into a real Postgres INSERT."""
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    provider = FakeProvider()  # script=None -> default_script(), chunk_id NOT overridden
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)

    lease = await tracker.attach_participant(
        org_id=org_id, meeting_id=meeting_id, participant_identity=str(uuid.uuid4())
    )
    await asyncio.sleep(0.2)
    await tracker.detach_participant(lease)

    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT text, is_final FROM transcript_chunks "
                        "WHERE org_id = :org_id AND meeting_id = :meeting_id"
                    ),
                    {"org_id": org_id, "meeting_id": meeting_id},
                )
            )
            .mappings()
            .one()
        )
    assert row["is_final"] is True
    assert row["text"] == "hello there, how are you"
