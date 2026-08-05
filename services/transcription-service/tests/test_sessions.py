"""`SessionTracker`: one Deepgram session per `meeting_id`, opened on the first join, closed on
the last leave; a redelivered `joined`/`left` must never open a second stream or double-close.
Drives `FakeProvider` so this is real end-to-end pipeline wiring (session open -> utterance pump
-> `persistence.process_utterance` -> real Postgres), just not a real Kafka consumer loop
around it (see `meeting_room_consumer.py`'s docstring for that scope note).
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
async def test_second_join_does_not_open_a_second_stream(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    provider = FakeProvider(script=[])  # empty script: nothing to pump, just proves open count
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)

    opened_first = await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)
    opened_second = await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)

    assert opened_first is True
    assert opened_second is False, "a second (or redelivered) join must not open a second stream"
    assert len(provider.opens) == 1


@pytest.mark.asyncio
async def test_stream_closes_only_on_last_leave(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    provider = FakeProvider(script=[])
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)

    await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)
    await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)  # 2 participants

    closed_first = await tracker.on_participant_left(org_id=org_id, meeting_id=meeting_id)
    assert closed_first is False, "one leave with a second participant still present must not close"

    closed_second = await tracker.on_participant_left(org_id=org_id, meeting_id=meeting_id)
    assert closed_second is True, "the last leave must close the stream"

    redelivered_leave = await tracker.on_participant_left(org_id=org_id, meeting_id=meeting_id)
    assert redelivered_leave is False, "a redelivered leave after close must be a safe no-op"


@pytest.mark.asyncio
async def test_final_utterances_from_the_pump_persist_exactly_once(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    chunk_id = str(uuid.uuid4())
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

    await tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id)
    await asyncio.sleep(0.2)  # let the background pump task drain the (short) scripted stream
    await tracker.on_participant_left(org_id=org_id, meeting_id=meeting_id)

    async with session_factory() as session:
        count = (
            await session.execute(
                text("SELECT count(*) FROM transcript_chunks WHERE id = :id"), {"id": chunk_id}
            )
        ).scalar_one()
    assert count == 1
