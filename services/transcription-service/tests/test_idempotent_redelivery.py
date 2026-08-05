"""A redelivered final (`chunk:{chunk_id}`) never double-writes -- plan 3.6 acceptance check.
Simulates redelivery by calling `process_utterance` twice with the identical final `Utterance`
(same `chunk_id`), against the real Postgres."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transcription_service.persistence import process_utterance
from transcription_service.providers.base import Utterance


@pytest.mark.asyncio
async def test_redelivered_final_does_not_double_write(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    chunk_id = str(uuid.uuid4())
    final = Utterance(
        chunk_id=chunk_id,
        speaker_id="1",
        start_ms=100,
        end_ms=500,
        text="redelivery test",
        is_final=True,
        confidence=0.9,
    )

    first = await process_utterance(
        session_factory=session_factory,
        redis=redis_client,
        org_id=org_id,
        meeting_id=meeting_id,
        utterance=final,
        pii_redaction_enabled=False,
    )
    second = await process_utterance(
        session_factory=session_factory,
        redis=redis_client,
        org_id=org_id,
        meeting_id=meeting_id,
        utterance=final,
        pii_redaction_enabled=False,
    )

    assert first is not None
    assert second is None, "a redelivered final must be a no-op, not a second write"

    async with session_factory() as session:
        chunk_count = (
            await session.execute(
                text(
                    "SELECT count(*) FROM transcript_chunks WHERE org_id = :org_id "
                    "AND meeting_id = :meeting_id"
                ),
                {"org_id": org_id, "meeting_id": meeting_id},
            )
        ).scalar_one()
        outbox_count = (
            await session.execute(
                text(
                    "SELECT count(*) FROM outbox WHERE org_id = :org_id "
                    "AND topic = 'transcript-chunks'"
                ),
                {"org_id": org_id},
            )
        ).scalar_one()
    assert chunk_count == 1
    assert outbox_count == 1
