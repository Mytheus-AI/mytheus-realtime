"""Fix round 2: `_DeepgramStream.utterances()`'s `chunk_id` derivation.

`request_id` is Deepgram's own per-SESSION identifier — constant across every message in one
streaming connection, not unique per utterance. The original derivation,
`uuid5(NAMESPACE_URL, f"deepgram:{request_id}:{start_ms}")`, therefore collided whenever two
GENUINELY DISTINCT finals shared a `start_ms` — ordinary behavior in a diarized transcript,
where two speakers starting to talk at the same millisecond is overlapping speech, not an
edge case. The collision was not an error: the second final's `chunk_id` matched the first's,
so the outbox's `(topic, idempotency_key)` dedup gate in `persistence.py::_write_final`
silently treated it as a redelivery and dropped it — a real transcript line lost with no log
and no exception.

`speaker_id` and `end_ms` are now part of the key material, so two distinct utterances need to
share speaker AND both time boundaries to collide (effectively only a true redelivery of the
identical message), while the key stays a pure, deterministic function of the underlying
provider event -- required for `chunk:{chunk_id}` idempotency to mean anything on genuine
redelivery.

Both halves matter and are both tested here: the collision is gone (two distinct utterances
sharing `start_ms` both persist), and idempotency was not broken achieving it (byte-identical
redelivery of the same message still dedupes, end to end through the real outbox).
"""

from __future__ import annotations

import json

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transcription_service.persistence import process_utterance
from transcription_service.providers.deepgram import _DeepgramStream


async def _fake_ws(messages: list[dict[str, object]]):
    for m in messages:
        yield json.dumps(m)


def _final_message(
    *, request_id: str, speaker: int, start_s: float, duration_s: float, transcript: str
) -> dict[str, object]:
    return {
        "request_id": request_id,
        "is_final": True,
        "start": start_s,
        "duration": duration_s,
        "channel": {
            "alternatives": [
                {"transcript": transcript, "confidence": 0.95, "words": [{"speaker": speaker}]}
            ]
        },
    }


@pytest.mark.asyncio
async def test_two_distinct_utterances_sharing_start_ms_get_different_chunk_ids() -> None:
    """The collision the reviewer found: same session (`request_id`), same `start_ms`,
    different speaker -- two overlapping speakers starting at the same millisecond."""
    messages = [
        _final_message(
            request_id="sess-1", speaker=0, start_s=0.400, duration_s=0.500, transcript="hello"
        ),
        _final_message(
            request_id="sess-1", speaker=1, start_s=0.400, duration_s=0.300, transcript="wait"
        ),
    ]
    stream = _DeepgramStream(_ws=_fake_ws(messages))
    utterances = [u async for u in stream.utterances()]

    assert len(utterances) == 2
    assert utterances[0].start_ms == utterances[1].start_ms == 400
    assert utterances[0].speaker_id != utterances[1].speaker_id
    assert utterances[0].chunk_id != utterances[1].chunk_id, "distinct utterances collided"


@pytest.mark.asyncio
async def test_byte_identical_redelivery_yields_the_same_chunk_id() -> None:
    """The determinism half: the SAME raw message processed twice (what a genuine redelivery
    looks like at this boundary) must still derive the identical chunk_id, or
    `chunk:{chunk_id}` idempotency means nothing."""
    message = _final_message(
        request_id="sess-1", speaker=0, start_s=1.200, duration_s=0.600, transcript="redelivered"
    )
    stream = _DeepgramStream(_ws=_fake_ws([message, message]))
    utterances = [u async for u in stream.utterances()]

    assert len(utterances) == 2
    assert utterances[0].chunk_id == utterances[1].chunk_id


@pytest.mark.asyncio
async def test_two_distinct_utterances_both_persist_and_a_redelivery_still_dedupes(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    """End-to-end through the real pipeline against real Postgres: proves the collision fix
    and the idempotency guarantee together, not just the key-derivation unit above."""
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    concurrent_speech = [
        _final_message(
            request_id="sess-e2e", speaker=0, start_s=2.000, duration_s=0.400, transcript="go ahead"
        ),
        _final_message(
            request_id="sess-e2e", speaker=1, start_s=2.000, duration_s=0.250, transcript="sorry no"
        ),
    ]
    stream = _DeepgramStream(_ws=_fake_ws(concurrent_speech))
    utterances = [u async for u in stream.utterances()]

    for u in utterances:
        result = await process_utterance(
            session_factory=session_factory,
            redis=redis_client,
            org_id=org_id,
            meeting_id=meeting_id,
            utterance=u,
            pii_redaction_enabled=False,
        )
        assert result is not None, "a genuinely distinct concurrent utterance must not be dropped"

    async with session_factory() as session:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT speaker_id, text FROM transcript_chunks "
                        "WHERE org_id = :org_id AND meeting_id = :meeting_id ORDER BY speaker_id"
                    ),
                    {"org_id": org_id, "meeting_id": meeting_id},
                )
            )
            .mappings()
            .all()
        )
    assert len(rows) == 2, "both concurrent finals must persist as distinct rows"
    assert {r["text"] for r in rows} == {"go ahead", "sorry no"}

    # Now genuinely redeliver the FIRST message (same request_id/speaker/start/end) and prove
    # it still dedupes rather than creating a third row.
    redelivered_stream = _DeepgramStream(_ws=_fake_ws([concurrent_speech[0]]))
    redelivered = [u async for u in redelivered_stream.utterances()][0]
    dedupe_result = await process_utterance(
        session_factory=session_factory,
        redis=redis_client,
        org_id=org_id,
        meeting_id=meeting_id,
        utterance=redelivered,
        pii_redaction_enabled=False,
    )
    assert dedupe_result is None, "redelivery of an already-written final must be a no-op"

    async with session_factory() as session:
        count = (
            await session.execute(
                text(
                    "SELECT count(*) FROM transcript_chunks "
                    "WHERE org_id = :org_id AND meeting_id = :meeting_id"
                ),
                {"org_id": org_id, "meeting_id": meeting_id},
            )
        ).scalar_one()
    assert count == 2, "redelivery must not create a third row"
