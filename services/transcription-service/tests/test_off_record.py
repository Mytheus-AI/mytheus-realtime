"""Off the record: while `nexus:{org_id}:off-record:{meeting_id}` is set, `process_utterance`
drops every utterance, interim or final, before any caption, row or message.

The gate itself is unit-tested (cache window, Redis errors keep the last state, unknown counts as
on the record). The end-to-end check runs the real pipeline against the real Postgres and Redis:
an off-the-record final writes no `transcript_chunks` row, no outbox row and publishes no
caption; the same final once the flag is gone writes exactly one of each.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transcription_service import off_record
from transcription_service.off_record import OffRecordGate, off_record_key
from transcription_service.persistence import process_utterance
from transcription_service.providers.base import Utterance


class FakeRedis:
    def __init__(self) -> None:
        self.keys: set[str] = set()
        self.fail = False
        self.reads = 0
        self.published: list[tuple[str, str]] = []

    async def exists(self, key: str) -> int:
        self.reads += 1
        if self.fail:
            raise ConnectionError("down")
        return 1 if key in self.keys else 0

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1


class Clock:
    def __init__(self) -> None:
        self.t = 100.0

    def __call__(self) -> float:
        return self.t


async def test_gate_reads_the_flag_and_caches_briefly() -> None:
    redis, clock = FakeRedis(), Clock()
    gate = OffRecordGate(cache_s=0.5, clock=clock)
    assert await gate.active(redis, "o", "m") is False
    redis.keys.add(off_record_key("o", "m"))
    assert await gate.active(redis, "o", "m") is False  # cached for half a second
    assert redis.reads == 1
    clock.t += 0.6
    assert await gate.active(redis, "o", "m") is True
    assert redis.reads == 2
    assert await gate.active(redis, "o", "other") is False  # per meeting


async def test_gate_keeps_the_last_state_when_redis_fails() -> None:
    redis, clock = FakeRedis(), Clock()
    gate = OffRecordGate(cache_s=0.5, clock=clock)
    redis.keys.add(off_record_key("o", "m"))
    assert await gate.active(redis, "o", "m") is True
    redis.fail = True
    clock.t += 1
    assert await gate.active(redis, "o", "m") is True  # still paused
    assert await gate.active(redis, "o", "never-read") is False  # unknown: on the record


def _utterance(chunk_id: str, *, final: bool) -> Utterance:
    return Utterance(
        chunk_id=chunk_id,
        speaker_id="0",
        start_ms=0,
        end_ms=900,
        text="the number they offered was lower",
        is_final=final,
        confidence=0.95,
    )


async def test_off_record_interim_publishes_no_caption(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(off_record, "GATE", OffRecordGate(cache_s=0))
    import transcription_service.persistence as persistence

    monkeypatch.setattr(persistence, "GATE", off_record.GATE)
    redis = FakeRedis()
    redis.keys.add(off_record_key("o", "m"))
    result = await process_utterance(
        session_factory=None,  # type: ignore[arg-type]  # never reached while off the record
        redis=redis,  # type: ignore[arg-type]
        org_id="o",
        meeting_id="m",
        utterance=_utterance(str(uuid.uuid4()), final=False),
        pii_redaction_enabled=False,
    )
    assert result is None and redis.published == []
    redis.keys.clear()
    await process_utterance(
        session_factory=None,  # type: ignore[arg-type]  # an interim never touches the DB
        redis=redis,  # type: ignore[arg-type]
        org_id="o",
        meeting_id="m",
        utterance=_utterance(str(uuid.uuid4()), final=False),
        pii_redaction_enabled=False,
    )
    assert len(redis.published) == 1
    assert json.loads(redis.published[0][1])["type"] == "caption"


async def _count(factory: async_sessionmaker[AsyncSession], sql: str, org_id: str) -> int:
    async with factory() as session:
        return int((await session.execute(text(sql), {"o": org_id})).scalar_one())


async def test_off_record_final_is_not_persisted_or_produced(
    session_factory: async_sessionmaker[AsyncSession],
    redis_client: Any,
    tenant: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import transcription_service.persistence as persistence

    monkeypatch.setattr(persistence, "GATE", OffRecordGate(cache_s=0))
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    key = off_record_key(org_id, meeting_id)
    channel = f"nexus:{org_id}:pubsub:meeting:{meeting_id}"
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)
    await redis_client.set(key, json.dumps({"segment_id": str(uuid.uuid4())}))
    try:
        result = await process_utterance(
            session_factory=session_factory,
            redis=redis_client,
            org_id=org_id,
            meeting_id=meeting_id,
            utterance=_utterance(str(uuid.uuid4()), final=True),
            pii_redaction_enabled=False,
        )
        assert result is None
        rows = "SELECT count(*) FROM transcript_chunks WHERE org_id = :o"
        outbox = "SELECT count(*) FROM outbox WHERE org_id = :o AND topic = 'transcript-chunks'"
        assert await _count(session_factory, rows, org_id) == 0
        assert await _count(session_factory, outbox, org_id) == 0

        await redis_client.delete(key)
        result = await process_utterance(
            session_factory=session_factory,
            redis=redis_client,
            org_id=org_id,
            meeting_id=meeting_id,
            utterance=_utterance(str(uuid.uuid4()), final=True),
            pii_redaction_enabled=False,
        )
        assert result is not None
        assert await _count(session_factory, rows, org_id) == 1
        assert await _count(session_factory, outbox, org_id) == 1
        captions = []
        for _ in range(20):
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if msg is not None:
                captions.append(json.loads(msg["data"]))
            elif captions:
                break
            await asyncio.sleep(0.01)
        assert [c["type"] for c in captions] == ["caption"]  # only the on-the-record final
    finally:
        await redis_client.delete(key)
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()
