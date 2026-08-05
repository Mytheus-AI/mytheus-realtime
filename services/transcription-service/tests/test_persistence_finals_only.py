"""THE LOAD-BEARING TEST (task brief, resolution #2): "interims produce ZERO transcript_chunks
rows and ZERO bus messages." Drives a realistic interim-then-final sequence through the real
pipeline (`persistence.process_utterance`) against the real Postgres and the real Redpanda
broker, asserting both zeros explicitly, then asserting the final produces exactly one row and
exactly one message.
"""

from __future__ import annotations

import uuid

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from conftest import TEST_REDPANDA_BROKERS, TEST_SCHEMA_REGISTRY_URL
from helpers.relay import drain_outbox_for_org
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transcription_service.persistence import process_utterance
from transcription_service.providers.base import Utterance


async def _outbox_count(session_factory: async_sessionmaker[AsyncSession], org_id: str) -> int:
    async with session_factory() as session:
        result = await session.execute(
            text(
                "SELECT count(*) FROM outbox WHERE org_id = :org_id AND topic = 'transcript-chunks'"
            ),
            {"org_id": org_id},
        )
        return int(result.scalar_one())


async def _chunk_count(session_factory: async_sessionmaker[AsyncSession], org_id: str) -> int:
    async with session_factory() as session:
        result = await session.execute(
            text("SELECT count(*) FROM transcript_chunks WHERE org_id = :org_id"),
            {"org_id": org_id},
        )
        return int(result.scalar_one())


@pytest.mark.asyncio
async def test_interims_zero_rows_zero_bus_messages_final_writes_exactly_one(
    session_factory: async_sessionmaker[AsyncSession], redis_client, tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    chunk_id = str(uuid.uuid4())

    interim_1 = Utterance(
        chunk_id=chunk_id,
        speaker_id="0",
        start_ms=0,
        end_ms=400,
        text="hello",
        is_final=False,
        confidence=0.4,
    )
    interim_2 = Utterance(
        chunk_id=chunk_id,
        speaker_id="0",
        start_ms=0,
        end_ms=900,
        text="hello there",
        is_final=False,
        confidence=0.6,
    )
    final = Utterance(
        chunk_id=chunk_id,
        speaker_id="0",
        start_ms=0,
        end_ms=1200,
        text="hello there, how are you",
        is_final=True,
        confidence=0.97,
    )

    # --- Drive the two interims first -------------------------------------------------
    for interim in (interim_1, interim_2):
        result = await process_utterance(
            session_factory=session_factory,
            redis=redis_client,
            org_id=org_id,
            meeting_id=meeting_id,
            utterance=interim,
            pii_redaction_enabled=True,
        )
        assert result is None, "an interim must never return a FinalWriteResult"

    # THE LOAD-BEARING ASSERTION, part 1: zero rows, zero bus messages after interims only.
    assert await _chunk_count(session_factory, org_id) == 0
    assert await _outbox_count(session_factory, org_id) == 0

    # Drain the (empty) outbox against the REAL broker too -- proves there is nothing to
    # produce, not merely that we didn't look.
    async with session_factory() as session:
        async with session.begin():
            produced = await drain_outbox_for_org(
                session,
                org_id=org_id,
                brokers=TEST_REDPANDA_BROKERS,
                schema_registry_url=TEST_SCHEMA_REGISTRY_URL,
            )
    assert produced == []

    # --- Now the final ------------------------------------------------------------------
    result = await process_utterance(
        session_factory=session_factory,
        redis=redis_client,
        org_id=org_id,
        meeting_id=meeting_id,
        utterance=final,
        pii_redaction_enabled=True,
    )
    assert result is not None
    assert result.chunk_id == chunk_id

    assert await _chunk_count(session_factory, org_id) == 1
    assert await _outbox_count(session_factory, org_id) == 1

    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT speaker_id, start_ms, end_ms, text, is_final, confidence, redacted "
                        "FROM transcript_chunks WHERE org_id = :org_id AND meeting_id = :meeting_id"
                    ),
                    {"org_id": org_id, "meeting_id": meeting_id},
                )
            )
            .mappings()
            .one()
        )
    assert row["speaker_id"] == "0"
    assert row["start_ms"] == 0
    assert row["end_ms"] == 1200
    assert row["text"] == "hello there, how are you"
    assert row["is_final"] is True
    assert row["redacted"] is False  # no PII in this utterance

    async with session_factory() as session:
        outbox_row = (
            (
                await session.execute(
                    text(
                        "SELECT partition_key, idempotency_key, schema_subject FROM outbox "
                        "WHERE org_id = :org_id AND topic = 'transcript-chunks'"
                    ),
                    {"org_id": org_id},
                )
            )
            .mappings()
            .one()
        )
    # Composite partition key per ADR-0017: {org_id}:{meeting_id}, transcript-chunks/agent-stream
    # only.
    assert outbox_row["partition_key"] == f"{org_id}:{meeting_id}"
    assert outbox_row["idempotency_key"] == f"chunk:{chunk_id}"
    assert outbox_row["schema_subject"] == "transcript-chunks-value"

    # --- Drain against the REAL broker and consume it back with a real consumer --------
    async with session_factory() as session:
        async with session.begin():
            produced = await drain_outbox_for_org(
                session,
                org_id=org_id,
                brokers=TEST_REDPANDA_BROKERS,
                schema_registry_url=TEST_SCHEMA_REGISTRY_URL,
            )
    assert len(produced) == 1

    sr = SchemaRegistryClient({"url": TEST_SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(sr)
    # Seek directly to the partition/offset the delivery callback reported, rather than
    # scanning from the earliest offset: `transcript-chunks` has 180 partitions and is shared
    # with every other agent's concurrent test traffic in this environment, so an unbounded
    # earliest-offset scan is neither fast nor deterministic.
    topic_partition = TopicPartition(
        "transcript-chunks", produced[0]["partition"], produced[0]["offset"]
    )
    consumer = Consumer(
        {
            "bootstrap.servers": TEST_REDPANDA_BROKERS,
            "group.id": f"test-consumer-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.assign([topic_partition])
    try:
        found = None
        for _ in range(20):
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            value = deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            # `chunk_id`/`meeting_id`/`org_id` are Avro `logicalType: uuid`; fastavro decodes
            # those to `uuid.UUID` objects, not `str` -- compare via `str()` on both sides.
            if value is not None and str(value.get("chunk_id")) == chunk_id:
                found = value
                assert msg.key().decode() == f"{org_id}:{meeting_id}"
                break
        assert found is not None, "the final's message was never observed on the real broker"
        assert found["is_final"] is True
        assert found["text"] == "hello there, how are you"
        assert found["redacted"] is False
        assert str(found["header"]["org_id"]) == org_id
        assert found["header"]["producer"] == "transcription-service"
    finally:
        consumer.close()
