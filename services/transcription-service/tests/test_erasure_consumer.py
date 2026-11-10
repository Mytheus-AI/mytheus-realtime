"""`erasure_consumer`: `admin-events` `erasure_requested` finishes an erasure in transcripts.

Against the real local Postgres and Redis, like the rest of this suite:

1. A MEETING erasure deletes that meeting's rows and nothing else: the same org's other meeting
   and ANOTHER org's rows (same meeting id, to prove the org predicate) survive.
2. A PERSON erasure renames the lines whose `speaker_id` is their `users.id` to `erased` and
   keeps the words (core-api's rule); diarized labels and another org's rows are untouched.
3. Both write one `erasure_completed` outbox row whose value encodes against the proposed
   `AdminEvent` v2 schema (the registered v1 has no such symbol yet).
4. The same event twice is a no-op (Redis), and so is a redelivery after the Redis key is gone
   (the completion outbox row is the durable record).
5. Other admin events are ignored; an unusable one is dead-lettered; a transient failure is
   retried without leaving a dedupe mark behind, then dead-lettered after the last attempt.
"""

from __future__ import annotations

import io
import json
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import fastavro
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transcription_service.erasure_consumer import (
    COMPLETED,
    CONSUMER_GROUP,
    DLQ_TOPIC,
    ERASED_SPEAKER,
    MAX_ATTEMPTS,
    STORE,
    TOPIC,
    dedupe_key,
    handle_erasure_event,
    process_decoded_message,
)

pytestmark = pytest.mark.asyncio

# The proposed `admin-events-value` v2: v1 plus the `erasure_completed` symbol, appended.
_ENVELOPE = {
    "type": "record",
    "name": "EventEnvelopeHeader",
    "namespace": "io.nexus.events",
    "fields": [
        {"name": "event_id", "type": {"type": "string", "logicalType": "uuid"}},
        {"name": "idempotency_key", "type": "string"},
        {"name": "org_id", "type": {"type": "string", "logicalType": "uuid"}},
        {"name": "schema_version", "type": "int", "default": 1},
        {"name": "producer", "type": "string"},
        {"name": "occurred_at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "correlation_id", "type": ["null", "string"], "default": None},
        {"name": "causation_id", "type": ["null", "string"], "default": None},
    ],
}
ADMIN_EVENT_V2 = {
    "type": "record",
    "name": "AdminEvent",
    "namespace": "io.nexus.events",
    "fields": [
        {"name": "header", "type": _ENVELOPE},
        {
            "name": "event_type",
            "type": {
                "type": "enum",
                "name": "AdminEventType",
                "symbols": [
                    "flag_changed",
                    "tenant_suspended",
                    "erasure_requested",
                    "erasure_completed",
                ],
            },
        },
        {"name": "actor_id", "type": "string"},
        {"name": "target_type", "type": "string"},
        {"name": "target_id", "type": "string"},
        {"name": "detail_json", "type": "string"},
    ],
}


class _Dlq:
    def __init__(self) -> None:
        self.produced: list[dict[str, Any]] = []

    def produce(self, *, topic: str, key: bytes | None, value: bytes) -> None:
        self.produced.append({"topic": topic, "key": key, "value": json.loads(value)})

    def flush(self, timeout: float = 10) -> int:
        return 0


async def _no_sleep(_: float) -> None:
    return None


def _event(
    org_id: str,
    *,
    kind: str,
    target_id: str,
    event_type: str = "erasure_requested",
    erasure_id: str | None = None,
) -> dict[str, Any]:
    return {
        "header": {
            "event_id": str(uuid.uuid4()),
            "idempotency_key": f"admin:{event_type}:{kind}:{target_id}:{uuid.uuid4()}",
            "org_id": uuid.UUID(org_id),  # the Avro decoder yields a UUID for logicalType uuid
            "schema_version": 1,
            "producer": "admin-ops-service",
            "occurred_at": int(time.time() * 1_000_000),
            "correlation_id": None,
            "causation_id": None,
        },
        "event_type": event_type,
        "actor_id": str(uuid.uuid4()),
        "target_type": kind,
        "target_id": target_id,
        "detail_json": json.dumps({"erasure_id": erasure_id or str(uuid.uuid4()), "scope": kind}),
    }


@pytest_asyncio.fixture
async def other_org(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[str]:
    org_id = str(uuid.uuid4())
    async with session_factory() as session, session.begin():
        await session.execute(
            text("INSERT INTO organizations (id, name, slug) VALUES (:id, 'Other', :slug)"),
            {"id": org_id, "slug": f"other-{uuid.uuid4().hex[:12]}"},
        )
    try:
        yield org_id
    finally:
        async with session_factory() as session, session.begin():
            for table in ("transcript_chunks", "outbox"):
                await session.execute(
                    text(f"DELETE FROM {table} WHERE org_id = :o"), {"o": org_id}  # noqa: S608
                )
            await session.execute(text("DELETE FROM organizations WHERE id = :o"), {"o": org_id})


async def _chunk(
    factory: async_sessionmaker[AsyncSession], org: str, meeting: str, speaker: str, words: str
) -> None:
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO transcript_chunks "
                "(org_id, meeting_id, speaker_id, start_ms, end_ms, text, confidence) "
                "VALUES (:o, :m, :s, 0, 1000, :t, 0.9)"
            ),
            {"o": org, "m": meeting, "s": speaker, "t": words},
        )


async def _rows(factory: async_sessionmaker[AsyncSession], org: str) -> list[tuple[str, str, str]]:
    async with factory() as session:
        rows = await session.execute(
            text(
                "SELECT meeting_id::text, speaker_id, text FROM transcript_chunks "
                "WHERE org_id = :o ORDER BY text"
            ),
            {"o": org},
        )
        return [(r[0], r[1], r[2]) for r in rows]


async def _completions(factory: async_sessionmaker[AsyncSession], org: str) -> list[Any]:
    async with factory() as session:
        rows = await session.execute(
            text(
                "SELECT idempotency_key, schema_subject, partition_key, payload FROM outbox "
                "WHERE org_id = :o AND topic = :t"
            ),
            {"o": org, "t": TOPIC},
        )
        return list(rows)


def _encodes_against_v2(value: dict[str, Any]) -> None:
    fastavro.schemaless_writer(io.BytesIO(), fastavro.parse_schema(ADMIN_EVENT_V2), value)


async def test_meeting_erasure_deletes_only_that_meetings_rows_and_records_completion(
    session_factory, redis_client, tenant, other_org
):
    org, meeting = tenant["org_id"], tenant["meeting_id"]
    kept_meeting = str(uuid.uuid4())
    await _chunk(session_factory, org, meeting, "1", "a erased line")
    await _chunk(session_factory, org, meeting, str(uuid.uuid4()), "b erased line")
    await _chunk(session_factory, org, kept_meeting, "1", "c other meeting")
    await _chunk(session_factory, other_org, meeting, "1", "d other org, same meeting id")

    erasure_id = str(uuid.uuid4())
    event = _event(org, kind="meeting", target_id=meeting, erasure_id=erasure_id)
    assert await handle_erasure_event(session_factory, redis_client, event) == "applied"

    assert await _rows(session_factory, org) == [(kept_meeting, "1", "c other meeting")]
    assert await _rows(session_factory, other_org) == [
        (meeting, "1", "d other org, same meeting id")
    ]

    [(key, subject, partition_key, payload)] = await _completions(session_factory, org)
    assert key == f"{COMPLETED}:{erasure_id}:{STORE}"
    assert subject == "admin-events-value" and partition_key == org
    assert payload["event_type"] == COMPLETED
    assert payload["target_type"] == "meeting" and payload["target_id"] == meeting
    assert payload["header"]["org_id"] == org
    assert payload["header"]["idempotency_key"] == key
    assert json.loads(payload["detail_json"]) == {
        "erasure_id": erasure_id,
        "store": STORE,
        "service": "transcription-service",
        "counts": {"deleted": 2},
    }
    _encodes_against_v2(payload)
    assert await _completions(session_factory, other_org) == []


async def test_the_same_event_twice_is_a_no_op_with_and_without_the_redis_mark(
    session_factory, redis_client, tenant
):
    org, meeting = tenant["org_id"], tenant["meeting_id"]
    await _chunk(session_factory, org, meeting, "1", "gone")
    event = _event(org, kind="meeting", target_id=meeting)
    assert await handle_erasure_event(session_factory, redis_client, event) == "applied"

    # Rows written later for the same meeting must not be touched by a redelivery.
    await _chunk(session_factory, org, meeting, "1", "written after")
    assert await handle_erasure_event(session_factory, redis_client, event) == "duplicate"

    await redis_client.delete(dedupe_key(org, event["header"]["idempotency_key"]))
    assert await handle_erasure_event(session_factory, redis_client, event) == "duplicate"

    assert [r[2] for r in await _rows(session_factory, org)] == ["written after"]
    assert len(await _completions(session_factory, org)) == 1


async def test_person_erasure_removes_their_attribution_and_keeps_the_words(
    session_factory, redis_client, tenant, other_org
):
    org, meeting = tenant["org_id"], tenant["meeting_id"]
    person, colleague = str(uuid.uuid4()), str(uuid.uuid4())
    await _chunk(session_factory, org, meeting, person, "a their line")
    await _chunk(session_factory, org, str(uuid.uuid4()), person.upper(), "b their other line")
    await _chunk(session_factory, org, meeting, colleague, "c colleague")
    await _chunk(session_factory, org, meeting, "1", "d diarized")
    await _chunk(session_factory, other_org, meeting, person, "e other org")

    event = _event(org, kind="person", target_id=person)
    assert await handle_erasure_event(session_factory, redis_client, event) == "applied"

    rows = await _rows(session_factory, org)
    assert [(r[1], r[2]) for r in rows] == [
        (ERASED_SPEAKER, "a their line"),
        (ERASED_SPEAKER, "b their other line"),
        (colleague, "c colleague"),
        ("1", "d diarized"),
    ]
    assert [r[1] for r in await _rows(session_factory, other_org)] == [person]
    [(_, _, _, payload)] = await _completions(session_factory, org)
    assert json.loads(payload["detail_json"])["counts"] == {"anonymized": 2}
    assert payload["target_type"] == "person" and payload["target_id"] == person
    _encodes_against_v2(payload)

    assert await handle_erasure_event(session_factory, redis_client, event) == "duplicate"
    assert len(await _completions(session_factory, org)) == 1


async def test_other_admin_events_are_ignored(session_factory, redis_client, tenant):
    org = tenant["org_id"]
    for event in (
        _event(org, kind="meeting", target_id=str(uuid.uuid4()), event_type="flag_changed"),
        _event(org, kind="tenant", target_id=org),
        _event(org, kind="meeting", target_id=str(uuid.uuid4()), event_type=COMPLETED),
    ):
        assert await handle_erasure_event(session_factory, redis_client, event) == "ignored"
    assert await _completions(session_factory, org) == []


async def test_an_unusable_event_is_dead_lettered(session_factory, redis_client, tenant):
    dlq = _Dlq()
    bad_target = _event(tenant["org_id"], kind="meeting", target_id="not-a-uuid")
    no_org = _event(tenant["org_id"], kind="meeting", target_id=str(uuid.uuid4()))
    no_org["header"]["org_id"] = None
    for value in (bad_target, no_org, None):
        outcome = await process_decoded_message(
            session_factory, redis_client, value, raw_value=b"x", key=None, dlq_producer=dlq
        )
        assert outcome == "dlq"
    assert [p["topic"] for p in dlq.produced] == [DLQ_TOPIC] * 3
    assert {p["value"]["failure_class"] for p in dlq.produced} == {"BUSINESS_INVALID"}
    assert {p["value"]["consumer_group"] for p in dlq.produced} == {CONSUMER_GROUP}


class _Flaky:
    """A session factory that fails the first `failures` transactions, then delegates."""

    def __init__(self, real: async_sessionmaker[AsyncSession], failures: int) -> None:
        self.real, self.failures, self.calls = real, failures, 0

    def __call__(self) -> AsyncSession:
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError("postgres unavailable")
        return self.real()


async def test_a_transient_failure_is_retried_and_leaves_no_dedupe_mark(
    session_factory, redis_client, tenant
):
    org, meeting = tenant["org_id"], tenant["meeting_id"]
    await _chunk(session_factory, org, meeting, "1", "gone")
    event = _event(org, kind="meeting", target_id=meeting)
    flaky = _Flaky(session_factory, failures=2)
    outcome = await process_decoded_message(
        flaky,  # type: ignore[arg-type]
        redis_client,
        event,
        raw_value=b"x",
        key=None,
        dlq_producer=_Dlq(),
        sleep=_no_sleep,
    )
    assert outcome == "applied" and flaky.calls == 3
    assert await _rows(session_factory, org) == []

    always = _Flaky(session_factory, failures=99)
    dlq = _Dlq()
    other = _event(org, kind="meeting", target_id=str(uuid.uuid4()))
    outcome = await process_decoded_message(
        always,  # type: ignore[arg-type]
        redis_client,
        other,
        raw_value=b"x",
        key=None,
        dlq_producer=dlq,
        sleep=_no_sleep,
    )
    assert outcome == "dlq" and always.calls == MAX_ATTEMPTS
    assert dlq.produced[0]["value"]["failure_class"] == "MAX_RETRIES"
    assert not await redis_client.exists(dedupe_key(org, other["header"]["idempotency_key"]))
