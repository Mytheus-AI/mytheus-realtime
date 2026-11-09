"""`meeting_room_consumer`, two guarantees.

1. `handle_meeting_room_event`: redelivery of the SAME `meeting-room-events` fact (same
   `idempotency_key`) must never open a duplicate stream, per plan 3.6 step 8 / this repo's
   CLAUDE.md recipe "Handle a new meeting-room-events case" ("ensure idempotency so a
   redelivered event does not open a duplicate stream").
2. `process_decoded_message`: a message that cannot be safely applied goes to
   `meeting-room-events.dlq` and the caller returns normally, so `run_consumer` commits the
   offset and the partition keeps moving -- "never block the partition", the same contract
   `mytheus-backend`'s consumers hold.
"""

from __future__ import annotations

import uuid

import pytest

from transcription_service.meeting_room_consumer import (
    PoisonMessage,
    handle_meeting_room_event,
    process_decoded_message,
)
from transcription_service.providers.fake import FakeProvider
from transcription_service.sessions import SessionTracker


class _FakeDlqProducer:
    def __init__(self) -> None:
        self.produced: list[dict[str, object]] = []

    def produce(self, *, topic: str, key: bytes | None, value: bytes) -> None:
        self.produced.append({"topic": topic, "key": key, "value": value})

    def flush(self, timeout: float = 10) -> int:
        return 0


def _joined_event(*, org_id: str, meeting_id: str, idempotency_key: str) -> dict[str, object]:
    return {
        "header": {
            "event_id": str(uuid.uuid4()),
            "idempotency_key": idempotency_key,
            "org_id": org_id,
            "schema_version": 1,
            "producer": "meeting-service",
            "occurred_at": 0,
            "correlation_id": None,
            "causation_id": None,
        },
        "meeting_id": meeting_id,
        "room": f"meeting:{org_id}:{meeting_id}",
        "event_type": "joined",
        "participant_id": str(uuid.uuid4()),
    }


@pytest.mark.asyncio
async def test_redelivered_joined_event_does_not_reopen_the_stream(
    session_factory, redis_client, tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    provider = FakeProvider(script=[])
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)
    idempotency_key = f"meetingroom:{uuid.uuid4()}"
    event = _joined_event(org_id=org_id, meeting_id=meeting_id, idempotency_key=idempotency_key)

    first = await handle_meeting_room_event(tracker, redis_client, event)
    second = await handle_meeting_room_event(tracker, redis_client, dict(event))  # exact redelivery

    assert first is True
    assert second is False, "the dedupe cache must reject the redelivered idempotency_key"
    # `joined` opens the meeting-level session only; provider streams are opened per
    # participant audio connection (`/internal/audio`), never by the consumer.
    assert tracker.has_session(meeting_id)
    assert provider.opens == []


@pytest.mark.asyncio
async def test_recording_ready_is_acknowledged_but_has_no_session_effect(
    session_factory, redis_client, tenant: dict[str, str]
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    provider = FakeProvider(script=[])
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)
    event = _joined_event(
        org_id=org_id, meeting_id=meeting_id, idempotency_key=f"meetingroom:{uuid.uuid4()}"
    )
    event["event_type"] = "recording_ready"

    result = await handle_meeting_room_event(tracker, redis_client, event)
    assert result is False
    assert provider.opens == []


# The three poison-message tests below deliberately take no `tenant` fixture: a message that
# fails validation is rejected before any Postgres or Redis call, so requiring a seeded org
# would make them depend on infrastructure the path they cover never touches.


@pytest.mark.asyncio
async def test_missing_header_raises_poison_message(session_factory, redis_client) -> None:
    provider = FakeProvider(script=[])
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)
    event = {"meeting_id": str(uuid.uuid4()), "event_type": "joined"}  # no header

    with pytest.raises(PoisonMessage):
        await handle_meeting_room_event(tracker, redis_client, event)
    assert provider.opens == []


@pytest.mark.asyncio
async def test_poison_message_is_dead_lettered_and_not_reraised(
    session_factory, redis_client
) -> None:
    """The whole point of the DLQ path: `process_decoded_message` must RETURN on a poison
    message, not raise, so `run_consumer` goes on to commit the offset and the partition keeps
    moving instead of crash-looping on one bad message."""
    provider = FakeProvider(script=[])
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)
    event = {"meeting_id": str(uuid.uuid4()), "event_type": "joined"}  # no header
    dlq = _FakeDlqProducer()

    await process_decoded_message(
        tracker, redis_client, event, raw_value=b"raw", key=b"k", dlq_producer=dlq
    )

    assert len(dlq.produced) == 1
    assert dlq.produced[0]["topic"] == "meeting-room-events.dlq"
    assert provider.opens == []


@pytest.mark.asyncio
async def test_value_that_does_not_decode_to_a_record_is_dead_lettered(
    session_factory, redis_client
) -> None:
    """A tombstone decodes to `None`. Without the isinstance guard in
    `process_decoded_message` this raises `AttributeError` rather than `PoisonMessage`, which
    would escape the DLQ path and kill `run_consumer`'s loop -- and with it this pod's
    transcription -- instead of dead-lettering one message."""
    provider = FakeProvider(script=[])
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis_client)
    dlq = _FakeDlqProducer()

    await process_decoded_message(
        tracker, redis_client, None, raw_value=None, key=b"k", dlq_producer=dlq
    )

    assert len(dlq.produced) == 1
    assert dlq.produced[0]["topic"] == "meeting-room-events.dlq"


@pytest.mark.asyncio
async def test_avro_decoded_uuid_ids_open_the_session(
    session_factory, redis_client, tenant: dict[str, str]
) -> None:
    """The real `AvroDeserializer` decodes `logicalType: uuid` fields to `uuid.UUID`, not
    `str`. Passed straight to `set_config`, asyncpg refused it, the exception escaped the poll
    loop, and the consumer died for the life of the pod: no meeting opened a session again and
    every audio connection was closed. Ids are normalised to `str` at the boundary."""
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    tracker = SessionTracker(
        provider=FakeProvider(script=[]), session_factory=session_factory, redis=redis_client
    )
    event = _joined_event(
        org_id=org_id, meeting_id=meeting_id, idempotency_key=f"meetingroom:{uuid.uuid4()}"
    )
    event["header"]["org_id"] = uuid.UUID(org_id)  # type: ignore[index]
    event["meeting_id"] = uuid.UUID(meeting_id)

    assert await handle_meeting_room_event(tracker, redis_client, event) is True
    assert tracker.has_session(meeting_id)
