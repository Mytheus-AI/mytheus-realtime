"""`meeting_room_consumer.handle_meeting_room_event`: redelivery of the SAME `meeting-room-events`
fact (same `idempotency_key`) must never open a duplicate stream, per plan 3.6 step 8 / this
repo's CLAUDE.md recipe "Handle a new meeting-room-events case" ("ensure idempotency so a
redelivered event does not open a duplicate stream")."""

from __future__ import annotations

import uuid

import pytest

from transcription_service.meeting_room_consumer import handle_meeting_room_event
from transcription_service.providers.fake import FakeProvider
from transcription_service.sessions import SessionTracker


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
    assert len(provider.opens) == 1


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
