"""Transcript quality: the Deepgram request asks for Nova-3 multilingual with formatting, silence
is never saved, and everyone's lines share one meeting timeline."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from tests.helpers.fakes import FakeDatabase, FakeRedis
from tests.test_reconnect import FRAME, FlakyProvider, _u
from transcription_service.config import load_settings_from_env
from transcription_service.providers.deepgram import DeepgramProvider
from transcription_service.sessions import SessionTracker


@pytest.mark.asyncio
async def test_deepgram_is_asked_for_nova_3_multilingual_with_formatting() -> None:
    provider = DeepgramProvider(api_key="dg_test_key")
    with patch(
        "transcription_service.providers.deepgram.websockets.connect", new=AsyncMock()
    ) as connect:
        await provider.open_stream(org_id="o", meeting_id="m", region="us", diarize=False)
    query = parse_qs(urlparse(connect.call_args.args[0]).query)
    params = {k: v[0] for k, v in query.items()}
    assert params["model"] == "nova-3"
    assert params["language"] == "multi"
    assert params["punctuate"] == "true" and params["smart_format"] == "true"
    assert params["endpointing"] == "300" and params["utterance_end_ms"] == "1000"
    assert params["interim_results"] == "true"
    assert "dg_test_key" not in connect.call_args.args[0]


def test_model_and_language_can_be_changed_without_a_release(monkeypatch: Any) -> None:
    monkeypatch.setenv("DEEPGRAM_MODEL", "nova-3-medical")
    monkeypatch.setenv("DEEPGRAM_LANGUAGE", "en")
    settings = load_settings_from_env()
    assert (settings.deepgram_model, settings.deepgram_language) == ("nova-3-medical", "en")
    monkeypatch.delenv("DEEPGRAM_MODEL")
    monkeypatch.delenv("DEEPGRAM_LANGUAGE")
    defaults = load_settings_from_env()
    assert (defaults.deepgram_model, defaults.deepgram_language) == ("nova-3", "multi")


async def _tracker() -> tuple[FakeDatabase, FlakyProvider, SessionTracker, str, str]:
    org_id, meeting_id = str(uuid.uuid4()), str(uuid.uuid4())
    db = FakeDatabase()
    db.add_meeting(org_id, meeting_id, region="us")
    provider = FlakyProvider()
    tracker = SessionTracker(provider=provider, session_factory=db, redis=FakeRedis())  # type: ignore[arg-type]
    return db, provider, tracker, org_id, meeting_id


async def test_silence_is_never_saved_or_captioned() -> None:
    db, provider, tracker, org_id, meeting_id = await _tracker()
    lease = await tracker.attach_participant(
        org_id=org_id, meeting_id=meeting_id, participant_identity=str(uuid.uuid4())
    )
    await lease.stream.send_audio(FRAME)
    provider.streams[0].emit(_u(0, ""))
    provider.streams[0].emit(_u(100, "   "))
    provider.streams[0].emit(_u(200, "", final=False))
    provider.streams[0].emit(_u(300, "namaste everyone"))
    await asyncio.sleep(0.02)
    await tracker.detach_participant(lease)

    assert [r["text"] for r in db.transcript_chunks] == ["namaste everyone"]
    assert len(db.outbox) == 1


async def test_a_later_joiner_is_placed_later_on_the_meeting_timeline() -> None:
    db, provider, tracker, org_id, meeting_id = await _tracker()
    first = await tracker.attach_participant(
        org_id=org_id, meeting_id=meeting_id, participant_identity="u-first"
    )
    await asyncio.sleep(0.25)
    second = await tracker.attach_participant(
        org_id=org_id, meeting_id=meeting_id, participant_identity="u-second"
    )
    await first.stream.send_audio(FRAME)
    await second.stream.send_audio(FRAME)
    # Both providers report their own clock: each says "0 ms into my stream".
    provider.streams[0].emit(_u(0, "first speaker"))
    provider.streams[1].emit(_u(0, "second speaker"))
    await asyncio.sleep(0.02)
    await tracker.detach_participant(first)
    await tracker.detach_participant(second)

    starts = {r["speaker_id"]: r["start_ms"] for r in db.transcript_chunks}
    assert starts["u-first"] == 0
    assert starts["u-second"] >= 250, "the late joiner's line must not sit at 00:00"
