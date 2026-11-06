"""`/internal/audio` end to end over fakes: websocket in -> captions out -> final persisted ->
outbox row, plus every refusal path of the contract in `audio_in.py`'s docstring.

Runs with no infrastructure: `FakeDatabase`/`FakeRedis` (`tests/helpers/fakes.py`) stand in
for Postgres and Redis, `FakeProvider(emit_on_audio=True)` for Deepgram -- so the test proves
that the audio bytes pushed into the socket are what drive the captions and the final.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from helpers.fakes import FakeDatabase, FakeRedis
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from transcription_service import audio_in
from transcription_service.audio_in import AudioEndpointConfig, AudioHub
from transcription_service.main import build_app
from transcription_service.providers.base import Utterance
from transcription_service.providers.deepgram import DeepgramProvider
from transcription_service.providers.factory import UnavailableProvider
from transcription_service.providers.fake import FakeProvider
from transcription_service.sessions import SessionTracker

TOKEN = "test-internal-audio-token"
ALICE = str(uuid.uuid4())  # participant_identity is a users.id
BOB = str(uuid.uuid4())
FRAME = b"\x01\x00" * 320  # 20 ms of 16 kHz s16le mono


def _config(**overrides: Any) -> AudioEndpointConfig:
    values: dict[str, Any] = {
        "token": TOKEN,
        "init_timeout_seconds": 2.0,
        "idle_timeout_seconds": 5.0,
        "max_frame_bytes": 64 * 1024,
        "queue_max_frames": 100,
    }
    values.update(overrides)
    return AudioEndpointConfig(**values)


class Harness:
    def __init__(
        self,
        *,
        provider: Any = None,
        config: AudioEndpointConfig | None = None,
    ) -> None:
        self.org_id = str(uuid.uuid4())
        self.meeting_id = str(uuid.uuid4())
        self.db = FakeDatabase()
        self.db.add_meeting(self.org_id, self.meeting_id)
        self.redis = FakeRedis()
        self.provider = provider if provider is not None else FakeProvider(emit_on_audio=True)
        self.tracker = SessionTracker(
            provider=self.provider,
            session_factory=self.db,  # type: ignore[arg-type]
            redis=self.redis,  # type: ignore[arg-type]
        )
        self.hub = AudioHub(
            tracker=self.tracker,
            config=config or _config(),
            provider_mode=audio_in.provider_mode_for(self.provider),
        )
        self.app = build_app(lifespan=None)
        self.app.state.audio_hub = self.hub

    def init(self, **overrides: Any) -> str:
        body: dict[str, Any] = {
            "org_id": self.org_id,
            "meeting_id": self.meeting_id,
            "participant_identity": ALICE,
            "sample_rate": 16000,
            "encoding": "pcm_s16le",
        }
        body.update(overrides)
        return json.dumps(body)


@pytest.fixture
def harness() -> Iterator[Harness]:
    yield Harness()


def _headers(token: str | None = TOKEN) -> dict[str, str]:
    return {} if token is None else {"X-Internal-Auth": token}


def _wait_for(predicate: Callable[[], bool], timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not met in time")


def _expect_close(ws: Any) -> WebSocketDisconnect:
    with pytest.raises(WebSocketDisconnect) as info:
        ws.receive_text()
    return info.value


# ---------------------------------------------------------------- handshake / auth


@pytest.mark.parametrize("token", [None, "wrong-token", ""])
def test_missing_or_wrong_token_is_refused_before_accept(
    harness: Harness, token: str | None
) -> None:
    with TestClient(harness.app) as client:
        with pytest.raises(WebSocketDisconnect) as info:
            with client.websocket_connect("/internal/audio", headers=_headers(token)):
                pass
    assert info.value.code == audio_in.CLOSE_POLICY_VIOLATION
    assert harness.db.statements == [] and harness.provider.opens == []


def test_unset_server_token_fails_closed_even_with_a_header() -> None:
    h = Harness(config=_config(token=None))
    with TestClient(h.app) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/internal/audio", headers=_headers("anything")):
                pass
    assert h.provider.opens == []


def test_route_without_a_started_hub_never_accepts() -> None:
    app = build_app(lifespan=None)
    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/internal/audio", headers=_headers()):
                pass


# ---------------------------------------------------------------- init frame validation


@pytest.mark.parametrize(
    "init",
    [
        "not json",
        "[1, 2]",
        json.dumps({"meeting_id": str(uuid.uuid4())}),
    ],
)
def test_invalid_init_frame_closes_4400(harness: Harness, init: str) -> None:
    with TestClient(harness.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(init)
            assert _expect_close(ws).code == audio_in.CLOSE_BAD_INIT
    assert harness.provider.opens == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"org_id": "not-a-uuid"},
        {"participant_identity": ""},
        {"encoding": "opus"},
        {"sample_rate": True},
        {"sample_rate": 48000},
    ],
)
def test_init_field_validation(harness: Harness, overrides: dict[str, Any]) -> None:
    with TestClient(harness.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(harness.init(**overrides))
            assert _expect_close(ws).code == audio_in.CLOSE_BAD_INIT


def test_binary_before_init_closes_4400(harness: Harness) -> None:
    with TestClient(harness.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_bytes(FRAME)
            assert _expect_close(ws).code == audio_in.CLOSE_BAD_INIT


def test_init_timeout_closes_4408() -> None:
    h = Harness(config=_config(init_timeout_seconds=0.1))
    with TestClient(h.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            assert _expect_close(ws).code == audio_in.CLOSE_TIMEOUT


# ---------------------------------------------------------------- compliance gate


def test_compliance_gate_closed_drops_audio_and_emits_nothing() -> None:
    """A real (Deepgram) provider is selected: the gate is checked before ANY database read,
    audit write or provider contact, and the connection closes 4451. The gate constant stays
    False -- nothing here flips or bypasses it."""
    provider = DeepgramProvider(api_key="dg_test_key_never_used")
    h = Harness(provider=provider)
    assert h.hub.provider_mode == audio_in.PROVIDER_EXTERNAL
    with TestClient(h.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(h.init())
            ws.send_bytes(FRAME)
            close = _expect_close(ws)
    assert close.code == audio_in.CLOSE_COMPLIANCE_GATE
    assert "compliance gate" in (close.reason or "")
    assert h.db.statements == [], "no DB access while the gate is closed"
    assert h.redis.published == [], "no caption while the gate is closed"
    assert h.db.outbox == [] and h.db.transcript_chunks == [] and h.db.audit_logs == []
    assert not h.tracker.has_session(h.meeting_id)


def test_unknown_provider_types_are_gated_fail_closed() -> None:
    class SomeOtherProvider:
        async def open_stream(self, **kwargs: Any) -> Any:  # pragma: no cover
            raise AssertionError("must never be called")

    assert audio_in.provider_requires_compliance_gate(SomeOtherProvider()) is True  # type: ignore[arg-type]
    assert audio_in.provider_requires_compliance_gate(FakeProvider()) is False
    assert audio_in.provider_mode_for(UnavailableProvider()) == audio_in.PROVIDER_UNAVAILABLE


def test_no_provider_configured_fails_closed_before_any_db_access() -> None:
    """No DEEPGRAM_API_KEY and no TRANSCRIPTION_ALLOW_FAKE_PROVIDER=1: the fake is NOT used for
    a real meeting; the connection is refused 1011 and nothing is read, written or emitted."""
    h = Harness(provider=UnavailableProvider())
    with TestClient(h.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(h.init())
            ws.send_bytes(FRAME)
            close = _expect_close(ws)
    assert close.code == audio_in.CLOSE_INTERNAL_ERROR
    assert "no transcription provider" in (close.reason or "")
    assert h.db.statements == [] and h.redis.published == []


# ---------------------------------------------------------------- tenant isolation


def test_meeting_not_in_org_closes_4403(harness: Harness) -> None:
    other_org = str(uuid.uuid4())
    with TestClient(harness.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(harness.init(org_id=other_org))
            assert _expect_close(ws).code == audio_in.CLOSE_TENANT_FORBIDDEN
    assert harness.provider.opens == []
    assert harness.db.audit_logs == []


def test_session_of_another_org_cannot_be_attached(harness: Harness) -> None:
    """Defence in depth: even if two orgs both claimed the same meeting_id, the second org can
    never attach to (feed audio into, or read results from) the first org's open session."""
    intruder = str(uuid.uuid4())
    harness.db.add_meeting(intruder, harness.meeting_id)
    with TestClient(harness.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as owner:
            owner.send_text(harness.init())
            owner.send_bytes(FRAME)
            _wait_for(lambda: harness.tracker.has_session(harness.meeting_id))
            with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
                ws.send_text(harness.init(org_id=intruder))
                assert _expect_close(ws).code == audio_in.CLOSE_TENANT_FORBIDDEN
            owner.send_text(json.dumps({"type": "stop"}))
            assert _expect_close(owner).code == audio_in.CLOSE_NORMAL
    assert len(harness.provider.opens) == 1
    assert all(r["_rls_org"] == harness.org_id for r in harness.db.outbox)


# ---------------------------------------------------------------- the happy path, end to end


def test_websocket_in_captions_out_final_persisted_and_outboxed() -> None:
    chunk_id = str(uuid.uuid4())
    script = [
        Utterance(chunk_id, "0", 0, 400, "email me at", False, 0.4),
        Utterance(chunk_id, "0", 0, 900, "email me at jane@acme.com", False, 0.6),
        Utterance(chunk_id, "0", 0, 1200, "email me at jane@acme.com today", True, 0.97),
    ]
    h = Harness(provider=FakeProvider(script=script, emit_on_audio=True))
    with TestClient(h.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(h.init())
            for _ in range(3):
                ws.send_bytes(FRAME)
            _wait_for(lambda: len(h.db.outbox) == 1 and len(h.redis.published) == 3)
            ws.send_text(json.dumps({"type": "ping"}))  # ignored control frame, never audio
            ws.send_text(json.dumps({"type": "stop"}))
            close = _expect_close(ws)
    assert close.code == audio_in.CLOSE_NORMAL

    # Audio actually flowed, byte for byte, in order -- and control frames did not.
    stream = h.provider.streams[0]
    assert stream.frames == [FRAME, FRAME, FRAME]
    assert h.provider.opens[0].region == "us"
    assert h.provider.opens[0].diarize is False, "one speaker per track: no diarization"
    assert (h.provider.opens[0].encoding, h.provider.opens[0].sample_rate) == ("linear16", 16000)

    # Captions out on the canonical channel: interims raw, final redacted, in order.
    channel = f"nexus:{h.org_id}:pubsub:meeting:{h.meeting_id}"
    assert {c for c, _ in h.redis.published} == {channel}
    captions = [json.loads(m)["payload"] for _, m in h.redis.published]
    assert [c["isFinal"] for c in captions] == [False, False, True]
    assert {c["speakerId"] for c in captions} == {ALICE}
    assert captions[1]["text"] == "email me at jane@acme.com"
    assert captions[2]["text"] == "email me at ‹EMAIL› today"

    # Exactly one final persisted, redacted, under the tenant's RLS context.
    assert len(h.db.transcript_chunks) == 1
    row = h.db.transcript_chunks[0]
    assert row["id"] == chunk_id and row["text"] == "email me at ‹EMAIL› today"
    assert row["redacted"] is True and row["_rls_org"] == h.org_id
    assert row["speaker_id"] == ALICE, "attributed to the connection's participant"

    # Exactly one outbox row for the transcript-chunks topic.
    (outbox,) = h.db.outbox
    assert outbox["topic"] == "transcript-chunks"
    assert outbox["idempotency_key"] == f"chunk:{chunk_id}"
    assert outbox["partition_key"] == f"{h.org_id}:{h.meeting_id}"
    assert outbox["_rls_org"] == h.org_id
    payload = json.loads(outbox["payload"])
    assert payload["is_final"] is True and payload["text"] == "email me at ‹EMAIL› today"
    assert payload["speaker_id"] == ALICE

    # Session opened by the audio connection wrote its audit entry and closed on detach.
    assert len(h.db.audit_logs) == 1
    assert not h.tracker.has_session(h.meeting_id)
    assert stream.closed is True
    assert h.hub.active_connections == 0


def test_attaches_to_a_session_the_consumer_already_opened(harness: Harness) -> None:
    with TestClient(harness.app) as client:
        opened = client.portal.call(  # type: ignore[union-attr]
            lambda: harness.tracker.on_participant_joined(
                org_id=harness.org_id, meeting_id=harness.meeting_id
            )
        )
        assert opened is True and harness.provider.opens == [], "hints open no stream"
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(harness.init())
            ws.send_bytes(FRAME)
            _wait_for(lambda: len(harness.provider.streams[0].frames) == 1)
            ws.send_text(json.dumps({"type": "stop"}))
            _expect_close(ws)
        assert len(harness.provider.opens) == 1
        assert len(harness.db.audit_logs) == 1, "the meeting session was reused, not reopened"
        # The last participant connection ended: the meeting session closed with it.
        assert not harness.tracker.has_session(harness.meeting_id)
        left = client.portal.call(  # type: ignore[union-attr]
            lambda: harness.tracker.on_participant_left(
                org_id=harness.org_id, meeting_id=harness.meeting_id
            )
        )
    assert left is False, "a late left hint for a closed meeting is a safe no-op"


# ---------------------------------------------------------------- frame limits, timeouts


def test_oversized_frame_closes_1009() -> None:
    h = Harness(config=_config(max_frame_bytes=1024))
    with TestClient(h.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(h.init())
            ws.send_bytes(b"\x00" * 1026)
            assert _expect_close(ws).code == audio_in.CLOSE_TOO_BIG
    assert not h.tracker.has_session(h.meeting_id)


def test_odd_length_frame_closes_1007(harness: Harness) -> None:
    with TestClient(harness.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(harness.init())
            ws.send_bytes(b"\x00\x01\x02")
            assert _expect_close(ws).code == audio_in.CLOSE_INVALID_PAYLOAD


def test_idle_timeout_closes_4408_and_releases_the_session() -> None:
    h = Harness(config=_config(idle_timeout_seconds=0.2))
    with TestClient(h.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(h.init())
            ws.send_bytes(FRAME)
            assert _expect_close(ws).code == audio_in.CLOSE_TIMEOUT
    assert not h.tracker.has_session(h.meeting_id)


def test_backpressure_drops_oldest_and_stays_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(audio_in, "_STOP_FLUSH_TIMEOUT_SECONDS", 0.1)

    class StuckStream:
        def __init__(self) -> None:
            self.sent: list[bytes] = []

        async def utterances(self) -> Any:
            if False:  # pragma: no cover
                yield

        async def send_audio(self, frame: bytes) -> None:
            self.sent.append(frame)
            await asyncio.Event().wait()  # the provider never catches up

        async def close(self) -> None:
            return None

    class StuckProvider(FakeProvider):
        async def open_stream(self, **kwargs: Any) -> Any:
            self.opens.append(kwargs)  # type: ignore[arg-type]
            return StuckStream()

    h = Harness(provider=StuckProvider(), config=_config(queue_max_frames=2))
    with TestClient(h.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(h.init())
            for i in range(10):
                ws.send_bytes(bytes([i, 0]) * 8)
            _wait_for(lambda: h.hub.active_connections == 1)
            ws.send_text(json.dumps({"type": "stop"}))
            assert _expect_close(ws).code == audio_in.CLOSE_NORMAL
        _wait_for(lambda: len(h.hub.finished) == 1)
    stats = h.hub.finished[0]
    assert stats.frames_received == 10
    # One frame in flight at the stuck provider, at most two queued; the rest dropped.
    assert 7 <= stats.frames_dropped <= 8
    assert not h.tracker.has_session(h.meeting_id)


# ---------------------------------------------------------------- supersede, shutdown


def _stop(ws: Any) -> int:
    ws.send_text(json.dumps({"type": "stop"}))
    return _expect_close(ws).code


def test_two_participants_stream_concurrently_with_correct_attribution(harness: Harness) -> None:
    with TestClient(harness.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as alice:
            alice.send_text(harness.init(participant_identity=ALICE))
            with client.websocket_connect("/internal/audio", headers=_headers()) as bob:
                bob.send_text(harness.init(participant_identity=BOB))
                for _ in range(3):
                    alice.send_bytes(FRAME)
                    bob.send_bytes(b"\x02\x00" * 160)  # ~10 ms frames, like the real sender
                _wait_for(lambda: len(harness.db.transcript_chunks) == 2)
                _wait_for(lambda: len(harness.redis.published) == 6)
                assert harness.tracker.participant_count(harness.meeting_id) == 2
                assert _stop(bob) == audio_in.CLOSE_NORMAL
            # Bob leaving leaves Alice running on the same meeting session.
            assert harness.tracker.has_session(harness.meeting_id)
            assert harness.tracker.participant_count(harness.meeting_id) == 1
            assert harness.provider.streams[0].closed is False
            alice.send_bytes(FRAME)
            _wait_for(lambda: len(harness.provider.streams[0].frames) == 4)
            assert _stop(alice) == audio_in.CLOSE_NORMAL
    # The last participant closing ends the meeting session.
    assert not harness.tracker.has_session(harness.meeting_id)
    assert len(harness.provider.opens) == 2, "one provider stream per participant connection"
    assert len(harness.db.audit_logs) == 1, "one meeting session for both participants"

    captions = [json.loads(m)["payload"] for _, m in harness.redis.published]
    assert sorted(c["speakerId"] for c in captions) == sorted([ALICE] * 3 + [BOB] * 3)
    by_speaker = {r["speaker_id"]: r for r in harness.db.transcript_chunks}
    assert set(by_speaker) == {ALICE, BOB}
    assert len({r["id"] for r in harness.db.transcript_chunks}) == 2
    assert len(harness.db.outbox) == 2
    assert all(r["_rls_org"] == harness.org_id for r in harness.db.outbox)
    alice_stream, bob_stream = harness.provider.streams
    assert alice_stream.frames == [FRAME] * 4
    assert bob_stream.frames == [b"\x02\x00" * 160] * 3


def test_participant_reconnect_supersedes_only_its_own_old_connection(harness: Harness) -> None:
    with TestClient(harness.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as bob:
            bob.send_text(harness.init(participant_identity=BOB))
            bob.send_bytes(FRAME)
            with client.websocket_connect("/internal/audio", headers=_headers()) as old:
                old.send_text(harness.init(participant_identity=ALICE))
                old.send_bytes(FRAME)
                _wait_for(lambda: harness.tracker.participant_count(harness.meeting_id) == 2)
                with client.websocket_connect("/internal/audio", headers=_headers()) as new:
                    new.send_text(harness.init(participant_identity=ALICE))  # same participant
                    assert _expect_close(old).code == audio_in.CLOSE_SUPERSEDED
                    new.send_bytes(FRAME)
                    _wait_for(lambda: len(harness.provider.streams) == 3)
                    _wait_for(lambda: len(harness.provider.streams[2].frames) == 1)
                    old_stream = harness.provider.streams[1]
                    assert old_stream.closed is True, "the superseded stream is closed"
                    assert harness.provider.streams[0].closed is False, "Bob is untouched"
                    assert harness.tracker.participant_count(harness.meeting_id) == 2
                    bob.send_bytes(FRAME)
                    _wait_for(lambda: len(harness.provider.streams[0].frames) == 2)
                    assert _stop(new) == audio_in.CLOSE_NORMAL
            assert harness.tracker.has_session(harness.meeting_id)
            assert _stop(bob) == audio_in.CLOSE_NORMAL
    assert not harness.tracker.has_session(harness.meeting_id)
    assert len(harness.db.audit_logs) == 1


def test_shutdown_drain_closes_live_connections_with_1001(harness: Harness) -> None:
    with TestClient(harness.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(harness.init())
            ws.send_bytes(FRAME)
            _wait_for(lambda: harness.hub.active_connections == 1)
            _wait_for(lambda: harness.tracker.has_session(harness.meeting_id))
            client.portal.call(harness.hub.drain, 2.0)  # type: ignore[union-attr]
            assert _expect_close(ws).code == audio_in.CLOSE_GOING_AWAY
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/internal/audio", headers=_headers()):
                pass
    assert harness.hub.active_connections == 0


def test_db_failure_during_tenant_check_closes_1011(harness: Harness) -> None:
    harness.db.fail_next = RuntimeError("db down")
    with TestClient(harness.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(harness.init())
            assert _expect_close(ws).code == audio_in.CLOSE_INTERNAL_ERROR
    assert harness.provider.opens == []


def test_transcript_text_and_audio_are_never_logged_at_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret_text = "the quarterly number is 4815162342"
    chunk_id = str(uuid.uuid4())
    h = Harness(
        provider=FakeProvider(
            script=[Utterance(chunk_id, "0", 0, 100, secret_text, True, 0.9)],
            emit_on_audio=True,
        )
    )
    caplog.set_level("INFO")
    with TestClient(h.app) as client:
        with client.websocket_connect("/internal/audio", headers=_headers()) as ws:
            ws.send_text(h.init())
            ws.send_bytes(FRAME)
            _wait_for(lambda: len(h.db.outbox) == 1)
            ws.send_text(json.dumps({"type": "stop"}))
            _expect_close(ws)
    for record in caplog.records:
        message = record.getMessage()
        assert secret_text not in message
        assert repr(FRAME) not in message
        assert TOKEN not in message


async def test_concurrent_join_and_audio_attach_open_exactly_one_stream() -> None:
    org_id, meeting_id = str(uuid.uuid4()), str(uuid.uuid4())
    db = FakeDatabase()
    db.add_meeting(org_id, meeting_id)
    provider = FakeProvider(script=[])
    tracker = SessionTracker(provider=provider, session_factory=db, redis=None)  # type: ignore[arg-type]
    await asyncio.gather(
        tracker.on_participant_joined(org_id=org_id, meeting_id=meeting_id),
        tracker.attach_participant(
            org_id=org_id, meeting_id=meeting_id, participant_identity=ALICE
        ),
        tracker.attach_participant(org_id=org_id, meeting_id=meeting_id, participant_identity=BOB),
    )
    assert len(db.audit_logs) == 1, "exactly one meeting session opened"
    assert len(provider.opens) == 2, "one stream per participant"
    assert tracker._locks == {}, "per-meeting locks are released, not leaked"
