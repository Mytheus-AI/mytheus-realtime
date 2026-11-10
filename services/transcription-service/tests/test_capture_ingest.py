"""`/capture/audio` (the Mytheus Capture browser extension) over fakes: the capture token,
every refusal path, and the chunks a captured call emits -- mic attributed to the user,
tab audio diarized into `remote-N` speakers -- through the same pipeline as a Mytheus room.

No infrastructure: `FakeDatabase`/`FakeRedis` for Postgres and Redis, `FakeProvider` for
Deepgram (and a patched `websockets.connect` for the one test that checks the real Deepgram
request carries the org's key terms).
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlparse

import pytest
from helpers.fakes import FakeDatabase, FakeRedis
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from transcription_service import audio_in, capture_in
from transcription_service.audio_in import AudioEndpointConfig
from transcription_service.capture_auth import (
    CaptureTokenError,
    sign_capture_token,
    verify_capture_token,
)
from transcription_service.capture_in import CaptureHub, parse_allowed_origins
from transcription_service.main import build_app
from transcription_service.org_context import OrgKeyterms
from transcription_service.providers.base import Utterance
from transcription_service.providers.deepgram import DeepgramProvider
from transcription_service.providers.factory import UnavailableProvider
from transcription_service.providers.fake import FakeProvider
from transcription_service.sessions import SessionTracker

SECRET = "capture-secret-" + "x" * 32
EXTENSION_ORIGIN = "chrome-extension://abcdefghijklmnopabcdefghijklmnop"
FRAME = b"\x01\x00" * 800  # 50 ms of 16 kHz s16le mono, the extension's frame size


def _claims(**overrides: Any) -> dict[str, Any]:
    now = int(time.time())
    claims: dict[str, Any] = {
        "typ": "capture",
        "iss": "mytheus-ui",
        "aud": "transcription-service",
        "sub": str(uuid.uuid4()),
        "org": str(uuid.uuid4()),
        "mid": str(uuid.uuid4()),
        "src": "meet",
        "consent_at": "2026-11-11T09:30:00.000Z",
        "iat": now,
        "exp": now + 900,
    }
    claims.update(overrides)
    return claims


class Harness:
    def __init__(
        self,
        *,
        provider: Any = None,
        secret: str | None = SECRET,
        allowed_origins: str | None = None,
        init_timeout: float = 2.0,
    ) -> None:
        self.user_id = str(uuid.uuid4())
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
        self.hub = CaptureHub(
            tracker=self.tracker,
            config=AudioEndpointConfig(
                token=None,  # the internal shared secret plays no part in capture
                init_timeout_seconds=init_timeout,
                idle_timeout_seconds=5.0,
                max_frame_bytes=64 * 1024,
                queue_max_frames=100,
            ),
            provider_mode=audio_in.provider_mode_for(self.provider),
            secret=secret,
            allowed_origins=parse_allowed_origins(allowed_origins),
        )
        self.app = build_app(lifespan=None)
        self.app.state.capture_hub = self.hub

    def token(self, **overrides: Any) -> str:
        claims = _claims(sub=self.user_id, org=self.org_id, mid=self.meeting_id)
        claims.update(overrides)
        return sign_capture_token(claims, SECRET)

    def init(self, channel: str = "mic", token: str | None = None, **overrides: Any) -> str:
        body: dict[str, Any] = {
            "type": "init",
            "token": self.token() if token is None else token,
            "channel": channel,
            "sample_rate": 16000,
            "encoding": "pcm_s16le",
        }
        body.update(overrides)
        return json.dumps(body)


def _wait_for(predicate: Callable[[], bool], timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition not met in time")


def _expect_close(ws: Any) -> WebSocketDisconnect:
    with pytest.raises(WebSocketDisconnect) as info:
        while True:
            ws.receive_text()
    return info.value


def _ready(ws: Any) -> dict[str, Any]:
    msg: dict[str, Any] = json.loads(ws.receive_text())
    assert msg["type"] == "ready"
    return msg


# ---------------------------------------------------------------- the capture token


def test_a_token_signed_with_the_secret_verifies_to_its_claims() -> None:
    raw = _claims(src="teams")
    claims = verify_capture_token(sign_capture_token(raw, SECRET), SECRET)
    ids = (claims.user_id, claims.org_id, claims.meeting_id)
    assert ids == (raw["sub"], raw["org"], raw["mid"])
    assert claims.source == "teams" and claims.consent_at == raw["consent_at"]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"typ": "capture-link"}, "not a capture token"),
        ({"aud": "realtime-gateway"}, "not issued for this service"),
        ({"iss": "someone-else"}, "not issued for this service"),
        ({"exp": int(time.time()) - 120, "iat": int(time.time()) - 900}, "expired"),
        ({"iat": int(time.time()) + 600, "exp": int(time.time()) + 900}, "not valid yet"),
        ({"exp": int(time.time()) + 7200}, "lifetime"),
        ({"src": "webex"}, "src"),
        ({"sub": "not-a-uuid"}, "sub must be a UUID"),
        ({"org": None}, "org is required"),
        ({"consent_at": "yesterday"}, "consent_at"),
        ({"consent_at": None}, "consent_at is required"),
    ],
)
def test_invalid_claims_are_refused(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(CaptureTokenError, match=message):
        verify_capture_token(sign_capture_token(_claims(**overrides), SECRET), SECRET)


def test_a_token_signed_with_another_secret_is_refused() -> None:
    token = sign_capture_token(_claims(), "another-secret-" + "y" * 32)
    with pytest.raises(CaptureTokenError, match="signature"):
        verify_capture_token(token, SECRET)


def test_alg_none_and_tampered_payloads_are_refused() -> None:
    header, payload, signature = sign_capture_token(_claims(), SECRET).split(".")
    none_header = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0"  # {"alg":"none","typ":"JWT"}
    with pytest.raises(CaptureTokenError, match="algorithm"):
        verify_capture_token(f"{none_header}.{payload}.", SECRET)
    other = sign_capture_token(_claims(), SECRET).split(".")[1]
    with pytest.raises(CaptureTokenError, match="signature"):
        verify_capture_token(f"{header}.{other}.{signature}", SECRET)
    with pytest.raises(CaptureTokenError):
        verify_capture_token("a.b", SECRET)


@pytest.mark.parametrize("secret", [None, "", "too-short"])
def test_an_unusable_secret_refuses_everything(secret: str | None) -> None:
    with pytest.raises(CaptureTokenError, match="not configured"):
        verify_capture_token(sign_capture_token(_claims(), "too-short"), secret)


# ---------------------------------------------------------------- handshake / auth


def test_unconfigured_capture_refuses_before_accept() -> None:
    h = Harness(secret=None)
    with TestClient(h.app) as client:
        with pytest.raises(WebSocketDisconnect) as info:
            with client.websocket_connect("/capture/audio"):
                pass
    assert info.value.code == audio_in.CLOSE_POLICY_VIOLATION
    assert h.db.statements == [] and h.provider.opens == []


def test_origin_allowlist_refuses_other_origins_before_accept() -> None:
    h = Harness(allowed_origins=f"{EXTENSION_ORIGIN}, chrome-extension://other")
    with TestClient(h.app) as client:
        with pytest.raises(WebSocketDisconnect) as info:
            with client.websocket_connect(
                "/capture/audio", headers={"origin": "https://evil.example"}
            ):
                pass
        assert info.value.code == audio_in.CLOSE_POLICY_VIOLATION
        with client.websocket_connect("/capture/audio", headers={"origin": EXTENSION_ORIGIN}) as ws:
            ws.send_text(h.init())
            assert _ready(ws)["channel"] == "mic"
            ws.send_text(json.dumps({"type": "stop"}))
            assert _expect_close(ws).code == audio_in.CLOSE_NORMAL


def test_route_without_a_started_hub_never_accepts() -> None:
    app = build_app(lifespan=None)
    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/capture/audio"):
                pass


@pytest.mark.parametrize(
    "token_fn",
    [
        lambda h: "",
        lambda h: "garbage",
        lambda h: h.token(exp=int(time.time()) - 300, iat=int(time.time()) - 1000),
        lambda h: sign_capture_token(_claims(), "another-secret-" + "y" * 32),
    ],
)
def test_missing_invalid_or_expired_token_closes_4401(token_fn: Callable[[Harness], str]) -> None:
    h = Harness()
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as ws:
            ws.send_text(h.init(token=token_fn(h)))
            assert _expect_close(ws).code == capture_in.CLOSE_UNAUTHORIZED
    assert h.db.statements == [] and h.provider.opens == []
    assert h.db.audit_logs == []


@pytest.mark.parametrize(
    "init",
    [
        "not json",
        json.dumps({"type": "hello"}),
        json.dumps({"type": "init", "token": "t", "channel": "screen"}),
    ],
)
def test_malformed_init_closes_4400(init: str) -> None:
    h = Harness()
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as ws:
            ws.send_text(init)
            assert _expect_close(ws).code == audio_in.CLOSE_BAD_INIT


@pytest.mark.parametrize("overrides", [{"sample_rate": 48000}, {"encoding": "opus"}])
def test_unsupported_audio_format_closes_4400(overrides: dict[str, Any]) -> None:
    h = Harness()
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as ws:
            ws.send_text(h.init(**overrides))
            assert _expect_close(ws).code == audio_in.CLOSE_BAD_INIT


def test_init_timeout_closes_4408() -> None:
    h = Harness(init_timeout=0.1)
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as ws:
            assert _expect_close(ws).code == audio_in.CLOSE_TIMEOUT


def test_a_meeting_of_another_org_is_refused_and_records_no_consent() -> None:
    h = Harness()
    other_org = str(uuid.uuid4())
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as ws:
            ws.send_text(h.init(token=h.token(org=other_org)))
            assert _expect_close(ws).code == audio_in.CLOSE_TENANT_FORBIDDEN
    assert h.db.audit_logs == [] and h.provider.opens == []


def test_no_provider_configured_closes_1011_before_any_database_access() -> None:
    h = Harness(provider=UnavailableProvider())
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as ws:
            ws.send_text(h.init())
            assert _expect_close(ws).code == audio_in.CLOSE_INTERNAL_ERROR
    assert h.db.statements == []


@pytest.mark.usefixtures("gate_closed")
def test_compliance_gate_closed_drops_capture_audio() -> None:
    h = Harness(provider=DeepgramProvider(api_key="dg"))
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as ws:
            ws.send_text(h.init())
            assert _expect_close(ws).code == audio_in.CLOSE_COMPLIANCE_GATE
    assert h.db.statements == []


def test_consent_that_cannot_be_recorded_never_streams() -> None:
    h = Harness()
    h.db.fail_next = RuntimeError("db down")
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as ws:
            ws.send_text(h.init())
            assert _expect_close(ws).code == audio_in.CLOSE_INTERNAL_ERROR
    assert h.provider.opens == []


# ---------------------------------------------------------------- chunk emission


def test_mic_channel_emits_chunks_as_the_user_and_records_consent() -> None:
    chunk_id = str(uuid.uuid4())
    script = [
        Utterance(chunk_id, "0", 0, 500, "let's ship", False, 0.5),
        Utterance(chunk_id, "0", 0, 1100, "let's ship it on Friday", True, 0.95),
    ]
    h = Harness(provider=FakeProvider(script=script, emit_on_audio=True))
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as ws:
            ws.send_text(h.init("mic"))
            assert _ready(ws) == {"type": "ready", "meeting_id": h.meeting_id, "channel": "mic"}
            ws.send_bytes(FRAME)
            ws.send_bytes(FRAME)
            _wait_for(lambda: len(h.db.outbox) == 1)
            ws.send_text(json.dumps({"type": "stop"}))
            assert _expect_close(ws).code == audio_in.CLOSE_NORMAL

    assert h.provider.streams[0].frames == [FRAME, FRAME]
    assert h.provider.opens[0].diarize is False, "the mic is one speaker: the user"

    (row,) = h.db.transcript_chunks
    assert row["meeting_id"] == h.meeting_id and row["_rls_org"] == h.org_id
    assert row["speaker_id"] == h.user_id
    (outbox,) = h.db.outbox
    assert outbox["topic"] == "transcript-chunks"
    assert outbox["partition_key"] == f"{h.org_id}:{h.meeting_id}"
    payload = json.loads(outbox["payload"])
    assert payload["meeting_id"] == h.meeting_id and payload["speaker_id"] == h.user_id
    assert payload["is_final"] is True and payload["text"] == "let's ship it on Friday"

    channel = f"nexus:{h.org_id}:pubsub:meeting:{h.meeting_id}"
    assert {c for c, _ in h.redis.published} == {channel}

    consent = [a for a in h.db.audit_logs if a["action"] == "transcription.capture_consent"]
    assert len(consent) == 1
    assert consent[0]["actor_id"] == h.user_id and consent[0]["_rls_org"] == h.org_id
    assert json.loads(consent[0]["metadata"]) == {
        "source": "meet",
        "channel": "mic",
        "participants_told_at": "2026-11-11T09:30:00.000Z",
    }
    assert not h.tracker.has_session(h.meeting_id), "the session closes with its last channel"


def test_tab_channel_is_diarized_into_remote_speakers() -> None:
    script = [
        Utterance(str(uuid.uuid4()), "0", 0, 900, "Can we move the launch?", True, 0.9),
        Utterance(str(uuid.uuid4()), "1", 900, 1800, "Only if legal signs off.", True, 0.9),
    ]
    h = Harness(provider=FakeProvider(script=script, emit_on_audio=True))
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as ws:
            ws.send_text(h.init("tab", token=h.token(src="zoom")))
            assert _ready(ws)["channel"] == "tab"
            ws.send_bytes(FRAME)
            ws.send_bytes(FRAME)
            _wait_for(lambda: len(h.db.transcript_chunks) == 2)
            ws.send_text(json.dumps({"type": "stop"}))
            assert _expect_close(ws).code == audio_in.CLOSE_NORMAL

    assert h.provider.opens[0].diarize is True
    assert [r["speaker_id"] for r in h.db.transcript_chunks] == ["remote-1", "remote-2"]
    payloads = [json.loads(o["payload"]) for o in h.db.outbox]
    assert [p["speaker_id"] for p in payloads] == ["remote-1", "remote-2"]
    assert {p["meeting_id"] for p in payloads} == {h.meeting_id}
    (consent,) = [a for a in h.db.audit_logs if a["action"] == "transcription.capture_consent"]
    assert json.loads(consent["metadata"])["source"] == "zoom"


def test_mic_and_tab_run_side_by_side_in_one_meeting_session() -> None:
    h = Harness(provider=FakeProvider(emit_on_audio=True))
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as mic:
            mic.send_text(h.init("mic"))
            _ready(mic)
            with client.websocket_connect("/capture/audio") as tab:
                tab.send_text(h.init("tab"))
                _ready(tab)
                assert h.tracker.participant_count(h.meeting_id) == 2
                tab.send_text(json.dumps({"type": "stop"}))
                assert _expect_close(tab).code == audio_in.CLOSE_NORMAL
            assert h.tracker.participant_count(h.meeting_id) == 1, "the mic is untouched"
            mic.send_text(json.dumps({"type": "stop"}))
            assert _expect_close(mic).code == audio_in.CLOSE_NORMAL
    assert not h.tracker.has_session(h.meeting_id)


def test_a_reconnecting_channel_supersedes_only_itself() -> None:
    h = Harness(provider=FakeProvider(emit_on_audio=True))
    with TestClient(h.app) as client:
        with client.websocket_connect("/capture/audio") as old:
            old.send_text(h.init("tab"))
            _ready(old)
            with client.websocket_connect("/capture/audio") as new:
                new.send_text(h.init("tab"))
                _ready(new)
                assert _expect_close(old).code == audio_in.CLOSE_SUPERSEDED
                new.send_text(json.dumps({"type": "stop"}))
                assert _expect_close(new).code == audio_in.CLOSE_NORMAL


async def test_the_org_vocabulary_reaches_deepgram_for_a_captured_call() -> None:
    """ADR-0039 D1 holds for captured calls: the tab and mic streams open through the same
    `DeepgramProvider`, so the org's key terms ride the Deepgram request."""

    class _Fetcher:
        async def fetch(self, org_id: str) -> Any:
            from transcription_service.org_context import OrgContext

            return OrgContext(products=["Mytheus Brain"], customers=["Acme"])

    provider = DeepgramProvider(api_key="dg", keyterms=OrgKeyterms(_Fetcher()))
    db = FakeDatabase()
    org_id, meeting_id = str(uuid.uuid4()), str(uuid.uuid4())
    db.add_meeting(org_id, meeting_id)
    tracker = SessionTracker(provider=provider, session_factory=db, redis=None)  # type: ignore[arg-type]
    with patch(
        "transcription_service.providers.deepgram.websockets.connect", new=AsyncMock()
    ) as connect:
        lease = await tracker.attach_participant(
            org_id=org_id,
            meeting_id=meeting_id,
            participant_identity="capture-tab:u",
            diarize=True,
            speaker_prefix="remote-",
        )
        await tracker.detach_participant(lease)
    query = parse_qs(urlparse(connect.call_args.args[0]).query)
    assert query["keyterm"] == ["Mytheus Brain", "Acme"]
    assert query["diarize"] == ["true"]
