"""Smoke test: transcription-service boots, answers /healthz, and accepts the audio-hook WS."""

from __future__ import annotations

from starlette.testclient import TestClient

from transcription_service.main import app


def test_healthz() -> None:
    resp = TestClient(app).get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "service": "transcription-service"}


def test_internal_audio_ws_accepts() -> None:
    with TestClient(app).websocket_connect("/internal/audio"):
        # Scaffold: the server accepts then closes; a clean handshake is the contract we assert.
        pass
