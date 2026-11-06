"""Smoke tests: `/healthz` is cheap liveness, `/readyz` reports dependency state, and the audio
hook never accepts on a process whose lifespan has not started it.

The old `test_internal_audio_ws_accepts` asserted that `/internal/audio` accepted an
unauthenticated handshake -- true of the scaffold stub, and exactly the behaviour the real
endpoint must NOT have. Its replacement is the last test here; the full contract is covered
by `tests/test_internal_audio.py`.
"""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from transcription_service.main import app, build_app


def test_healthz() -> None:
    resp = TestClient(app).get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "service": "transcription-service"}


def _app_with_checks(**results: object) -> TestClient:
    application = build_app(lifespan=None)

    def _check(value: object):  # type: ignore[no-untyped-def]
        async def run() -> bool:
            if isinstance(value, Exception):
                raise value
            return bool(value)

        return run

    application.state.readiness = {name: _check(v) for name, v in results.items()}
    return TestClient(application)


def test_readyz_all_dependencies_ok() -> None:
    resp = _app_with_checks(db=True, redis=True, broker=True).get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ready",
        "service": "transcription-service",
        "checks": {"db": "ok", "redis": "ok", "broker": "ok"},
    }


@pytest.mark.parametrize(
    "results,failing",
    [
        ({"db": ConnectionError("down"), "redis": True, "broker": True}, "db"),
        ({"db": True, "redis": False, "broker": True}, "redis"),
        ({"db": True, "redis": True, "broker": False}, "broker"),
    ],
)
def test_readyz_reports_the_failing_dependency(results: dict[str, object], failing: str) -> None:
    resp = _app_with_checks(**results).get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    assert body["checks"][failing] == "fail"
    assert all(v == "ok" for k, v in body["checks"].items() if k != failing)


def test_readyz_without_started_dependencies_is_not_ready() -> None:
    resp = TestClient(build_app(lifespan=None)).get("/readyz")
    assert resp.status_code == 503


def test_healthz_stays_200_when_dependencies_are_down() -> None:
    client = _app_with_checks(db=False, redis=False, broker=False)
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 503


def test_internal_audio_refuses_without_a_started_hub() -> None:
    with pytest.raises(WebSocketDisconnect):
        with TestClient(app).websocket_connect("/internal/audio"):
            pass
