"""transcription-service: the server-side audio hook -> Deepgram WSS proxy.

Routes:

- `GET /healthz` -- liveness. Cheap by design: answers 200 as long as the event loop serves
  HTTP, and touches no dependency, so a slow Postgres can never get a healthy pod killed.
- `GET /readyz` -- readiness. Checks each dependency (`db`: `SELECT 1`; `redis`: `PING`;
  `broker`: the `meeting-room-events` consumer task is still running) with a short timeout
  and answers 200 only when all pass, else 503 with the per-dependency result. A dead
  consumer task (previously only logged, the KNOWN GAP recorded in
  `mytheus-docs/work-done-till-now/`) now takes the pod out of rotation.
- `WS /internal/audio` -- the audio hook. Contract, close codes and enforcement live in
  `audio_in.py`'s module docstring (and `README.md`).
- `WS /capture/audio` -- audio from the Mytheus Capture browser extension (calls on Meet, Teams
  or Zoom web), authenticated by a capture token in the first frame. Contract in
  `capture_in.py`'s module docstring.

On startup, launches `meeting_room_consumer.run_consumer` as a background task -- the
`meeting-room-events` I/O loop whose `joined`/`left` hints open/close meeting sessions via
`SessionTracker`. On shutdown: live audio connections are closed with 1001 and drained, the
consumer is signalled to stop (not cancelled -- see the `finally` below), every open session
is closed, then Redis and the engine are released.

The consumer shares this process rather than running as its own deployment because
`SessionTracker` keeps each meeting's open `TranscriptionStream` in memory and `/internal/audio`
has to push audio into that same object; see `meeting_room_consumer.run_consumer` for what that
constrains about how the poll loop is driven.

`build_app(lifespan=...)` exists so tests can mount the same routes over fakes (an `AudioHub`
and readiness checks placed on `app.state`) without real infrastructure.

Run: uvicorn transcription_service.main:app --port 8091
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse
from redis.asyncio import from_url as redis_from_url
from sqlalchemy import text

from .audio_in import (
    CLOSE_INTERNAL_ERROR,
    AudioEndpointConfig,
    AudioHub,
    provider_mode_for,
)
from .capture_auth import secret_usable
from .capture_in import CaptureHub, parse_allowed_origins
from .config import load_settings_from_env
from .db import make_engine, make_session_factory
from .meeting_room_consumer import run_consumer
from .providers.factory import select_provider
from .sessions import SessionTracker

ReadinessCheck = Callable[[], Awaitable[bool]]

logger = logging.getLogger(__name__)

# Headroom for one in-flight `Consumer.poll` (1 s) plus the message it may be processing,
# before shutdown gives up on a clean stop and cancels.
_CONSUMER_STOP_TIMEOUT_SECONDS = 10.0
_AUDIO_DRAIN_TIMEOUT_SECONDS = 5.0
_READINESS_CHECK_TIMEOUT_SECONDS = 2.0


def _log_consumer_exit(task: asyncio.Task[None]) -> None:
    """The consumer runs as a background task, so an unhandled exception in it would otherwise
    be swallowed until shutdown: the pod would keep serving `/healthz` while transcription was
    silently dead. (Its counterpart in `mytheus-backend` owns its whole process, so the same
    crash there takes the process down and Kubernetes restarts it -- this task cannot rely on
    that.) Logging from a done-callback makes the death visible the moment it happens."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            "meeting-room-events consumer exited unexpectedly; this pod is no longer opening "
            "or closing transcription sessions",
            exc_info=exc,
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = load_settings_from_env()
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    # `decode_responses=True` matches `tests/conftest.py`'s client, so the tested and the
    # running configuration cannot diverge. Nothing here reads a value back (the dedupe key is
    # `SET NX` and captions are `PUBLISH`), so the setting is about consistency, not behaviour.
    redis = redis_from_url(settings.redis_url, decode_responses=True)
    provider = select_provider(settings)
    tracker = SessionTracker(provider=provider, session_factory=session_factory, redis=redis)
    audio_hub = AudioHub(
        tracker=tracker,
        config=AudioEndpointConfig.from_settings(settings),
        provider_mode=provider_mode_for(provider),
    )
    capture_hub = CaptureHub(
        tracker=tracker,
        config=AudioEndpointConfig.from_settings(settings),
        provider_mode=provider_mode_for(provider),
        secret=settings.capture_token_secret,
        allowed_origins=parse_allowed_origins(settings.capture_allowed_origins),
    )
    if not secret_usable(settings.capture_token_secret):
        logger.warning(
            "CAPTURE_TOKEN_SECRET is unset or shorter than 32 characters: /capture/audio will "
            "refuse every connection (capture companion not connected)"
        )
    if provider_mode_for(provider) == "unavailable":
        logger.error(
            "no DEEPGRAM_API_KEY and TRANSCRIPTION_ALLOW_FAKE_PROVIDER is not 1: "
            "/internal/audio will refuse every connection (fail closed)"
        )
    if not settings.internal_audio_token:
        logger.error("INTERNAL_AUDIO_TOKEN is unset: /internal/audio will refuse every connection")

    stop = asyncio.Event()
    consumer_task = asyncio.create_task(
        run_consumer(
            tracker,
            redis,
            brokers=settings.redpanda_brokers,
            schema_registry_url=settings.schema_registry_url,
            stop=stop,
        )
    )
    consumer_task.add_done_callback(_log_consumer_exit)

    async def _db_ready() -> bool:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True

    async def _redis_ready() -> bool:
        return bool(await redis.ping())

    async def _broker_ready() -> bool:
        return not consumer_task.done()

    app.state.audio_hub = audio_hub
    app.state.capture_hub = capture_hub
    app.state.readiness = {"db": _db_ready, "redis": _redis_ready, "broker": _broker_ready}

    try:
        yield
    finally:
        await asyncio.gather(
            audio_hub.drain(timeout=_AUDIO_DRAIN_TIMEOUT_SECONDS),
            capture_hub.drain(timeout=_AUDIO_DRAIN_TIMEOUT_SECONDS),
        )
        # Signal, do not cancel: `run_consumer` must be allowed to finish its in-flight poll
        # before it closes the consumer, or `close()` races a worker thread still inside
        # `poll()`. See that function's docstring. Cancellation is the timeout fallback only.
        stop.set()
        try:
            await asyncio.wait_for(consumer_task, timeout=_CONSUMER_STOP_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.error(
                "meeting-room-events consumer did not stop within %ss; cancelling",
                _CONSUMER_STOP_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001 - shutdown must release Redis/the engine regardless
            logger.exception("meeting-room-events consumer raised during shutdown")
        await tracker.close_all()
        await redis.aclose()
        await engine.dispose()


def build_app(
    *, lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = lifespan
) -> FastAPI:
    application = FastAPI(title="transcription-service", lifespan=lifespan)

    @application.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"status": "ok", "service": "transcription-service"}

    @application.get("/readyz")
    async def readyz() -> JSONResponse:
        checks: dict[str, ReadinessCheck] = getattr(application.state, "readiness", {})
        results: dict[str, str] = {}
        for name, check in checks.items():
            try:
                ok = await asyncio.wait_for(check(), timeout=_READINESS_CHECK_TIMEOUT_SECONDS)
            except Exception:  # noqa: BLE001 - any failure (incl. timeout) is "not ready"
                ok = False
            results[name] = "ok" if ok else "fail"
        ready = bool(checks) and all(v == "ok" for v in results.values())
        return JSONResponse(
            status_code=200 if ready else 503,
            content={
                "status": "ready" if ready else "not_ready",
                "service": "transcription-service",
                "checks": results,
            },
        )

    @application.websocket("/internal/audio")
    async def internal_audio(websocket: WebSocket) -> None:
        hub: AudioHub | None = getattr(websocket.app.state, "audio_hub", None)
        if hub is None:
            # Not started (no lifespan): fail closed, never accept.
            await websocket.close(code=CLOSE_INTERNAL_ERROR, reason="service not ready")
            return
        await hub.handle(websocket)

    @application.websocket("/capture/audio")
    async def capture_audio(websocket: WebSocket) -> None:
        capture: CaptureHub | None = getattr(websocket.app.state, "capture_hub", None)
        if capture is None:
            await websocket.close(code=CLOSE_INTERNAL_ERROR, reason="service not ready")
            return
        await capture.handle(websocket)

    return application


app = build_app()
