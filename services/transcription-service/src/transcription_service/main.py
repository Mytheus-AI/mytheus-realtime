"""transcription-service: the LiveKit server-side audio hook -> Deepgram WSS proxy.

Boots a FastAPI app with `/healthz` and the internal WS route `/internal/audio` (the LiveKit
audio-hook target). On startup, launches `meeting_room_consumer.run_consumer` as a background
task -- the `meeting-room-events` I/O loop that opens/closes the per-meeting Deepgram stream via
`SessionTracker`. On shutdown it is signalled to stop and drain (not cancelled -- see the
`finally` below), so a pod restart leaves the consumer group cleanly rather than waiting out the
session timeout.

The consumer shares this process rather than running as its own deployment because
`SessionTracker` keeps each meeting's open `TranscriptionStream` in memory and `/internal/audio`
has to push audio into that same object; see `meeting_room_consumer.run_consumer` for what that
constrains about how the poll loop is driven.

KNOWN GAP, recorded in `mytheus-docs/work-done-till-now/`: `/healthz` reports only that the HTTP
app is up. If the consumer task dies, its death is logged loudly (`_log_consumer_exit` below)
but `/healthz` still answers ok, so a liveness probe would not restart the pod. Wiring consumer
liveness into the probe changes the endpoint's response contract and its test, and is left as a
disclosed follow-up rather than smuggled in here.

Run: uvicorn transcription_service.main:app --port 8091
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket
from redis.asyncio import from_url as redis_from_url

from .config import load_settings_from_env
from .db import make_engine, make_session_factory
from .meeting_room_consumer import run_consumer
from .providers.factory import select_provider
from .sessions import SessionTracker

logger = logging.getLogger(__name__)

# Headroom for one in-flight `Consumer.poll` (1 s) plus the message it may be processing,
# before shutdown gives up on a clean stop and cancels.
_CONSUMER_STOP_TIMEOUT_SECONDS = 10.0


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

    try:
        yield
    finally:
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
        await redis.aclose()
        await engine.dispose()


app = FastAPI(title="transcription-service", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {"status": "ok", "service": "transcription-service"}


@app.websocket("/internal/audio")
async def internal_audio(websocket: WebSocket) -> None:
    # LiveKit server-side audio-hook target. Scaffold: accept then close cleanly so the transport
    # contract exists. The Deepgram WSS proxy + transcript_chunks persistence land later.
    await websocket.accept()
    await websocket.close()
