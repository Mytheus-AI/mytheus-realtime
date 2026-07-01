"""transcription-service: the LiveKit server-side audio hook -> Deepgram WSS proxy (scaffold).

Boots a FastAPI app with `/healthz` and the internal WS route `/internal/audio` (the LiveKit
audio-hook target). The Deepgram session-per-meeting, interim-vs-final handling, `transcript_chunks`
writes (finals only), and the `transcript-chunks` Avro producer land in the realtime band. The
Deepgram key stays server-side and audio never crosses the backend trust boundary.

Run: uvicorn transcription_service.main:app --port 8091
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, WebSocket

app = FastAPI(title="transcription-service")


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {"status": "ok", "service": "transcription-service"}


@app.websocket("/internal/audio")
async def internal_audio(websocket: WebSocket) -> None:
    # LiveKit server-side audio-hook target. Scaffold: accept then close cleanly so the transport
    # contract exists. The Deepgram WSS proxy + transcript_chunks persistence land later.
    await websocket.accept()
    await websocket.close()
