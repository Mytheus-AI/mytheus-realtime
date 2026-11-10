"""Environment configuration. One place that reads `os.environ` so tests can construct a
`Settings` directly instead of monkeypatching the environment.

Env vars per the plan (3.6 step 12) plus this service's `.env.example`:
`DEEPGRAM_API_KEY`, `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`, `REDIS_URL`,
`REDPANDA_BROKERS`, `SCHEMA_REGISTRY_URL`, `TRANSCRIPTION_WS_BIND`, `DATABASE_URL`,
`OTEL_EXPORTER_OTLP_ENDPOINT`, `IDENTITY_GRPC_ADDR` (identity-tenant-service gRPC, for the
org's Deepgram key terms; unset = none).

`/internal/audio` (see `audio_in.py` for the contract): `INTERNAL_AUDIO_TOKEN` (shared secret
the sender presents in `X-Internal-Auth`; unset means every connection is refused -- fail
closed), `AUDIO_INIT_TIMEOUT_SECONDS`, `AUDIO_IDLE_TIMEOUT_SECONDS`, `AUDIO_MAX_FRAME_BYTES`,
`AUDIO_QUEUE_MAX_FRAMES`. `/capture/audio` (`capture_in.py`): `CAPTURE_TOKEN_SECRET`,
`CAPTURE_ALLOWED_ORIGINS`. `TRANSCRIPTION_ALLOW_FAKE_PROVIDER=1` (default off) is the explicit
dev opt-in that lets `FakeProvider` be selected when no `DEEPGRAM_API_KEY` is set; without it
the service fails closed (`providers/factory.py`). The new fields carry defaults so existing
direct `Settings(...)` constructions keep working.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_AUDIO_INIT_TIMEOUT_SECONDS = 10.0
DEFAULT_AUDIO_IDLE_TIMEOUT_SECONDS = 30.0
# 16 kHz s16le mono is 32 KiB/s; 64 KiB admits a 2 s frame, far above the 20-100 ms frames a
# streaming sender uses, while bounding what one frame can make this process allocate.
DEFAULT_AUDIO_MAX_FRAME_BYTES = 64 * 1024
# About 2 s of audio at 20 ms frames. Beyond that, captions would already blow the p95 < 2 s
# SLO, so dropping the oldest audio beats buffering it.
DEFAULT_AUDIO_QUEUE_MAX_FRAMES = 100


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str
    redpanda_brokers: str
    schema_registry_url: str
    transcription_ws_bind: str
    deepgram_api_key: str | None
    livekit_url: str | None
    livekit_api_key: str | None
    livekit_api_secret: str | None
    otel_exporter_otlp_endpoint: str | None
    internal_audio_token: str | None = None
    audio_init_timeout_seconds: float = DEFAULT_AUDIO_INIT_TIMEOUT_SECONDS
    audio_idle_timeout_seconds: float = DEFAULT_AUDIO_IDLE_TIMEOUT_SECONDS
    audio_max_frame_bytes: int = DEFAULT_AUDIO_MAX_FRAME_BYTES
    audio_queue_max_frames: int = DEFAULT_AUDIO_QUEUE_MAX_FRAMES
    allow_fake_provider: bool = False
    # Deepgram model and language. Nova-3 is Deepgram's most accurate streaming model, and
    # "multi" is its code-switching mode (English and Hindi mixed in one stream, as calls here
    # are). Overridable without a release, e.g. DEEPGRAM_LANGUAGE=en for English-only.
    deepgram_model: str = "nova-3"
    deepgram_language: str = "multi"
    # identity-tenant-service gRPC (ADR-0039 D1: GetOrgContext for Deepgram key terms). Same
    # variable name as mytheus-intelligence. Unset means streams open without key terms.
    identity_grpc_addr: str | None = None
    # `/capture/audio` (the browser capture companion, `capture_in.py`). The HS256 secret
    # mytheus-ui signs capture tokens with; unset or under 32 characters refuses every capture
    # connection (fail closed). `CAPTURE_ALLOWED_ORIGINS` optionally pins the extension origins
    # (`chrome-extension://<id>`, comma separated); unset admits any origin holding a valid token.
    capture_token_secret: str | None = None
    capture_allowed_origins: str | None = None

    def __repr__(self) -> str:
        # Never print secrets (DB URL credentials, provider keys, the audio token).
        return "Settings(<redacted>)"


def load_settings_from_env() -> Settings:
    return Settings(
        database_url=os.environ.get(
            "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/mytheus"
        ),
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379"),
        redpanda_brokers=os.environ.get("REDPANDA_BROKERS", "localhost:9092"),
        schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8085"),
        transcription_ws_bind=os.environ.get("TRANSCRIPTION_WS_BIND", "0.0.0.0:8091"),
        deepgram_api_key=os.environ.get("DEEPGRAM_API_KEY") or None,
        deepgram_model=os.environ.get("DEEPGRAM_MODEL") or "nova-3",
        deepgram_language=os.environ.get("DEEPGRAM_LANGUAGE") or "multi",
        identity_grpc_addr=os.environ.get("IDENTITY_GRPC_ADDR") or None,
        capture_token_secret=os.environ.get("CAPTURE_TOKEN_SECRET") or None,
        capture_allowed_origins=os.environ.get("CAPTURE_ALLOWED_ORIGINS") or None,
        livekit_url=os.environ.get("LIVEKIT_URL") or None,
        livekit_api_key=os.environ.get("LIVEKIT_API_KEY") or None,
        livekit_api_secret=os.environ.get("LIVEKIT_API_SECRET") or None,
        otel_exporter_otlp_endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or None,
        internal_audio_token=os.environ.get("INTERNAL_AUDIO_TOKEN") or None,
        audio_init_timeout_seconds=float(
            os.environ.get("AUDIO_INIT_TIMEOUT_SECONDS") or DEFAULT_AUDIO_INIT_TIMEOUT_SECONDS
        ),
        audio_idle_timeout_seconds=float(
            os.environ.get("AUDIO_IDLE_TIMEOUT_SECONDS") or DEFAULT_AUDIO_IDLE_TIMEOUT_SECONDS
        ),
        audio_max_frame_bytes=int(
            os.environ.get("AUDIO_MAX_FRAME_BYTES") or DEFAULT_AUDIO_MAX_FRAME_BYTES
        ),
        audio_queue_max_frames=int(
            os.environ.get("AUDIO_QUEUE_MAX_FRAMES") or DEFAULT_AUDIO_QUEUE_MAX_FRAMES
        ),
        # Exactly "1" opts in; anything else (unset, "0", "true", "yes") stays fail-closed.
        allow_fake_provider=os.environ.get("TRANSCRIPTION_ALLOW_FAKE_PROVIDER") == "1",
    )
