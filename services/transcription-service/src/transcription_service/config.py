"""Environment configuration. One place that reads `os.environ` so tests can construct a
`Settings` directly instead of monkeypatching the environment.

Env vars per the plan (3.6 step 12) plus this service's `.env.example`:
`DEEPGRAM_API_KEY`, `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`, `REDIS_URL`,
`REDPANDA_BROKERS`, `SCHEMA_REGISTRY_URL`, `TRANSCRIPTION_WS_BIND`, `DATABASE_URL`,
`OTEL_EXPORTER_OTLP_ENDPOINT`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


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


def load_settings_from_env() -> Settings:
    return Settings(
        database_url=os.environ.get(
            "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/mytheus"
        ),
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379"),
        redpanda_brokers=os.environ.get("REDPANDA_BROKERS", "localhost:9092"),
        schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        transcription_ws_bind=os.environ.get("TRANSCRIPTION_WS_BIND", "0.0.0.0:8091"),
        deepgram_api_key=os.environ.get("DEEPGRAM_API_KEY") or None,
        livekit_url=os.environ.get("LIVEKIT_URL") or None,
        livekit_api_key=os.environ.get("LIVEKIT_API_KEY") or None,
        livekit_api_secret=os.environ.get("LIVEKIT_API_SECRET") or None,
        otel_exporter_otlp_endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or None,
    )
