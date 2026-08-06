"""Config-selected provider binding: `FakeProvider` unless `DEEPGRAM_API_KEY` is set.

Deliberately mirrors `select_transport()` in
`mytheus-backend/services/meeting_service/transport.py`. Selection is ONLY about whether a key
is configured — the compliance-gate refusal lives inside `DeepgramProvider.open_stream`
(`compliance.py`), not here, so that even a misconfigured environment that somehow sets
`DEEPGRAM_API_KEY` still cannot open a real stream. Defaulting to the fake means a forgotten
key fails toward "obviously not talking to Deepgram" rather than a `DeepgramProvider` raising
deep inside a meeting-room-events handler.
"""

from __future__ import annotations

from ..config import Settings
from .base import TranscriptionProvider
from .deepgram import DeepgramProvider
from .fake import FakeProvider


def select_provider(settings: Settings) -> TranscriptionProvider:
    if not settings.deepgram_api_key:
        return FakeProvider()
    return DeepgramProvider(api_key=settings.deepgram_api_key)
