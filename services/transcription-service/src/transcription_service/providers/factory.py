"""Config-selected provider binding.

- `DEEPGRAM_API_KEY` set -> `DeepgramProvider` (whose `open_stream` still refuses while the
  compliance gate is closed -- see `compliance.py`; selection never bypasses the gate).
- no key AND `TRANSCRIPTION_ALLOW_FAKE_PROVIDER=1` -> `FakeProvider`, an explicit dev opt-in.
- no key and no opt-in -> `UnavailableProvider`: FAIL CLOSED. Its `open_stream` always
  raises, and `/internal/audio` refuses its connections (1011) before any database access.

Why the opt-in exists: before it, a missing key silently selected `FakeProvider`, which emits
scripted text regardless of input. A production pod that lost its key would have accepted
real tenant meeting audio and persisted fabricated transcripts into `transcript_chunks` and
`transcript-chunks`. Fake output for a real meeting is now only possible on a process someone
deliberately started with the dev flag.

Deliberately mirrors `select_transport()` in
`mytheus-backend/services/meeting_service/transport.py`.
"""

from __future__ import annotations

from ..config import Settings
from .base import (
    DEFAULT_ENCODING,
    DEFAULT_SAMPLE_RATE,
    TranscriptionProvider,
    TranscriptionStream,
)
from .deepgram import DeepgramProvider
from .fake import FakeProvider


class ProviderNotConfiguredError(RuntimeError):
    """No real provider is configured and the fake was not explicitly allowed."""


class UnavailableProvider:
    """The fail-closed binding: no `DEEPGRAM_API_KEY` and no fake opt-in."""

    async def open_stream(
        self,
        *,
        org_id: str,
        meeting_id: str,
        region: str,
        diarize: bool = True,
        encoding: str = DEFAULT_ENCODING,
        sample_rate: int = DEFAULT_SAMPLE_RATE,
    ) -> TranscriptionStream:
        raise ProviderNotConfiguredError(
            "no transcription provider configured: set DEEPGRAM_API_KEY, or "
            "TRANSCRIPTION_ALLOW_FAKE_PROVIDER=1 for local development only"
        )


def select_provider(settings: Settings) -> TranscriptionProvider:
    if settings.deepgram_api_key:
        return DeepgramProvider(
            api_key=settings.deepgram_api_key,
            model=settings.deepgram_model,
            language=settings.deepgram_language,
        )
    if settings.allow_fake_provider:
        return FakeProvider()
    return UnavailableProvider()


def _assert_conforms(_cls: type[TranscriptionProvider]) -> None:
    """No-op at runtime; makes mypy check this class against the Protocol."""


_assert_conforms(UnavailableProvider)
