"""The `TranscriptionProvider` adapter — plan 3.6 step 3, same pattern as `meeting-service`'s
`MeetingTransport` (`mytheus-backend/services/meeting_service/transport.py`): one Protocol,
one real-but-unexercisable binding, one deterministic fake, one config-selected factory
function. Keeps Deepgram swappable so a DPA-constrained tenant can be pinned to self-hosted
Whisper later ([10 section 3.3](../../../mytheus-docs/10-realtime-and-transcription.md))
without touching callers.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Utterance:
    """One Deepgram result — interim or final. `chunk_id` is stable across interim/final
    revisions of the SAME utterance only for a real provider that assigns it that way;
    finals are what get persisted and `chunk_id` is what `chunk:{chunk_id}` idempotency keys
    off, so it must be stable across redelivery of the same final (i.e. the provider/consumer
    boundary, not per network read)."""

    chunk_id: str
    speaker_id: str
    start_ms: int
    end_ms: int
    text: str
    is_final: bool
    confidence: float


class TranscriptionStream(Protocol):
    def utterances(self) -> AsyncIterator[Utterance]:
        """Yields interim and final `Utterance`s for the lifetime of the stream."""
        ...

    async def close(self) -> None: ...


class TranscriptionProvider(Protocol):
    async def open_stream(
        self, *, org_id: str, meeting_id: str, region: str, diarize: bool = True
    ) -> TranscriptionStream: ...
