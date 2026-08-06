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
    """One Deepgram result — interim or final. `chunk_id` MUST be a valid UUID string: it is
    the `transcript_chunks.id` primary key (`uuid NOT NULL`) and the Avro `chunk_id` field is
    `logicalType: uuid` — a non-UUID value fails the INSERT (fix round 1, item 1: Deepgram's
    `request_id` is not a UUID). It must also be DETERMINISTIC given the same underlying
    provider event, not random per read: it is stable across interim/final revisions of the
    SAME utterance for a real provider that assigns it that way, and `chunk:{chunk_id}` is
    what redelivery idempotency keys off, so redelivery of the same final must derive the same
    `chunk_id` again, not a fresh random one (a random UUID per read would silently defeat the
    outbox's `(topic, idempotency_key)` dedup guard).

    `provider_correlation_id` carries the vendor's own request/session identifier (Deepgram's
    `request_id`) for support/debugging correlation against the vendor's own dashboard, kept
    separate from `chunk_id` precisely because `chunk_id` must be a UUID and the vendor's id is
    not."""

    chunk_id: str
    speaker_id: str
    start_ms: int
    end_ms: int
    text: str
    is_final: bool
    confidence: float
    provider_correlation_id: str | None = None


class TranscriptionStream(Protocol):
    def utterances(self) -> AsyncIterator[Utterance]:
        """Yields interim and final `Utterance`s for the lifetime of the stream."""
        ...

    async def close(self) -> None: ...


class TranscriptionProvider(Protocol):
    async def open_stream(
        self, *, org_id: str, meeting_id: str, region: str, diarize: bool = True
    ) -> TranscriptionStream: ...
