"""`TranscriptChunk` Avro value, mirrored field-for-field from
`mytheus-events/schemas/transcript-chunks/v1.avsc` (as amended today to carry `redacted`,
commit `4139d5a` on `mytheus-events` branch `plan-3-4-meeting-and-transcription`) and matching
the generated `mytheus_events.transcript_chunks.TranscriptChunk` pydantic model verbatim,
field for field including the `redacted: bool = False` default.

See `envelope.py` for why this is a hand-kept mirror rather than an import of the generated
client. `header` is `EventEnvelopeHeader` from this module's sibling, not
`mytheus_events.envelope.EventEnvelopeHeader`, for the same reason.

Partition key: composite `{org_id}:{meeting_id}` per ADR-0017 (mytheus-events
`codegen/_schemas.py` `COMPOSITE_PARTITION_KEY_TOPICS`) — `transcript-chunks` and
`agent-stream` only. Built by callers, not by this model; see `outbox.py`.

Idempotency key: `chunk:{chunk_id}` (mytheus-events `codegen/_schemas.py` `IDEMPOTENCY_KEYS`).
"""

from __future__ import annotations

from pydantic import BaseModel

from .envelope import EventEnvelopeHeader


class TranscriptChunk(BaseModel):
    header: EventEnvelopeHeader
    meeting_id: str
    chunk_id: str
    speaker_id: str
    start_ms: int
    end_ms: int
    text: str
    is_final: bool
    confidence: float
    redacted: bool = False
