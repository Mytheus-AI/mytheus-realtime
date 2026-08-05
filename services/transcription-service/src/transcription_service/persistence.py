"""The interim-vs-final split — plan 3.6 step 5, THE HARD RULE this whole service exists to
get right:

  Interims -> live captions only. Emit to `realtime-gateway`. NEVER persisted, NEVER on the bus.
  Finals   -> redact -> ONE `transcript_chunks` row -> ONE `transcript-chunks` message ->
              also emit as a caption.

`process_utterance` is the single call site both `providers.fake.FakeProvider` and (once the
compliance gate opens) `providers.deepgram.DeepgramProvider` streams are driven through, so
there is exactly one place in this codebase that decides "does this utterance touch Postgres
or Redpanda" — an interim can never reach `outbox.enqueue` or a `transcript_chunks` INSERT by
construction, not by a comment.
"""

from __future__ import annotations

from dataclasses import dataclass

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .captions import publish_caption
from .db import tenant_transaction
from .events import TranscriptChunk, build_envelope_header
from .outbox import enqueue
from .providers.base import Utterance
from .redaction import redact

PRODUCER_NAME = "transcription-service"
TOPIC = "transcript-chunks"
SCHEMA_SUBJECT = "transcript-chunks-value"


@dataclass(frozen=True)
class FinalWriteResult:
    outbox_id: int
    chunk_id: str
    redacted: bool


def _idempotency_key(chunk_id: str) -> str:
    return f"chunk:{chunk_id}"


def _partition_key(org_id: str, meeting_id: str) -> str:
    """Composite `{org_id}:{meeting_id}` per ADR-0017 — `transcript-chunks` and `agent-stream`
    only; every other topic in this program keeps `org_id` alone. `org_id` still rides the
    envelope header regardless (mandatory, non-nullable)."""
    return f"{org_id}:{meeting_id}"


async def _write_final(
    session: AsyncSession,
    *,
    org_id: str,
    meeting_id: str,
    utterance: Utterance,
    text_value: str,
    redacted: bool,
) -> FinalWriteResult | None:
    idempotency_key = _idempotency_key(utterance.chunk_id)
    header = build_envelope_header(
        org_id=org_id, producer=PRODUCER_NAME, idempotency_key=idempotency_key
    )
    value = TranscriptChunk(
        header=header,
        meeting_id=meeting_id,
        chunk_id=utterance.chunk_id,
        speaker_id=utterance.speaker_id,
        start_ms=utterance.start_ms,
        end_ms=utterance.end_ms,
        text=text_value,
        is_final=True,
        confidence=utterance.confidence,
        redacted=redacted,
    )

    # The single dedup gate for BOTH the outbox row and the transcript_chunks row: only when
    # the outbox insert is genuinely new do we also write transcript_chunks, in the SAME
    # transaction, so a redelivered final (same chunk_id -> same idempotency_key) is a no-op
    # for both, atomically. See outbox.py's docstring for why this replaces a cross-partition
    # unique index on transcript_chunks.
    outbox_result = await enqueue(
        session,
        org_id=org_id,
        topic=TOPIC,
        partition_key=_partition_key(org_id, meeting_id),
        idempotency_key=idempotency_key,
        schema_subject=SCHEMA_SUBJECT,
        payload=value.model_dump(),
    )
    if not outbox_result.is_new:
        return None

    await session.execute(
        text(
            "INSERT INTO transcript_chunks "
            "(id, org_id, meeting_id, speaker_id, start_ms, end_ms, text, is_final, "
            "confidence, redacted) "
            "VALUES (:id, :org_id, :meeting_id, :speaker_id, :start_ms, :end_ms, :text, "
            "true, :confidence, :redacted)"
        ),
        {
            "id": utterance.chunk_id,
            "org_id": org_id,
            "meeting_id": meeting_id,
            "speaker_id": utterance.speaker_id,
            "start_ms": utterance.start_ms,
            "end_ms": utterance.end_ms,
            "text": text_value,
            "confidence": utterance.confidence,
            "redacted": redacted,
        },
    )
    return FinalWriteResult(
        outbox_id=outbox_result.outbox_id, chunk_id=utterance.chunk_id, redacted=redacted
    )


async def process_utterance(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis | None,
    org_id: str,
    meeting_id: str,
    utterance: Utterance,
    pii_redaction_enabled: bool,
) -> FinalWriteResult | None:
    """The one call site. Returns `None` for every interim (by construction — the `if not
    is_final` branch never touches `session_factory` or the outbox at all) and for a
    duplicate final; returns a `FinalWriteResult` only for a genuinely new final write."""
    if not utterance.is_final:
        # Interim captions publish `utterance.text` RAW -- redaction never runs on this path
        # at all, not even computed. This is deliberate, decided on review (not an oversight
        # of where the redact() call happened to sit): PII redaction's job, per plan 3.5, is
        # to keep PII out of what OUTLIVES the meeting -- `transcript_chunks`, the
        # `transcript-chunks` topic, and everything downstream of it (embedding-service,
        # agent-runtime-service, search-service). Interims never reach any of that: they are
        # never persisted, never produced (this function's whole reason to exist), and gone
        # from every browser's memory the moment the connection drops or the utterance
        # finalizes. Redacting them would protect a surface redaction was never meant to
        # cover -- every participant in the meeting already heard the speaker say the words
        # out loud in real time; the caption is a transcription of speech already disclosed
        # to that exact audience, not a new disclosure. It would also cost a second/third
        # redaction pass per utterance (once per interim revision) on the p95 < 2 s live
        # captions SLO for zero corresponding protection.
        #
        # Finals are NOT "at-rest only" despite the plan's framing suggesting that scope: the
        # final's OWN caption emission below uses `text_override=text_value`, the ALREADY
        # REDACTED text, not `utterance.text` -- so the live caption for a final and the
        # persisted row show the identical redacted text, with no flash of raw PII before the
        # utterance settles. Only interims are ever unredacted, and only because they never
        # graduate into anything redaction is meant to protect.
        #
        # Follow-up, not solved here: this assumes every tenant's compliance posture treats
        # "at rest" as the redaction boundary, matching plan 3.5/12's framing. A tenant whose
        # posture treats the live plane itself as in-scope (the caption still traverses
        # realtime-gateway and lands in browser memory, even if briefly) would need a
        # DIFFERENT default than this service ships today -- e.g. a second tenant_settings
        # flag distinct from pii_redaction_enabled, gating the live-caption path
        # independently. No such flag exists; this is a real gap for that case, not an
        # oversight to silently assume away.
        if redis is not None:
            await publish_caption(redis, org_id=org_id, meeting_id=meeting_id, utterance=utterance)
        return None

    text_value, redacted = (
        redact(utterance.text) if pii_redaction_enabled else (utterance.text, False)
    )

    async with tenant_transaction(session_factory, org_id) as session:
        result = await _write_final(
            session,
            org_id=org_id,
            meeting_id=meeting_id,
            utterance=utterance,
            text_value=text_value,
            redacted=redacted,
        )

    if result is not None and redis is not None:
        await publish_caption(
            redis,
            org_id=org_id,
            meeting_id=meeting_id,
            utterance=utterance,
            text_override=text_value,
        )
    return result
