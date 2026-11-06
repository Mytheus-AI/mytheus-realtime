"""`FakeProvider` — not in the plan text, same rationale as `FakeTransport` in
`mytheus-backend/services/meeting_service/transport.py`: there is no Deepgram account
available to this build (compliance gate condition 1 is unmet), so this is what makes the
interim-vs-final pipeline, redaction, persistence, and production testable end-to-end without
one. Deterministic and dependency-free; every call is recorded so tests can assert what was
opened and with what region/diarize arguments, and every audio frame a stream receives is
recorded on that stream (`FakeProvider.streams[i].frames`).

Two script modes:

- default (`emit_on_audio=False`): the stream yields its whole script immediately, regardless
  of audio -- the shape every pre-existing test relies on.
- `emit_on_audio=True`: the stream yields ONE script item per received audio frame, so a test
  can prove that bytes pushed into `/internal/audio` are what drives captions and finals.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field

from .base import (
    DEFAULT_ENCODING,
    DEFAULT_SAMPLE_RATE,
    TranscriptionProvider,
    TranscriptionStream,
    Utterance,
)


@dataclass
class _RecordedOpen:
    org_id: str
    meeting_id: str
    region: str
    diarize: bool
    encoding: str = DEFAULT_ENCODING
    sample_rate: int = DEFAULT_SAMPLE_RATE


def default_script(chunk_id: str | None = None) -> list[Utterance]:
    """A realistic interim-then-final sequence for ONE utterance: two mutable interims that
    revise the transcript, then the final. This is the shape
    `tests/test_persistence_finals_only.py` (the load-bearing test) drives through the real
    pipeline.

    `chunk_id` defaults to a fresh `uuid.uuid4()` per call, NOT a fixed placeholder string
    (fix round 1, item 1: the old `"utt-1"` default was not a valid UUID and crashed the
    identical INSERT `DeepgramProvider` crashed on — a landmine nothing caught because every
    existing test overrode `chunk_id` with `str(uuid.uuid4())`; the first real caller to reach
    `select_provider()`'s no-key default, unmodified, would have hit it. See
    `tests/test_sessions.py::test_default_script_persists_through_the_real_pipeline`, which
    now drives this default end to end)."""
    if chunk_id is None:
        chunk_id = str(uuid.uuid4())
    return [
        Utterance(
            chunk_id=chunk_id,
            speaker_id="0",
            start_ms=0,
            end_ms=400,
            text="hello",
            is_final=False,
            confidence=0.4,
        ),
        Utterance(
            chunk_id=chunk_id,
            speaker_id="0",
            start_ms=0,
            end_ms=900,
            text="hello there",
            is_final=False,
            confidence=0.6,
        ),
        Utterance(
            chunk_id=chunk_id,
            speaker_id="0",
            start_ms=0,
            end_ms=1200,
            text="hello there, how are you",
            is_final=True,
            confidence=0.97,
        ),
    ]


@dataclass
class _FakeStream:
    _script: Sequence[Utterance]
    emit_on_audio: bool = False
    closed: bool = False
    frames: list[bytes] = field(default_factory=list)
    _ticks: asyncio.Queue[bool] = field(default_factory=asyncio.Queue)

    async def utterances(self) -> AsyncIterator[Utterance]:
        if not self.emit_on_audio:
            for u in self._script:
                yield u
            return
        for u in self._script:
            more = await self._ticks.get()
            if not more:
                return
            yield u

    async def send_audio(self, frame: bytes) -> None:
        if self.closed:
            return
        self.frames.append(frame)
        if self.emit_on_audio:
            self._ticks.put_nowait(True)

    async def close(self) -> None:
        self.closed = True
        # Unblocks an `emit_on_audio` iterator waiting for the next frame.
        self._ticks.put_nowait(False)


@dataclass
class FakeProvider:
    opens: list[_RecordedOpen] = field(default_factory=list)
    script: list[Utterance] | None = None
    """Override the default scripted sequence for a specific test; `None` uses
    `default_script()`."""
    emit_on_audio: bool = False
    streams: list[_FakeStream] = field(default_factory=list)

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
        self.opens.append(
            _RecordedOpen(
                org_id=org_id,
                meeting_id=meeting_id,
                region=region,
                diarize=diarize,
                encoding=encoding,
                sample_rate=sample_rate,
            )
        )
        stream = _FakeStream(
            _script=self.script if self.script is not None else default_script(),
            emit_on_audio=self.emit_on_audio,
        )
        self.streams.append(stream)
        return stream


def _assert_conforms(_cls: type[TranscriptionProvider]) -> None:
    """No-op at runtime; makes mypy check this class against the Protocol."""


_assert_conforms(FakeProvider)
