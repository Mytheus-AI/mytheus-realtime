"""Provider-drop recovery: `ReconnectingStream` (providers/resilient.py) and its use by
`SessionTracker`, over fakes only.

Covers: the drop is absorbed with exponential backoff; no audio frame is lost or reordered
across the reconnect; utterances keep their order and `start_ms` stays monotonic; a clean end
is never retried; a closed compliance gate is never retried; exhaustion surfaces as
`ProviderReconnectFailed`; and the reconnect-SLO harness -- many streams dropped at once all
recover inside the caption budget, with jittered (not lockstep) reconnects.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import pytest
from helpers.fakes import FakeDatabase, FakeRedis

from transcription_service.compliance import ComplianceGateClosedError
from transcription_service.providers.base import ProviderStreamDropped, Utterance
from transcription_service.providers.fake import FakeProvider
from transcription_service.providers.resilient import (
    ProviderReconnectFailed,
    ReconnectingStream,
    StreamClosedError,
)
from transcription_service.sessions import SessionTracker

# Caption latency SLO: p95 < 2 s. A reconnect must complete well inside it.
CAPTION_SLO_SECONDS = 2.0


@dataclass
class FlakyStream:
    """A provider stream that can be made to drop: after `drop()`, both `send_audio` and the
    `utterances()` iterator raise `ProviderStreamDropped`, as a dead vendor socket would."""

    index: int
    frames: list[bytes] = field(default_factory=list)
    closed: bool = False
    _dropped: bool = False
    _events: asyncio.Queue[Utterance | None] = field(default_factory=asyncio.Queue)

    def emit(self, utterance: Utterance) -> None:
        self._events.put_nowait(utterance)

    def drop(self) -> None:
        self._dropped = True
        self._events.put_nowait(None)

    async def utterances(self) -> AsyncIterator[Utterance]:
        while True:
            item = await self._events.get()
            if item is None:
                if self._dropped:
                    raise ProviderStreamDropped("fake drop")
                return
            yield item

    async def send_audio(self, frame: bytes) -> None:
        if self._dropped:
            raise ProviderStreamDropped("fake drop")
        self.frames.append(frame)

    async def close(self) -> None:
        self.closed = True
        self._events.put_nowait(None)


@dataclass
class FlakyProvider:
    streams: list[FlakyStream] = field(default_factory=list)
    open_failures: int = 0  # fail this many opens AFTER the first one
    gate_closed_on_reopen: bool = False
    open_times: list[float] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)

    async def open_stream(self, *, region: str = "us", **_: Any) -> FlakyStream:
        self.open_times.append(time.monotonic())
        self.regions.append(region)
        if self.streams:
            if self.gate_closed_on_reopen:
                raise ComplianceGateClosedError("gate closed")
            if self.open_failures > 0:
                self.open_failures -= 1
                raise ConnectionError("vendor unavailable")
        stream = FlakyStream(index=len(self.streams))
        self.streams.append(stream)
        return stream


class RecordingSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


def _u(start_ms: int, text: str, *, final: bool = True) -> Utterance:
    return Utterance(str(uuid.uuid4()), "0", start_ms, start_ms + 100, text, final, 0.9)


async def _open(provider: FlakyProvider, **kwargs: Any) -> ReconnectingStream:
    return await ReconnectingStream.open(lambda: provider.open_stream(), **kwargs)


FRAME_MS = 20
FRAME = b"\x00\x00" * 16 * FRAME_MS  # 20 ms at 16 kHz s16le


async def test_send_side_drop_reconnects_and_loses_no_frame_in_order() -> None:
    provider = FlakyProvider()
    sleep = RecordingSleep()
    stream = await _open(provider, sleep=sleep, jitter=lambda: 1.0)
    frames = [bytes([i, 0]) * 320 for i in range(6)]

    for f in frames[:3]:
        await stream.send_audio(f)
    provider.streams[0].drop()
    for f in frames[3:]:
        await stream.send_audio(f)

    assert len(provider.streams) == 2 and stream.reconnects == 1
    assert provider.streams[0].frames + provider.streams[1].frames == frames
    assert provider.streams[0].closed is True
    assert sleep.delays == [0.25]


async def test_backoff_grows_exponentially_with_jitter_and_is_capped() -> None:
    provider = FlakyProvider(open_failures=4)
    sleep = RecordingSleep()
    stream = await _open(
        provider, sleep=sleep, jitter=lambda: 0.0, max_attempts=6, base_delay=0.25, max_delay=1.0
    )
    provider.streams[0].drop()
    await stream.send_audio(FRAME)

    # jitter=0 -> factor 0.5: 0.25, 0.5, 1.0, 1.0(cap), 1.0(cap) halved.
    assert sleep.delays == [0.125, 0.25, 0.5, 0.5, 0.5]
    assert provider.streams[1].frames == [FRAME]


async def test_reconnect_exhaustion_raises_and_closes() -> None:
    provider = FlakyProvider(open_failures=99)
    stream = await _open(provider, sleep=RecordingSleep(), max_attempts=3)
    provider.streams[0].drop()
    with pytest.raises(ProviderReconnectFailed):
        await stream.send_audio(FRAME)
    with pytest.raises(ProviderReconnectFailed):
        await stream.send_audio(FRAME)
    assert stream.closed


async def test_closed_compliance_gate_is_never_retried() -> None:
    provider = FlakyProvider(gate_closed_on_reopen=True)
    sleep = RecordingSleep()
    stream = await _open(provider, sleep=sleep)
    provider.streams[0].drop()
    with pytest.raises(ComplianceGateClosedError):
        await stream.send_audio(FRAME)
    assert len(sleep.delays) == 1, "one backoff, one open, refused: no retry loop"
    assert stream.closed


async def test_clean_end_is_not_a_drop() -> None:
    provider = FakeProvider(script=[_u(0, "only")])
    stream = await ReconnectingStream.open(
        lambda: provider.open_stream(org_id="o", meeting_id="m", region="us")
    )
    seen = [u async for u in stream.utterances()]
    assert [u.text for u in seen] == ["only"]
    assert len(provider.opens) == 1 and stream.reconnects == 0


async def test_send_after_close_raises_stream_closed() -> None:
    stream = await _open(FlakyProvider())
    await stream.close()
    with pytest.raises(StreamClosedError):
        await stream.send_audio(FRAME)


async def test_utterances_survive_a_drop_in_order_with_monotonic_timestamps() -> None:
    provider = FlakyProvider()
    stream = await _open(provider, sleep=RecordingSleep())
    received: list[Utterance] = []

    async def consume() -> None:
        async for u in stream.utterances():
            received.append(u)

    consumer = asyncio.create_task(consume())
    for _ in range(50):  # 1 s of audio into generation 0
        await stream.send_audio(FRAME)
    first = provider.streams[0]
    first.emit(_u(0, "one"))
    first.emit(_u(500, "two"))
    first.drop()
    await asyncio.sleep(0.01)
    assert len(provider.streams) == 2, "the utterance side detected the drop and reconnected"
    second = provider.streams[1]
    second.emit(_u(0, "three"))  # a fresh vendor session restarts its clock at 0
    second.emit(_u(300, "four"))
    await asyncio.sleep(0.01)
    await stream.close()
    await asyncio.wait_for(consumer, timeout=1)

    assert [u.text for u in received] == ["one", "two", "three", "four"]
    starts = [u.start_ms for u in received]
    assert starts == [0, 500, 1000, 1300], "generation 1 shifted by the 1 s already sent"
    assert starts == sorted(starts)


async def test_drop_seen_by_both_sides_reconnects_exactly_once() -> None:
    provider = FlakyProvider()
    stream = await _open(provider, sleep=RecordingSleep())
    consumer = asyncio.create_task(_drain(stream))
    await asyncio.sleep(0)
    provider.streams[0].drop()
    await asyncio.gather(stream.send_audio(FRAME), asyncio.sleep(0.01))
    assert len(provider.streams) == 2 and stream.reconnects == 1
    await stream.close()
    await asyncio.wait_for(consumer, timeout=1)


async def _drain(stream: ReconnectingStream) -> list[Utterance]:
    return [u async for u in stream.utterances()]


async def test_session_pump_keeps_persisting_in_order_across_a_drop() -> None:
    """Through `SessionTracker`: the pump survives the drop, every final persists exactly
    once, in provider order, under the tenant's RLS context, and the residency region pin is
    re-applied on the reconnect."""
    org_id, meeting_id = str(uuid.uuid4()), str(uuid.uuid4())
    db = FakeDatabase()
    db.add_meeting(org_id, meeting_id, region="eu")
    provider = FlakyProvider()
    tracker = SessionTracker(provider=provider, session_factory=db, redis=FakeRedis())  # type: ignore[arg-type]

    participant = str(uuid.uuid4())
    lease = await tracker.attach_participant(
        org_id=org_id, meeting_id=meeting_id, participant_identity=participant
    )
    await lease.stream.send_audio(FRAME)  # 20 ms of audio into generation 0
    provider.streams[0].emit(_u(0, "before the drop"))
    await asyncio.sleep(0.01)
    provider.streams[0].drop()
    await lease.stream.send_audio(FRAME)  # triggers / joins the reconnect
    await asyncio.sleep(0.3)  # default backoff: first attempt within 0.125-0.25 s
    assert len(provider.streams) == 2
    provider.streams[1].emit(_u(0, "after the drop"))
    await asyncio.sleep(0.01)
    await tracker.detach_participant(lease)

    texts = [row["text"] for row in db.transcript_chunks]
    assert texts == ["before the drop", "after the drop"]
    assert [r["start_ms"] for r in db.transcript_chunks] == [0, FRAME_MS]
    assert all(r["_rls_org"] == org_id for r in db.transcript_chunks + db.outbox)
    assert len(db.outbox) == 2
    assert provider.regions == ["eu", "eu"]
    assert {r["speaker_id"] for r in db.transcript_chunks} == {participant}
    assert provider.streams[1].frames == [FRAME]


# ---------------------------------------------------------------- reconnect-SLO harness


async def test_reconnect_slo_harness_many_streams_drop_at_once() -> None:
    """Fifty live streams lose their vendor connection in the same instant (a vendor-side
    incident or a NAT reset). Real `asyncio.sleep`, default backoff. Every stream must accept
    audio again inside the caption SLO, and the reconnects must be spread by jitter rather
    than land in lockstep (a thundering herd against the vendor)."""
    n = 50
    providers = [FlakyProvider() for _ in range(n)]
    streams = [await _open(p) for p in providers]

    dropped_at = time.monotonic()
    for p in providers:
        p.streams[0].drop()
    await asyncio.gather(*(s.send_audio(FRAME) for s in streams))
    recovered = [p.open_times[1] - dropped_at for p in providers]

    assert all(len(p.streams) == 2 for p in providers)
    assert all(p.streams[1].frames == [FRAME] for p in providers)
    assert max(recovered) < CAPTION_SLO_SECONDS, f"slowest reconnect {max(recovered):.3f}s"
    spread = max(recovered) - min(recovered)
    assert spread > 0.02, f"reconnects in lockstep (spread {spread:.4f}s)"
    for s in streams:
        await s.close()
