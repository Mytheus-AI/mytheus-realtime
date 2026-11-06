"""`ReconnectingStream`: a `TranscriptionStream` that survives a provider drop.

A live Deepgram WSS session can die mid-meeting (network blip, vendor 1011, a pod-side TCP
reset). Without this wrapper one drop ends transcription for the rest of the meeting: the
pump's iterator raises, the pump logs and exits, and every later audio frame goes nowhere.

Behaviour, all of it covered by `tests/test_reconnect.py`:

- Only `ProviderStreamDropped` is retried. A clean end of the inner `utterances()` means the
  stream is over (e.g. `FakeProvider`'s finite script, or our own `close()`), and
  `ComplianceGateClosedError` is never retried -- a closed gate is a legal fact, not a fault.
- Exponential backoff with jitter (`base_delay * 2**attempt`, capped at `max_delay`, scaled
  by a random factor in [0.5, 1.0]) so a vendor-wide outage does not see every one of this
  pod's streams reconnect in lockstep. After `max_attempts` consecutive failures the stream
  gives up with `ProviderReconnectFailed`.
- Exactly one reconnect per drop. Both halves (`send_audio` and `utterances()`) can observe
  the same drop; a generation counter under a lock makes the second observer reuse the
  connection the first one opened instead of opening another.
- Ordering. Audio: `send_audio` is called by one sender in frame order, and the frame whose
  send failed is retried FIRST on the new connection, so no frame is reordered or skipped.
  Utterances: a single generator drains the old connection to its end before it reads the
  new one, so results never interleave across connections. Timestamps: a fresh vendor
  session restarts its clock at 0, so each new generation's `start_ms`/`end_ms` are shifted
  by the audio duration already sent to earlier generations, keeping `start_ms` monotonic
  across the reconnect. (Approximate by up to the in-flight audio the dead session had not
  yet transcribed; that audio is lost with the vendor session and cannot be recovered.)
  `chunk_id` is left exactly as the binding derived it: Deepgram's derivation includes the
  per-session `request_id`, so a new session can never collide with an old one.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import replace

from ..compliance import ComplianceGateClosedError
from .base import (
    DEFAULT_SAMPLE_RATE,
    ProviderStreamDropped,
    TranscriptionStream,
    Utterance,
)

logger = logging.getLogger(__name__)

# Two bytes per sample (s16le), mono.
_BYTES_PER_SAMPLE = 2


class ProviderReconnectFailed(Exception):
    """Every reconnect attempt failed; the stream is closed and will not recover."""


class StreamClosedError(Exception):
    """`send_audio` was called on a stream that has been closed (e.g. the meeting ended)."""


class ReconnectingStream:
    def __init__(
        self,
        open_inner: Callable[[], Awaitable[TranscriptionStream]],
        inner: TranscriptionStream,
        *,
        sample_rate: int = DEFAULT_SAMPLE_RATE,
        max_attempts: int = 5,
        base_delay: float = 0.25,
        max_delay: float = 5.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self._open_inner = open_inner
        self._inner = inner
        self._bytes_per_ms = sample_rate * _BYTES_PER_SAMPLE / 1000
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._sleep = sleep
        self._jitter = jitter
        self._generation = 0
        self._offset_ms = 0  # applied to utterances of the CURRENT generation
        self._bytes_this_generation = 0
        self._lock = asyncio.Lock()
        self._closed = False
        self._failed: ProviderReconnectFailed | None = None
        self.reconnects = 0

    @classmethod
    async def open(
        cls,
        open_inner: Callable[[], Awaitable[TranscriptionStream]],
        **kwargs: object,
    ) -> ReconnectingStream:
        """Opens the first inner stream. A failure here (including the compliance gate)
        propagates unchanged: there is no session to keep alive yet."""
        inner = await open_inner()
        return cls(open_inner, inner, **kwargs)  # type: ignore[arg-type]

    @property
    def closed(self) -> bool:
        return self._closed

    async def send_audio(self, frame: bytes) -> None:
        while True:
            if self._failed is not None:
                raise self._failed
            if self._closed:
                raise StreamClosedError("transcription stream is closed")
            generation, inner = self._generation, self._inner
            try:
                await inner.send_audio(frame)
            except ProviderStreamDropped:
                await self._reconnect(generation)
                continue  # retry THIS frame first on the new connection: order preserved
            if generation == self._generation:
                self._bytes_this_generation += len(frame)
            return

    async def utterances(self) -> AsyncIterator[Utterance]:
        while not self._closed:
            generation, inner, offset = self._generation, self._inner, self._offset_ms
            try:
                async for utterance in inner.utterances():
                    yield _shift(utterance, offset)
            except ProviderStreamDropped:
                if self._closed:
                    return
                await self._reconnect(generation)
                continue
            # Clean end. If the send side already swapped in a new connection (and closed this
            # one, which is what ended the iterator), keep going on the new one.
            if self._closed or self._generation == generation:
                return

    async def close(self) -> None:
        self._closed = True
        try:
            await self._inner.close()
        except Exception:  # noqa: BLE001 - best effort; the session is being torn down
            logger.warning("transcription stream close raised", exc_info=True)

    async def _reconnect(self, failed_generation: int) -> None:
        async with self._lock:
            if self._failed is not None:
                raise self._failed
            if self._closed or self._generation != failed_generation:
                return  # someone else already reconnected (or we are shutting down)
            try:
                await self._inner.close()
            except Exception:  # noqa: BLE001 - the old connection is already dead
                pass
            for attempt in range(self._max_attempts):
                delay = min(self._max_delay, self._base_delay * (2**attempt))
                await self._sleep(delay * (0.5 + self._jitter() / 2))
                if self._closed:
                    return
                try:
                    new_inner = await self._open_inner()
                except ComplianceGateClosedError:
                    # Terminal: the gate is a legal fact, never retried, never re-opened.
                    self._closed = True
                    raise
                except Exception:  # noqa: BLE001 - any open failure is retried with backoff
                    logger.warning(
                        "transcription provider reconnect attempt %d/%d failed",
                        attempt + 1,
                        self._max_attempts,
                        exc_info=True,
                    )
                    continue
                self._offset_ms += int(self._bytes_this_generation / self._bytes_per_ms)
                self._bytes_this_generation = 0
                self._inner = new_inner
                self._generation += 1
                self.reconnects += 1
                logger.info(
                    "transcription provider stream reconnected (reconnect #%d, attempt %d)",
                    self.reconnects,
                    attempt + 1,
                )
                return
            self._closed = True
            self._failed = ProviderReconnectFailed(
                f"transcription provider unavailable after {self._max_attempts} reconnect attempts"
            )
            raise self._failed


def _shift(utterance: Utterance, offset_ms: int) -> Utterance:
    if offset_ms == 0:
        return utterance
    return replace(
        utterance,
        start_ms=utterance.start_ms + offset_ms,
        end_ms=utterance.end_ms + offset_ms,
    )


def _assert_conforms(_obj: type[TranscriptionStream]) -> None:
    """No-op at runtime; makes mypy check this class against the Protocol."""


_assert_conforms(ReconnectingStream)
