"""`_DeepgramStream`'s audio half, against a fake socket (no network, gate untouched):
frames are forwarded verbatim, an abnormal vendor close becomes the retryable
`ProviderStreamDropped` on both halves, a clean close just ends iteration, `close()` sends
Deepgram's `CloseStream`, and non-`Results` messages are not turned into utterances.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from websockets.frames import Close

from transcription_service.providers.base import ProviderStreamDropped
from transcription_service.providers.deepgram import _DeepgramStream


class FakeWs:
    def __init__(self, messages: list[str], end: BaseException | None = None) -> None:
        self.messages = messages
        self.end = end
        self.sent: list[Any] = []
        self.closed = False
        self.send_error: BaseException | None = None

    def __aiter__(self) -> FakeWs:
        return self

    async def __anext__(self) -> str:
        if self.messages:
            return self.messages.pop(0)
        # Mirrors `websockets`' own `__aiter__`: a clean close (ConnectionClosedOK) ends
        # iteration; an abnormal one (ConnectionClosedError) propagates.
        if isinstance(self.end, ConnectionClosedOK):
            raise StopAsyncIteration
        if self.end is not None:
            raise self.end
        raise StopAsyncIteration

    async def send(self, data: Any) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(data)

    async def close(self) -> None:
        self.closed = True


def _abnormal() -> ConnectionClosedError:
    return ConnectionClosedError(Close(1011, "internal"), None)


def _result(text: str) -> str:
    return json.dumps(
        {
            "type": "Results",
            "request_id": "req-1",
            "is_final": True,
            "start": 0.0,
            "duration": 0.5,
            "channel": {"alternatives": [{"transcript": text, "confidence": 0.9, "words": []}]},
        }
    )


async def test_send_audio_forwards_bytes_verbatim() -> None:
    ws = FakeWs([])
    await _DeepgramStream(_ws=ws).send_audio(b"\x01\x02")
    assert ws.sent == [b"\x01\x02"]


async def test_abnormal_close_on_send_is_a_drop() -> None:
    ws = FakeWs([])
    ws.send_error = _abnormal()
    with pytest.raises(ProviderStreamDropped):
        await _DeepgramStream(_ws=ws).send_audio(b"\x00\x00")


async def test_abnormal_close_mid_stream_is_a_drop_after_yielding_what_arrived() -> None:
    stream = _DeepgramStream(_ws=FakeWs([_result("hi")], end=_abnormal()))
    seen = []
    with pytest.raises(ProviderStreamDropped):
        async for u in stream.utterances():
            seen.append(u.text)
    assert seen == ["hi"]


async def test_clean_close_ends_iteration_without_a_drop() -> None:
    ok = ConnectionClosedOK(Close(1000, ""), Close(1000, ""), True)
    stream = _DeepgramStream(_ws=FakeWs([_result("bye")], end=ok))
    assert [u.text async for u in stream.utterances()] == ["bye"]


async def test_non_results_messages_are_skipped() -> None:
    meta = json.dumps({"type": "Metadata", "request_id": "req-1"})
    stream = _DeepgramStream(_ws=FakeWs([meta, _result("x")]))
    assert [u.text async for u in stream.utterances()] == ["x"]


async def test_close_sends_close_stream_then_closes() -> None:
    ws = FakeWs([])
    await _DeepgramStream(_ws=ws).close()
    assert ws.sent == [json.dumps({"type": "CloseStream"})]
    assert ws.closed
