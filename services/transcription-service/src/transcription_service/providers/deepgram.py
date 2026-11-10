"""`DeepgramProvider` — LOCKED DEFAULT per the plan. Streaming WSS, diarization, interim+final.
Key server-side, never sent to a client.

Written for real, exactly as the plan specifies (plan 3.6 steps 3-4) — it is NOT stubbed-out
placeholder code. It is simply not exercisable end-to-end today, for the same reason
`LiveKitTransport` in `mytheus-backend/services/meeting_service/transport.py` is not: there is
no signed Deepgram DPA/SCC (compliance gate condition 1, `compliance.py`), so
`open_stream` refuses before it ever reaches the network. No test in this build claims
coverage of a real Deepgram WSS session; that is BLOCKED, not green-against-a-fake.

Uses the bare `websockets` library (already a scaffold dependency) to speak Deepgram's
documented WSS streaming protocol directly, rather than adding the `deepgram-sdk` package —
pulling in a whole vendor SDK for a code path that cannot run yet, and stays a thin proxy
even once it can, is a dependency this band does not need. If the SDK's ergonomics matter
once the gate goes green, swapping this file's internals for `deepgram-sdk` is a one-file
change; nothing else in this repo imports Deepgram.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from urllib.parse import urlencode

import websockets
from websockets.exceptions import ConnectionClosedError

from ..compliance import assert_compliance_gate_green
from ..org_context import OrgKeyterms, keyterm_params, keyterms_supported
from ..residency import deepgram_endpoint_for_region
from .base import (
    DEFAULT_ENCODING,
    DEFAULT_SAMPLE_RATE,
    ProviderStreamDropped,
    TranscriptionProvider,
    TranscriptionStream,
    Utterance,
)

logger = logging.getLogger(__name__)

# Deepgram's documented end-of-stream control message: flushes any pending final and then
# closes the socket cleanly (1000), which ends `utterances()` without a drop.
_CLOSE_STREAM_MESSAGE = json.dumps({"type": "CloseStream"})
_CLOSE_FLUSH_TIMEOUT_SECONDS = 1.5


@dataclass
class _DeepgramStream:
    _ws: (
        object  # websockets.ClientConnection — typed loosely; never exercised, see module docstring
    )

    async def utterances(self) -> AsyncIterator[Utterance]:
        # An abnormal close (`ConnectionClosedError`: network loss, Deepgram's 1011 on an
        # internal error or the NET-0001 no-audio timeout) is surfaced as the retryable
        # `ProviderStreamDropped`; a clean close (`ConnectionClosedOK`, e.g. after our own
        # CloseStream) just ends iteration. See `providers/base.py`.
        try:
            async for utterance in self._parse():
                yield utterance
        except ConnectionClosedError as exc:
            raise ProviderStreamDropped("deepgram stream closed abnormally") from exc

    async def send_audio(self, frame: bytes) -> None:
        try:
            await self._ws.send(frame)  # type: ignore[attr-defined]
        except ConnectionClosedError as exc:
            raise ProviderStreamDropped("deepgram stream closed abnormally") from exc

    async def _parse(self) -> AsyncIterator[Utterance]:
        # Deepgram's streaming response shape: {"is_final": bool, "channel": {"alternatives":
        # [{"transcript": str, "confidence": float}]}, "start": float, "duration": float,
        # "speaker"/"words[].speaker" for diarization}. Real parsing lives here so the shape
        # is correct once the gate opens; unexercised, per module docstring.
        async for raw in self._ws:  # type: ignore[attr-defined]
            msg = json.loads(raw)
            # Deepgram also sends `Metadata`/`SpeechStarted`/`UtteranceEnd` messages on the same
            # socket; only `Results` (or an untyped message, as in older API versions and the
            # existing tests) carries a transcript.
            if msg.get("type", "Results") != "Results":
                continue
            alt = msg.get("channel", {}).get("alternatives", [{}])[0]
            words = alt.get("words", [])
            speaker = str(words[0]["speaker"]) if words and "speaker" in words[0] else "0"
            start_ms = int(msg.get("start", 0) * 1000)
            duration_ms = int(msg.get("duration", 0) * 1000)
            # `request_id` (Deepgram's own session identifier) is NOT a UUID and is constant
            # across every message in one streaming session, not unique per utterance — it
            # cannot be used directly as `chunk_id` (fix round 1, item 1: this crashed every
            # real INSERT with "invalid input for query argument: invalid UUID"). `chunk_id`
            # is derived deterministically via uuid5 so the SAME underlying provider event
            # always yields the SAME valid UUID — required for `chunk:{chunk_id}` idempotency
            # to mean anything on redelivery of the identical message. The raw `request_id`
            # is preserved separately on `provider_correlation_id` for support/debugging
            # correlation against Deepgram's own dashboard, which the derived UUID does not
            # give up (uuid5 is one-way for this purpose; that's fine, it's not meant to be
            # decoded, only reproduced from the same inputs).
            #
            # Fix round 2: (request_id, start_ms) ALONE is not a sufficient key. request_id
            # is constant for the whole session, so it contributes nothing to disambiguation
            # within one session, and two GENUINELY DISTINCT finals can share a start_ms —
            # this is diarized transcription, so two speakers starting to talk at the same
            # millisecond is ordinary overlapping speech, not an edge case. Under the old key
            # the second speaker's final collided with the first's chunk_id and was silently
            # swallowed as a "redelivery" by the outbox's idempotency gate in
            # persistence.py::_write_final — a real transcript line dropped with no error and
            # no log. `speaker_id` is what actually distinguishes concurrent speech, so it is
            # now part of the key; `end_ms` is included too so two utterances from the SAME
            # speaker would additionally need to share both boundaries exactly to collide,
            # which for genuinely distinct utterances they will not. This keeps the
            # determinism property the idempotency contract depends on (same underlying
            # event -> same key) while making a collision between two DISTINCT utterances
            # require the same speaker AND the same start AND the same end, which is what a
            # true redelivery of the identical message looks like, not two different ones.
            request_id = msg.get("request_id", "")
            end_ms = start_ms + duration_ms
            chunk_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL, f"deepgram:{request_id}:{speaker}:{start_ms}:{end_ms}"
                )
            )
            yield Utterance(
                chunk_id=chunk_id,
                speaker_id=speaker,
                start_ms=start_ms,
                end_ms=end_ms,
                text=alt.get("transcript", ""),
                is_final=bool(msg.get("is_final", False)),
                confidence=float(alt.get("confidence", 0.0)),
                provider_correlation_id=request_id or None,
            )

    async def close(self) -> None:
        # CloseStream asks Deepgram to flush the pending final and then close the socket
        # itself; give it a moment to do so (the pump keeps reading meanwhile, see
        # `SessionTracker._stop_participant`) before forcing the close.
        try:
            await self._ws.send(_CLOSE_STREAM_MESSAGE)  # type: ignore[attr-defined]
            wait_closed = getattr(self._ws, "wait_closed", None)
            if wait_closed is not None:
                await asyncio.wait_for(wait_closed(), timeout=_CLOSE_FLUSH_TIMEOUT_SECONDS)
        except Exception:  # noqa: BLE001 - already closed or flush timed out; close anyway
            pass
        await self._ws.close()  # type: ignore[attr-defined]


class DeepgramProvider:
    """The Deepgram WSS proxy. `api_key` stays server-side: it is only ever used to build the
    `Authorization` header of the outbound WSS handshake to Deepgram, and is never logged, never
    echoed in an exception message, and never included in anything returned to a caller — see
    `tests/test_deepgram_key_never_leaks.py`.
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "nova-3",
        language: str = "multi",
        keyterms: OrgKeyterms | None = None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._language = language
        # ADR-0039 D1: the org's vocabulary as Deepgram `keyterm`s. None (no
        # IDENTITY_GRPC_ADDR) or an unsupported model means streams open without them.
        self._keyterms = keyterms
        self._keyterms_unsupported_logged = False

    async def _keyterms_for(self, org_id: str) -> list[str]:
        if self._keyterms is None:
            return []
        if not keyterms_supported(self._model, self._language):
            if not self._keyterms_unsupported_logged:
                self._keyterms_unsupported_logged = True
                logger.warning(
                    "Deepgram keyterm is not supported for model=%s language=%s; "
                    "streams open without company vocabulary",
                    self._model,
                    self._language,
                )
            return []
        # Never raises: any failure is "no key terms" (org_context.py).
        return await self._keyterms.for_org(org_id)

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
        # THE enforcement point (compliance.py docstring): checked first, before anything else
        # touches the network or the key.
        assert_compliance_gate_green()

        endpoint = deepgram_endpoint_for_region(region)
        # Raw PCM carries no container header, so Deepgram must be told the encoding, rate and
        # channel count explicitly or it cannot decode a single frame.
        params = {
            # No model named means Deepgram's older default; Nova-3 is the accurate one, and
            # `multi` handles English/Hindi code-switching in a single stream.
            "model": self._model,
            "language": self._language,
            # Readable captions and transcript lines: punctuation, casing, numbers and dates.
            "punctuate": "true",
            "smart_format": "true",
            # A final after 300 ms of silence keeps finals (and the agent windows built from
            # them) prompt; utterance_end_ms closes a turn even when background noise defeats
            # silence detection. Both need interim_results, which captions use anyway.
            "endpointing": "300",
            "utterance_end_ms": "1000",
            "diarize": "true" if diarize else "false",
            "interim_results": "true",
            "encoding": encoding,
            "sample_rate": str(sample_rate),
            "channels": "1",
        }
        # One `keyterm` per term (repeated parameter); spaces encode as "+", which Deepgram
        # accepts for multi-word terms.
        query = list(params.items()) + keyterm_params(await self._keyterms_for(org_id))
        url = f"{endpoint}?{urlencode(query)}"
        ws = await websockets.connect(
            url, additional_headers={"Authorization": f"Token {self._api_key}"}
        )
        return _DeepgramStream(_ws=ws)


def _assert_conforms(_cls: type[TranscriptionProvider]) -> None:
    """No-op at runtime; makes mypy check this class against the Protocol so a signature
    drift is caught at the point it happens, not the point something calls it."""


_assert_conforms(DeepgramProvider)
