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

import json
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from urllib.parse import urlencode

import websockets

from ..compliance import assert_compliance_gate_green
from ..residency import deepgram_endpoint_for_region
from .base import TranscriptionProvider, TranscriptionStream, Utterance


@dataclass
class _DeepgramStream:
    _ws: (
        object  # websockets.ClientConnection — typed loosely; never exercised, see module docstring
    )

    async def utterances(self) -> AsyncIterator[Utterance]:
        # Deepgram's streaming response shape: {"is_final": bool, "channel": {"alternatives":
        # [{"transcript": str, "confidence": float}]}, "start": float, "duration": float,
        # "speaker"/"words[].speaker" for diarization}. Real parsing lives here so the shape
        # is correct once the gate opens; unexercised, per module docstring.
        async for raw in self._ws:  # type: ignore[attr-defined]
            msg = json.loads(raw)
            alt = msg.get("channel", {}).get("alternatives", [{}])[0]
            words = alt.get("words", [])
            speaker = str(words[0]["speaker"]) if words and "speaker" in words[0] else "0"
            start_ms = int(msg.get("start", 0) * 1000)
            duration_ms = int(msg.get("duration", 0) * 1000)
            # `request_id` (Deepgram's own session identifier) is NOT a UUID and is constant
            # across every message in one streaming session, not unique per utterance — it
            # cannot be used directly as `chunk_id` (fix round 1, item 1: this crashed every
            # real INSERT with "invalid input for query argument: invalid UUID"). `chunk_id`
            # is derived deterministically via uuid5 so the SAME (request_id, start_ms) pair
            # always yields the SAME valid UUID — required for `chunk:{chunk_id}` idempotency
            # to mean anything on redelivery of the identical message. The raw `request_id`
            # is preserved separately on `provider_correlation_id` for support/debugging
            # correlation against Deepgram's own dashboard, which the derived UUID does not
            # give up (uuid5 is one-way for this purpose; that's fine, it's not meant to be
            # decoded, only reproduced from the same inputs).
            request_id = msg.get("request_id", "")
            chunk_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"deepgram:{request_id}:{start_ms}"))
            yield Utterance(
                chunk_id=chunk_id,
                speaker_id=speaker,
                start_ms=start_ms,
                end_ms=start_ms + duration_ms,
                text=alt.get("transcript", ""),
                is_final=bool(msg.get("is_final", False)),
                confidence=float(alt.get("confidence", 0.0)),
                provider_correlation_id=request_id or None,
            )

    async def close(self) -> None:
        await self._ws.close()  # type: ignore[attr-defined]


class DeepgramProvider:
    """The Deepgram WSS proxy. `api_key` stays server-side: it is only ever used to build the
    `Authorization` header of the outbound WSS handshake to Deepgram, and is never logged, never
    echoed in an exception message, and never included in anything returned to a caller — see
    `tests/test_deepgram_key_never_leaks.py`.
    """

    def __init__(self, *, api_key: str) -> None:
        self._api_key = api_key

    async def open_stream(
        self, *, org_id: str, meeting_id: str, region: str, diarize: bool = True
    ) -> TranscriptionStream:
        # THE enforcement point (compliance.py docstring): checked first, before anything else
        # touches the network or the key.
        assert_compliance_gate_green()

        endpoint = deepgram_endpoint_for_region(region)
        params = {"diarize": "true" if diarize else "false", "interim_results": "true"}
        url = f"{endpoint}?{urlencode(params)}"
        ws = await websockets.connect(
            url, additional_headers={"Authorization": f"Token {self._api_key}"}
        )
        return _DeepgramStream(_ws=ws)


def _assert_conforms(_cls: type[TranscriptionProvider]) -> None:
    """No-op at runtime; makes mypy check this class against the Protocol so a signature
    drift is caught at the point it happens, not the point something calls it."""


_assert_conforms(DeepgramProvider)
