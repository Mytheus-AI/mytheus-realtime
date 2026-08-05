"""The compliance gate (plan 3.5) — must be GREEN before any Deepgram stream opens against
real audio.

Three conditions, per `mytheus-docs/12-compliance-and-privacy.md` section 4-6:

1. Deepgram DPA + SCCs signed and on file (zero-retention / no-model-training vendor flag
   set), subprocessor register updated. A LEGAL artifact. It does not exist yet and cannot be
   produced by writing code — no PR against this repo can make condition 1 true.
2. At-rest PII redaction node live in the ingestion path, gated by
   `tenant_settings.pii_redaction_enabled`. Code. Implemented in `redaction.py` and wired into
   `persistence.py`; always live regardless of this gate, because it protects data that DOES
   get persisted once finals start flowing.
3. `data_residency.region` pin enforced (regional Deepgram endpoint, region-local storage).
   Code. Implemented in `residency.py`.

Because condition 1 is unmet, the plan's own consequence applies verbatim: "the Deepgram WSS
client is feature-flagged off and `transcription-service` refuses to open a stream against
real audio." `COMPLIANCE_GATE_GREEN` is that feature flag, and it is a SOURCE CONSTANT, not an
environment variable or a database row: nothing in this service's configuration surface can
flip it. Condition 1 is a legal fact, not an ops toggle — a `DEEPGRAM_COMPLIANCE_GATE=true`
env var would let anyone with deploy access override a fact about whether a contract has been
signed, which defeats the point of gating on it at all. Turning this to `True` requires editing
this file and getting the change past code review, once Legal confirms the DPA/SCC are on file
and the subprocessor register lists Deepgram — the correct level of ceremony for a compliance
gate. See `mytheus-docs/design/subprocessors.md`.

`DeepgramProvider.open_stream` (providers/deepgram.py) calls `assert_compliance_gate_green()`
as the FIRST thing it does, before touching the network, the API key, or any audio — so the
refusal holds even if a caller somehow got a `DeepgramProvider` instance directly instead of
going through `providers.factory.select_provider()`. There is no code path from "a
`DeepgramProvider` exists" to "a real Deepgram WSS connection opens" that does not pass through
this check.
"""

from __future__ import annotations

from typing import Final

COMPLIANCE_GATE_GREEN: Final[bool] = False


class ComplianceGateClosedError(RuntimeError):
    """Raised when something attempts to open a real Deepgram stream while the gate is closed."""


def assert_compliance_gate_green() -> None:
    """The one enforcement point. Raises unless `COMPLIANCE_GATE_GREEN` is `True`.

    Deliberately does not accept an override parameter of any kind (no `force=True`, no
    config lookup) — the only way to change this function's behavior is to change the
    constant above, in source, under review.
    """
    if not COMPLIANCE_GATE_GREEN:
        raise ComplianceGateClosedError(
            "compliance gate is not green: condition 1 (signed Deepgram DPA/SCC on file, "
            "zero-retention flag set, subprocessor register updated) is unmet. Refusing to "
            "open a real Deepgram stream. See mytheus-docs/12-compliance-and-privacy.md "
            "section 4-6 and mytheus-docs/design/subprocessors.md."
        )
