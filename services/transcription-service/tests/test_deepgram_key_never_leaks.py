"""Acceptance check: "The Deepgram key is never present in any client-facing payload or log
(grep + log-scrub test)." Two halves: a static grep over the source tree for the pattern that
would leak a key into a log/response (an f-string or `%s`-formatted log call that names
`api_key`/`_api_key` next to a logger/print/response call), and a runtime log-scrub test that
exercises the actual code paths that hold the key and asserts the real secret value never
appears in captured logs or in the exception raised when the gate refuses to open.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pytest

from transcription_service.compliance import ComplianceGateClosedError
from transcription_service.providers.deepgram import DeepgramProvider

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"

# Anything that logs/prints a whole DeepgramProvider (whose repr would include `_api_key` via
# dataclass-style repr) or interpolates `_api_key`/`api_key` directly into a log/print/response
# call. `self` alone in a log call is fine (this class has no auto-generated `__repr__` that
# includes the key -- it's a plain class, not a `@dataclass`, precisely to avoid an accidental
# `repr()` leak); this scan is defense-in-depth against a future edit reintroducing one.
_SUSPECT_PATTERN = re.compile(r"(log(ger)?\.\w+|print)\([^)]*\b(_api_key|api_key)\b")


def test_source_never_logs_the_deepgram_key() -> None:
    offenders = []
    for path in SRC_ROOT.rglob("*.py"):
        content = path.read_text()
        for match in _SUSPECT_PATTERN.finditer(content):
            offenders.append(f"{path}: {match.group(0)}")
    assert offenders == [], f"found log/print calls that may leak the Deepgram key: {offenders}"


def test_deepgram_provider_has_no_auto_repr_leak() -> None:
    """A `@dataclass`'s default `__repr__` would print every field, including `_api_key`. This
    class is deliberately a plain class with a private attribute so `repr(provider)` does not
    include the key."""
    provider = DeepgramProvider(api_key="dg_super_secret_value_12345")
    assert "dg_super_secret_value_12345" not in repr(provider)


@pytest.mark.asyncio
async def test_gate_refusal_error_never_contains_the_key(caplog: pytest.LogCaptureFixture) -> None:
    secret = "dg_super_secret_value_98765"
    provider = DeepgramProvider(api_key=secret)
    caplog.set_level(logging.DEBUG)
    with pytest.raises(ComplianceGateClosedError) as exc_info:
        await provider.open_stream(org_id="org-1", meeting_id="meeting-1", region="us")

    assert secret not in str(exc_info.value)
    for record in caplog.records:
        assert secret not in record.getMessage()
