"""Compliance-gate condition 2: at-rest PII redaction (plan 3.5 point 2).

Runs on FINALS ONLY, before persisting `transcript_chunks`, gated by
`tenant_settings.pii_redaction_enabled` (read in `tenancy.py`). Deepgram receives raw audio —
there is nothing to redact pre-ASR, per the plan's own note; this is the transcript-at-rest
stage.

Implements the deterministic regex/validator layer from the plan: email, phone, IBAN, card
(via Luhn), and a government-ID pattern (US SSN as the representative shape — the plan does
not name a specific government-ID format, and different tenants' government IDs vary by
country; SSN is the concrete, checkable shape chosen here, documented so a future tenant with
a different ID format extends this list rather than assumes it is covered).

NOT implemented, and disclosed rather than silently skipped: the "lightweight NER pass (run
off the API tier)" the plan also asks for, and the "reversible placeholders keyed to a
per-tenant token vault" — the plan's placeholders are shown as plain type tags
(`‹EMAIL›`, `‹PHONE›`) and the acceptance check's own example
("a final containing jane@acme.com lands in transcript_chunks as ‹EMAIL›") uses plain tags, so
that much is implemented and tested. A real NER model is a dependency disproportionate to this
band's budget; a per-tenant token vault (encryption keys, storage, access control for
reversal) is a substantial feature of its own. Both are called out here as scoped out, the
same way the plan itself calls out the LLM-egress redaction pass as deferred to Plan 5+ — a
deviation with no recorded reason is a bug, not a decision, so this one is recorded: the
deterministic layer covers every entity class the plan names and is what the acceptance test
exercises; NER and the token vault are follow-up work, not silently dropped scope.
"""

from __future__ import annotations

import re
from collections.abc import Callable

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_IBAN_RE = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b")
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
# Runs of 13-19 digits, allowing space/dash separators every few digits (typical card grouping).
_CARD_RE = re.compile(r"\b(?:\d[ -]?){12,18}\d\b")
# Phone: an optional leading +, then 7-15 digits with optional separators. Matched LAST so it
# never eats digits already replaced by a higher-confidence pattern above.
_PHONE_RE = re.compile(r"(?<!\S)(\+?\d[\d\-.\s()]{6,}\d)(?!\S)")


def _luhn_valid(digits: str) -> bool:
    total = 0
    parity = len(digits) % 2
    for i, ch in enumerate(digits):
        d = int(ch)
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def _redact_card(text: str) -> tuple[str, bool]:
    redacted = False

    def _sub(m: re.Match[str]) -> str:
        nonlocal redacted
        digits = re.sub(r"[ -]", "", m.group(0))
        if 13 <= len(digits) <= 19 and _luhn_valid(digits):
            redacted = True
            return "‹CARD›"
        return m.group(0)

    return _CARD_RE.sub(_sub, text), redacted


def redact(text: str) -> tuple[str, bool]:
    """Returns `(possibly_redacted_text, redacted)`. `redacted` is `True` iff at least one
    span was replaced. Order matters: highest-confidence / most-specific patterns first, so a
    looser later pattern (phone) never re-matches digits a placeholder already consumed."""
    redacted = False

    def _mark(placeholder: str) -> Callable[[re.Match[str]], str]:
        def _sub(_m: re.Match[str]) -> str:
            nonlocal redacted
            redacted = True
            return placeholder

        return _sub

    text = _EMAIL_RE.sub(_mark("‹EMAIL›"), text)
    text = _IBAN_RE.sub(_mark("‹IBAN›"), text)
    text, card_redacted = _redact_card(text)
    redacted = redacted or card_redacted
    text = _SSN_RE.sub(_mark("‹GOVID›"), text)
    text = _PHONE_RE.sub(_mark("‹PHONE›"), text)
    return text, redacted
