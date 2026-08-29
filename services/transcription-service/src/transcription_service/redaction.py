"""Compliance-gate condition 2: at-rest PII redaction (plan 3.5 point 2).

Runs on FINALS ONLY, before persisting `transcript_chunks`, gated by
`tenant_settings.pii_redaction_enabled` (read in `tenancy.py`). Deepgram receives raw audio -
there is nothing to redact pre-ASR, per the plan's own note; this is the transcript-at-rest
stage.

This module is the DETERMINISTIC layer: regex plus validators, no model, no network, no
dependency of any kind. It covers email, phone, IBAN, card (both via Luhn), and a government-ID
shape. It is the first pass and it does not go away now that NER exists - [ADR-0020](
../../../../../mytheus-docs/adr/0020-pii-token-vault-and-ner-placement.md)'s consequences say so
explicitly, and the reason is availability: this layer cannot fail, so when `ner-service` is
unreachable a tenant on the `regex_only` policy still gets every structured identifier removed.

WHAT CHANGED, AND WHY THE SHAPE IS NOW SPANS

This module used to substitute placeholders inline and return `(text, redacted)`. It now
detects SPANS and lets `pii_pipeline.py` do the substitution. That is not refactoring for its
own sake: the pipeline has to merge these spans with `ner-service`'s before replacing anything,
because the two detectors overlap (a model will happily tag the local part of an email address
as a `PERSON`) and substituting in two passes would splice a placeholder into the middle of
another placeholder. One merge, one substitution.

`redact()` is kept, implemented over the spans, because it is still the correct and complete
answer whenever the vault is unavailable - and because `devseed.py` and the existing tests are
built on it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# ADR-0020 D3's typed vocabulary. These five are what this layer can prove; `PERSON`, `ORG` and
# `LOC` are exactly the three it cannot and are why `ner-service` exists at all.
EMAIL = "EMAIL"
IBAN = "IBAN"
CARD = "CARD"
GOVID = "GOVID"
PHONE = "PHONE"

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_IBAN_RE = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b")
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
# Runs of 13-19 digits, allowing space/dash separators every few digits (typical card grouping).
_CARD_RE = re.compile(r"\b(?:\d[ -]?){12,18}\d\b")
# Phone: an optional leading +, then 7-15 digits with optional separators. Ordered LAST so it
# never eats digits a higher-confidence pattern already claimed.
_PHONE_RE = re.compile(r"(?<!\S)(\+?\d[\d\-.\s()]{6,}\d)(?!\S)")


@dataclass(frozen=True)
class Span:
    """A detected entity, as character offsets into the text it was detected in.

    Offsets rather than the matched string, because a caller that re-finds the substring
    replaces the FIRST occurrence rather than the one that was detected - so a transcript
    saying a name twice gets one occurrence redacted and the other left in place.
    """

    entity_type: str
    start: int
    end: int
    text: str


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


def detect_structured(text: str) -> list[Span]:
    """Every structured identifier this layer can prove, as non-overlapping spans in document
    order.

    Pattern ORDER is the precision mechanism and has to be preserved: email, then IBAN, then
    card (Luhn-checked), then government ID, then phone. A phone pattern will happily match the
    digits of a card number, so the looser patterns run last and any span they produce that
    overlaps an already-accepted one is discarded. This reproduces exactly what the previous
    sequential-substitution implementation achieved by consuming the text as it went.
    """
    accepted: list[Span] = []

    def _claim(entity_type: str, match: re.Match[str]) -> None:
        start, end = match.start(), match.end()
        if any(start < s.end and s.start < end for s in accepted):
            return
        accepted.append(Span(entity_type, start, end, text[start:end]))

    for match in _EMAIL_RE.finditer(text):
        _claim(EMAIL, match)
    for match in _IBAN_RE.finditer(text):
        _claim(IBAN, match)
    for match in _CARD_RE.finditer(text):
        # Luhn is what separates a card number from any other run of digits - an invoice
        # number, a phone number written without separators, a long ID. Without it this
        # pattern would redact most numbers in a transcript.
        digits = re.sub(r"[ -]", "", match.group(0))
        if 13 <= len(digits) <= 19 and _luhn_valid(digits):
            _claim(CARD, match)
    for match in _SSN_RE.finditer(text):
        _claim(GOVID, match)
    for match in _PHONE_RE.finditer(text):
        _claim(PHONE, match)

    accepted.sort(key=lambda s: s.start)
    return accepted


def apply_spans(text: str, replacements: list[tuple[Span, str]]) -> str:
    """Substitute spans right-to-left so earlier offsets stay valid as the string changes.

    Left-to-right would invalidate every subsequent offset the moment a replacement changed the
    length of the text, which is the single most common way a span-based redactor silently
    corrupts its output.
    """
    out = text
    for span, placeholder in sorted(replacements, key=lambda pair: pair[0].start, reverse=True):
        out = out[: span.start] + placeholder + out[span.end :]
    return out


def redact(text: str) -> tuple[str, bool]:
    """The irreversible deterministic redaction: `(possibly_redacted_text, redacted)`.

    This is the behaviour that has shipped since Plan 3-4 and it remains the fallback whenever
    the vault cannot be reached (`pii_pipeline.py`). Placeholders are plain type tags, so the
    raw value is discarded and nothing can reverse them - which is a real limitation and also
    exactly why it is SAFE to fall back to: it is never weaker than storing the value.
    """
    spans = detect_structured(text)
    if not spans:
        return text, False
    return apply_spans(text, [(s, f"‹{s.entity_type}›") for s in spans]), True
