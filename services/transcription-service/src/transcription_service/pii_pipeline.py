"""The full redaction pass: deterministic layer + NER + reversible token vault.

This is the transcript-at-rest boundary, the first of the three [Compliance §5.1](
../../../../../mytheus-docs/12-compliance-and-privacy.md) names, now built to the whole of §5.2
rather than to its structured half. It assembles the result itself and calls out for the two
pieces it must not own - the model (`ner-service`) and the vault
(`identity-tenant-service`) - exactly as [ADR-0020](
../../../../../mytheus-docs/adr/0020-pii-token-vault-and-ner-placement.md) D5 requires.

THE FOUR OUTCOMES, AND WHY NONE OF THEM IS EVER WORSE THAN WHAT SHIPPED BEFORE

| NER | vault | result |
| --- | --- | --- |
| up | up | full reversible redaction: `‹PERSON:7f3a›`, resolvable through the vault |
| up | down | full IRREVERSIBLE redaction: `‹PERSON›`, raw value discarded |
| down | up | tenant policy decides: `drop` the final, or persist regex-only and FLAG it |
| down | down | same policy decision, with irreversible placeholders |

Every degraded cell is at least as protective as the pre-vault behaviour. That property is what
makes it safe for the vault to REFUSE to mint under a development-only key provider (ADR-0024
§ The production condition): the refusal costs reversibility, never protection. Persisting raw
text is not reachable from any path here, because ADR-0020 rejected it outright.

WHY THE TWO DETECTORS' SPANS ARE MERGED BEFORE ANYTHING IS SUBSTITUTED

They overlap in practice. A model will tag the local part of `jane.doe@acme.com` as a `PERSON`,
and the deterministic layer will tag the whole address as an `EMAIL`. Substituting in two passes
splices one placeholder into the middle of another and produces text like `‹EMAIL:a1›` with
`‹PERSON:b2›` embedded in it - unreadable, unresolvable, and wrong. So: merge, resolve overlaps
in favour of the deterministic span, substitute once, right to left.

The deterministic span wins an overlap on purpose. It is a proof (a Luhn check, an RFC-shaped
address), where the model's is a probability, and preferring the proof also keeps the entity
TYPE right - an email tagged `PERSON` would mint into the wrong token space and never cohere
with the same address seen by `ingestion-service`.
"""

from __future__ import annotations

from dataclasses import dataclass

from .pii_clients import detect_entities, mint_tokens, ner_configured, vault_configured
from .redaction import Span, apply_spans, detect_structured

# `tenant_settings.ner_unavailable_policy`. ADR-0020 § The unavailable-NER decision assigned the
# choice to Security/Compliance and noted it may differ per tenant; ADR-0024 made it a column so
# their answer is configuration, not a code change.
POLICY_DROP = "drop"
POLICY_REGEX_ONLY = "regex_only"


@dataclass(frozen=True)
class RedactionResult:
    """`text is None` means DROP THE FINAL - the tenant's policy chose losing the record over
    persisting one whose unstructured entities were never inspected."""

    text: str | None
    redacted: bool
    partially_redacted: bool


def _merge(structured: list[Span], detected: list[Span]) -> list[Span]:
    """Deterministic spans win overlaps; the rest are kept, in document order."""
    merged = list(structured)
    for span in detected:
        if any(span.start < kept.end and kept.start < span.end for kept in merged):
            continue
        merged.append(span)
    merged.sort(key=lambda s: s.start)
    return merged


async def redact_final(org_id: str, text: str) -> RedactionResult:
    """Run the full pass over ONE transcript final. Called only when the tenant has
    `pii_redaction_enabled`; the caller does not call this at all when they do not."""
    structured = detect_structured(text)

    detected = await detect_entities(org_id, text) if ner_configured() else None
    ner_ran = detected is not None
    spans = _merge(structured, detected or [])

    if not spans:
        # Nothing to redact. `partially_redacted` still reflects whether NER actually ran: a
        # row that no model ever looked at is not the same fact as a row a model cleared, and
        # conflating them would make the flag useless for the audit it exists to serve.
        return RedactionResult(text=text, redacted=False, partially_redacted=not ner_ran)

    minted = await mint_tokens(org_id, spans) if vault_configured() else None
    if minted is not None:
        replacements = [
            (span, f"‹{token.entity_type}:{token.token}›")
            for span, token in zip(spans, minted, strict=True)
        ]
    else:
        # The pre-vault behaviour: typed but irreversible, raw value discarded.
        replacements = [(span, f"‹{span.entity_type}›") for span in spans]

    return RedactionResult(
        text=apply_spans(text, replacements),
        redacted=True,
        partially_redacted=not ner_ran,
    )


async def redact_final_with_policy(
    org_id: str, text: str, ner_unavailable_policy: str
) -> RedactionResult:
    """`redact_final` plus the tenant's answer to "what if the model was not reachable".

    `drop` is honoured only when NER genuinely did not run. It is deliberately NOT applied when
    the vault is the thing that failed: losing a meeting's record buys nothing there, because
    the text is already fully redacted - just irreversibly.
    """
    result = await redact_final(org_id, text)
    if result.partially_redacted and ner_unavailable_policy == POLICY_DROP:
        return RedactionResult(text=None, redacted=result.redacted, partially_redacted=True)
    return result
