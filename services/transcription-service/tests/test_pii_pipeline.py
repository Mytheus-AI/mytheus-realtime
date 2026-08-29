"""The full redaction pass and, above all, its four degraded outcomes (ADR-0020, ADR-0024).

No database and no network: `pii_clients` is monkeypatched, because what needs proving here is
the POLICY, not HTTP. The property every test below is really defending is the one that makes
this feature safe to ship at all - no path through this pipeline is ever weaker than the
irreversible redaction that shipped before it, and no path persists raw text.
"""

from __future__ import annotations

import uuid

import pytest

from transcription_service import pii_clients, pii_pipeline
from transcription_service.pii_pipeline import (
    POLICY_DROP,
    POLICY_REGEX_ONLY,
    redact_final,
    redact_final_with_policy,
)
from transcription_service.redaction import Span

ORG = str(uuid.uuid4())


@pytest.fixture()
def wire(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """Point the pipeline at scripted NER and vault behaviour.

    `ner=None` means the service was unreachable; `ner=[]` means it ran and found nothing.
    Collapsing those two is the bug `pii_clients.detect_entities` documents at length, so the
    fixture keeps them distinct too.
    """

    def _install(ner: list[Span] | None, vault_ok: bool) -> None:
        monkeypatch.setattr(pii_pipeline, "ner_configured", lambda: True)
        monkeypatch.setattr(pii_pipeline, "vault_configured", lambda: True)

        async def _detect(_org: str, _text: str) -> list[Span] | None:
            return ner

        async def _mint(_org: str, spans: list[Span]) -> list[pii_clients.MintedToken] | None:
            if not vault_ok:
                return None
            return [
                pii_clients.MintedToken(entity_type=s.entity_type, token=f"tok{i}")
                for i, s in enumerate(spans)
            ]

        monkeypatch.setattr(pii_pipeline, "detect_entities", _detect)
        monkeypatch.setattr(pii_pipeline, "mint_tokens", _mint)

    return _install


async def test_ner_up_vault_up_gives_reversible_placeholders(wire) -> None:  # type: ignore[no-untyped-def]
    """The intended path: typed AND reversible, so a confirmed decision reading
    '<PERSON:tok0> owns the renewal' can be resolved back for the humans it was extracted
    for - which is the whole requirement Compliance section 5.2 states."""
    wire(ner=[Span("PERSON", 0, 10, "David Chen")], vault_ok=True)
    result = await redact_final(ORG, "David Chen owes jane@acme.com")

    assert result.text == "‹PERSON:tok0› owes ‹EMAIL:tok1›"
    assert result.redacted is True
    assert result.partially_redacted is False


async def test_ner_up_vault_down_falls_back_to_irreversible(wire) -> None:  # type: ignore[no-untyped-def]
    """Exactly the behaviour that shipped before the vault existed. This is why the vault
    REFUSING to mint under a development-only key provider (ADR-0024) is safe: the refusal
    costs reversibility, never protection."""
    wire(ner=[Span("PERSON", 0, 10, "David Chen")], vault_ok=False)
    result = await redact_final(ORG, "David Chen owes jane@acme.com")

    assert result.text == "‹PERSON› owes ‹EMAIL›"
    assert result.redacted is True
    assert result.partially_redacted is False


async def test_ner_down_still_removes_every_structured_identifier(wire) -> None:  # type: ignore[no-untyped-def]
    """The dependency-free layer is why `regex_only` is an acceptable answer at all: the email
    is gone even though no model ran. Only the name survives, and the row says so."""
    wire(ner=None, vault_ok=True)
    result = await redact_final(ORG, "David Chen owes jane@acme.com")

    assert "jane@acme.com" not in result.text
    assert "David Chen" in result.text
    assert result.partially_redacted is True


async def test_a_clean_row_is_still_flagged_when_no_model_ran(wire) -> None:  # type: ignore[no-untyped-def]
    """`redacted=false, partially_redacted=true` - the combination a `redacted`-only schema
    cannot express, and the reason the column was added. Nothing is known about names here."""
    wire(ner=None, vault_ok=True)
    result = await redact_final(ORG, "we should ship on friday")

    assert result.redacted is False
    assert result.partially_redacted is True


async def test_a_model_that_ran_and_found_nothing_is_not_flagged(wire) -> None:  # type: ignore[no-untyped-def]
    """The other half of the distinction: `[]` is a COMPLETE redaction, `None` is an absent
    one. If `detect_entities` returned `[]` on failure these two tests would agree, and the
    flag would silently mean nothing."""
    wire(ner=[], vault_ok=True)
    result = await redact_final(ORG, "we should ship on friday")

    assert result.partially_redacted is False


async def test_the_drop_policy_discards_the_final(wire) -> None:  # type: ignore[no-untyped-def]
    wire(ner=None, vault_ok=True)
    result = await redact_final_with_policy(ORG, "David Chen owes money", POLICY_DROP)
    assert result.text is None


async def test_the_drop_policy_does_not_fire_when_only_the_vault_is_down(wire) -> None:  # type: ignore[no-untyped-def]
    """Losing a meeting's record buys nothing here: the text is already fully redacted, just
    irreversibly. `drop` is about text a model never inspected, not about reversibility."""
    wire(ner=[Span("PERSON", 0, 10, "David Chen")], vault_ok=False)
    result = await redact_final_with_policy(ORG, "David Chen owes money", POLICY_DROP)
    assert result.text == "‹PERSON› owes money"


async def test_regex_only_is_the_default_and_keeps_the_record(wire) -> None:  # type: ignore[no-untyped-def]
    wire(ner=None, vault_ok=True)
    result = await redact_final_with_policy(ORG, "David Chen owes money", POLICY_REGEX_ONLY)
    assert result.text is not None
    assert result.partially_redacted is True


async def test_overlapping_detections_do_not_splice_into_each_other(wire) -> None:  # type: ignore[no-untyped-def]
    """The model tags the local part of an address as a PERSON while the deterministic layer
    tags the whole address as an EMAIL. Substituting in two passes would embed one placeholder
    inside the other and produce something unreadable and unresolvable.

    The deterministic span wins, which also keeps the entity TYPE right - an address minted as
    a PERSON would land in the wrong token space and never cohere with the same address seen
    by `ingestion-service`."""
    text = "jane.doe@acme.com is on it"
    wire(ner=[Span("PERSON", 0, 8, "jane.doe")], vault_ok=True)
    result = await redact_final(ORG, text)

    assert result.text == "‹EMAIL:tok0› is on it"


async def test_repeated_values_are_replaced_at_every_occurrence(wire) -> None:  # type: ignore[no-untyped-def]
    """Offsets, not substring re-finding. A pipeline that re-found the string would replace the
    first occurrence twice and leave the second in place."""
    text = "David Chen briefed David Chen"
    wire(
        ner=[Span("PERSON", 0, 10, "David Chen"), Span("PERSON", 19, 29, "David Chen")],
        vault_ok=True,
    )
    result = await redact_final(ORG, text)

    assert "David Chen" not in result.text
    assert result.text.count("‹PERSON:") == 2


async def test_no_path_persists_the_raw_value(wire) -> None:  # type: ignore[no-untyped-def]
    """ADR-0020 rejected 'persisted raw' outright, so it must not be reachable from ANY
    combination of dependency states."""
    text = "David Chen owes jane@acme.com 4111111111111111"
    for ner, vault_ok in [
        ([Span("PERSON", 0, 10, "David Chen")], True),
        ([Span("PERSON", 0, 10, "David Chen")], False),
        (None, True),
        (None, False),
    ]:
        wire(ner=ner, vault_ok=vault_ok)
        for policy in (POLICY_DROP, POLICY_REGEX_ONLY):
            result = await redact_final_with_policy(ORG, text, policy)
            if result.text is None:
                continue
            assert "jane@acme.com" not in result.text
            assert "4111111111111111" not in result.text


async def test_a_length_mismatch_from_the_vault_falls_back_rather_than_mispairing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A short token list would make the zip pair a token with the WRONG span, writing one
    person's placeholder over another's name. `mint_tokens` refuses; here we prove the
    pipeline's `strict=True` zip would catch it even if that check were removed."""
    monkeypatch.setattr(pii_pipeline, "ner_configured", lambda: True)
    monkeypatch.setattr(pii_pipeline, "vault_configured", lambda: True)

    async def _detect(_org: str, _text: str) -> list[Span] | None:
        return [Span("PERSON", 0, 10, "David Chen")]

    async def _mint(_org: str, _spans: list[Span]) -> list[pii_clients.MintedToken] | None:
        return [pii_clients.MintedToken("PERSON", "only-one")]

    monkeypatch.setattr(pii_pipeline, "detect_entities", _detect)
    monkeypatch.setattr(pii_pipeline, "mint_tokens", _mint)

    with pytest.raises(ValueError):
        await redact_final(ORG, "David Chen owes jane@acme.com")
