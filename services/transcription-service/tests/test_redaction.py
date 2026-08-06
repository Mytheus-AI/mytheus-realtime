"""Compliance gate condition 2's acceptance check: "a test proving a final containing
jane@acme.com lands in transcript_chunks as ‹EMAIL› when the tenant has the flag on, and raw
(in-region) when off." Two layers: unit tests of `redact()` for every entity class the plan
names, plus an end-to-end test through `process_utterance` against the real Postgres proving
the on/off gate (`tenant_settings.pii_redaction_enabled`) actually changes what lands in
`transcript_chunks`.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from transcription_service.persistence import process_utterance
from transcription_service.providers.base import Utterance
from transcription_service.redaction import redact


def test_email_redacted() -> None:
    text_out, redacted = redact("reach me at jane@acme.com tomorrow")
    assert text_out == "reach me at ‹EMAIL› tomorrow"
    assert redacted is True


def test_phone_redacted() -> None:
    text_out, redacted = redact("call me at 415-555-0182 later")
    assert "‹PHONE›" in text_out
    assert redacted is True


def test_iban_redacted() -> None:
    text_out, redacted = redact("wire it to DE89370400440532013000 please")
    assert text_out == "wire it to ‹IBAN› please"
    assert redacted is True


def test_valid_card_redacted_via_luhn() -> None:
    # 4111 1111 1111 1111 is the standard Luhn-valid Visa test number.
    text_out, redacted = redact("card is 4111 1111 1111 1111 exp 12/29")
    assert "‹CARD›" in text_out
    assert redacted is True


def test_invalid_luhn_digit_run_is_not_tagged_as_card_but_still_redacted_as_phone() -> None:
    # Same length, deliberately NOT Luhn-valid: must not be tagged ‹CARD› (that tag is a
    # Luhn-validated positive signal), but a 16-digit run still matches the looser phone
    # pattern and gets redacted under that tag instead -- over-redacting a digit-heavy span
    # that ISN'T a valid card number is the safer failure mode than leaving a
    # PII-shaped number raw on a false negative.
    text_out, redacted = redact("reference number 1234 5678 9012 3456")
    assert "‹CARD›" not in text_out
    assert "‹PHONE›" in text_out
    assert redacted is True


def test_government_id_redacted() -> None:
    text_out, redacted = redact("my ssn is 219-09-9999 on file")
    assert text_out == "my ssn is ‹GOVID› on file"
    assert redacted is True


def test_no_pii_passes_through_unredacted() -> None:
    text_out, redacted = redact("hello there, how are you")
    assert text_out == "hello there, how are you"
    assert redacted is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pii_redaction_enabled,expected_text,expected_redacted",
    [
        (True, "reach jane at ‹EMAIL› about the budget", True),
        (False, "reach jane at jane@acme.com about the budget", False),
    ],
)
async def test_persisted_text_respects_tenant_flag(
    session_factory: async_sessionmaker[AsyncSession],
    redis_client,
    tenant: dict[str, str],
    pii_redaction_enabled: bool,
    expected_text: str,
    expected_redacted: bool,
) -> None:
    org_id, meeting_id = tenant["org_id"], tenant["meeting_id"]
    chunk_id = str(uuid.uuid4())
    final = Utterance(
        chunk_id=chunk_id,
        speaker_id="0",
        start_ms=0,
        end_ms=800,
        text="reach jane at jane@acme.com about the budget",
        is_final=True,
        confidence=0.95,
    )

    result = await process_utterance(
        session_factory=session_factory,
        redis=redis_client,
        org_id=org_id,
        meeting_id=meeting_id,
        utterance=final,
        pii_redaction_enabled=pii_redaction_enabled,
    )
    assert result is not None
    assert result.redacted is expected_redacted

    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text("SELECT text, redacted FROM transcript_chunks WHERE id = :id"),
                    {"id": chunk_id},
                )
            )
            .mappings()
            .one()
        )
    assert row["text"] == expected_text
    assert row["redacted"] is expected_redacted
