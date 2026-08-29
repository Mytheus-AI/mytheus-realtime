"""The two network calls the reversible redaction pass makes, and their failure semantics.

`ner-service` (`mytheus-intelligence`) DETECTS unstructured entities; the PII vault
(`identity-tenant-service` in `mytheus-backend`) MINTS the reversible tokens. Both are called
over HTTP with the workload credential, because this path has no user: it runs from a Deepgram
callback while proxying live meeting audio.

WHY THESE ARE HTTP CALLS AND NOT DIRECT SQL

`tenancy.py` reads `tenant_settings` straight out of the shared Postgres, and that is sanctioned
(`db.py`: cross-service READS against the shared database are the established pattern; only
cross-service WRITES are forbidden). Minting is a WRITE to `pii_tokens`, a table this service
does not own, so the same rule that permits the settings read forbids the mint. [ADR-0020](
../../../../../mytheus-docs/adr/0020-pii-token-vault-and-ner-placement.md) D2 says the same
thing from the other direction: the three redaction boundaries are callers of the vault's API,
never writers of its table, which is what keeps one shared token space per tenant from becoming
three writers of one store.

FAILURE IS A FIRST-CLASS OUTCOME HERE, NOT AN EXCEPTION TO HANDLE LATER

Both calls sit on the path that persists a transcript final, inside a p95 < 2 s caption SLO, on
a service that scales on anchor A5 (20k concurrent live streams). ADR-0020's consequences name
this availability coupling as the accepted cost of keeping the model off the audio proxy. So
both clients:

  - carry a SHORT timeout, because a hung dependency must become a fast, catchable failure
    rather than an unbounded stall (the same reasoning `libs/shared/cache.py` gives for Redis);
  - return `None` on any failure rather than raising, so the caller decides policy; and
  - never retry. A retry inside this path spends the latency budget twice for a dependency
    that is either up or down, and the tenant-side answer to "it is down" is already decided
    by `tenant_settings.ner_unavailable_policy`.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import httpx

from .redaction import Span

logger = logging.getLogger(__name__)

# Short, and short for a reason: this is inside the live caption budget. A caller that would
# rather wait is choosing to blow the SLO for every OTHER speaker on the same stream.
_TIMEOUT_SECONDS = 1.5

NER_URL_ENV = "NER_SERVICE_URL"
VAULT_URL_ENV = "PII_VAULT_URL"
INTERNAL_TOKEN_ENV = "INTERNAL_SERVICE_TOKEN"


@dataclass(frozen=True)
class MintedToken:
    entity_type: str
    token: str


def _headers() -> dict[str, str]:
    return {"X-Internal-Service-Token": os.environ.get(INTERNAL_TOKEN_ENV, "")}


def ner_configured() -> bool:
    return bool(os.environ.get(NER_URL_ENV))


def vault_configured() -> bool:
    return bool(os.environ.get(VAULT_URL_ENV))


async def detect_entities(org_id: str, text: str) -> list[Span] | None:
    """`POST /ner`. Returns spans, or `None` when the service is unconfigured or unreachable.

    The `None`/`[]` distinction is load-bearing and must not be collapsed: `[]` means the model
    ran and found no unstructured entities, which is a COMPLETE redaction. `None` means the
    model never ran, so the text is only partially covered and the tenant's
    `ner_unavailable_policy` decides what happens to the final. Returning `[]` on failure would
    silently mark a partially-redacted row as fully redacted.
    """
    base = os.environ.get(NER_URL_ENV)
    if not base:
        return None
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{base.rstrip('/')}/internal/ner",
                json={"org_id": org_id, "text": text},
                headers=_headers(),
            )
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:  # noqa: BLE001 - every failure mode has the same answer here
        # `text` is NEVER included in this log line. It is the unredacted utterance, and a
        # log written on the failure path is exactly where PII escapes unnoticed.
        logger.warning("ner-service unavailable, falling back", extra={"error": str(exc)})
        return None

    return [
        Span(
            entity_type=str(span["entity_type"]),
            start=int(span["start"]),
            end=int(span["end"]),
            text=str(span["text"]),
        )
        for span in payload.get("spans", [])
    ]


async def mint_tokens(org_id: str, spans: list[Span]) -> list[MintedToken] | None:
    """`POST /internal/pii-tokens/mint`. Returns tokens in REQUEST ORDER, or `None` on failure.

    Order matters because the caller zips these back onto the spans it sent; the vault's own
    contract guarantees it, and this function does not re-sort.

    A `None` here is not a partial redaction - the deterministic layer still covers everything
    it covers, and the caller falls back to irreversible placeholders, which is the behaviour
    that shipped before the vault existed. That fallback is why the vault refusing to mint
    under a development-only key provider (ADR-0024) is safe rather than a regression.
    """
    base = os.environ.get(VAULT_URL_ENV)
    if not base or not spans:
        return None
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{base.rstrip('/')}/internal/pii-tokens/mint",
                json={
                    "org_id": org_id,
                    "items": [{"entity_type": s.entity_type, "value": s.text} for s in spans],
                },
                headers=_headers(),
            )
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("pii vault unavailable, falling back", extra={"error": str(exc)})
        return None

    minted = [
        MintedToken(entity_type=str(item["entity_type"]), token=str(item["token"]))
        for item in payload.get("tokens", [])
    ]
    if len(minted) != len(spans):
        # A length mismatch would make the zip below silently pair a token with the WRONG span,
        # writing one person's placeholder over another's name. Refuse rather than guess.
        logger.error(
            "pii vault returned %d tokens for %d spans; falling back", len(minted), len(spans)
        )
        return None
    return minted
