"""Reads of `tenant_settings.pii_redaction_enabled` and `data_residency.region` — both tables
owned and migrated by `identity-tenant-service` in `mytheus-backend`, both in the same shared
Postgres this service's `DATABASE_URL` points at. See `db.py`'s docstring for why a direct SQL
read (rather than a REST call to a service that, in this band, doesn't yet expose either
value over HTTP) is the sanctioned pattern for a cross-service READ in this program.

Both tables are RLS-protected (`ENABLE`+`FORCE`+`tenant_isolation`, keyed on `org_id` /
`app.current_org`) exactly like every other tenant table in the shared database, so every read
here runs inside a transaction that has already called `set_current_org`.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Fail-safe defaults when a tenant somehow has no row yet (should not happen in steady state --
# `identity-tenant-service` provisions both rows at org creation -- but a missing row must fail
# toward the SAFER behavior: redact by default, and pin to `us` rather than guess a region).
_DEFAULT_PII_REDACTION_ENABLED = True
_DEFAULT_REGION = "us"
# ADR-0020 § The unavailable-NER decision, carried as `tenant_settings.ner_unavailable_policy`
# by ADR-0024. `regex_only` matches the column's own DEFAULT, so a tenant with no row and a
# tenant who never touched the setting behave identically. It is also the safer default in the
# sense that matters for a MISSING row: `drop` would silently discard a tenant's entire
# transcript whenever this read fell through, which is a data-loss failure mode dressed as a
# privacy one. Structured PII is still removed on this path regardless.
_DEFAULT_NER_UNAVAILABLE_POLICY = "regex_only"


async def get_pii_redaction_enabled(session: AsyncSession, org_id: str) -> bool:
    row = (
        await session.execute(
            text("SELECT pii_redaction_enabled FROM tenant_settings WHERE org_id = :org_id"),
            {"org_id": org_id},
        )
    ).first()
    if row is None:
        return _DEFAULT_PII_REDACTION_ENABLED
    return bool(row[0])


async def get_data_residency_region(session: AsyncSession, org_id: str) -> str:
    row = (
        await session.execute(
            text("SELECT region FROM data_residency WHERE org_id = :org_id"), {"org_id": org_id}
        )
    ).first()
    if row is None:
        return _DEFAULT_REGION
    return str(row[0])


async def get_ner_unavailable_policy(session: AsyncSession, org_id: str) -> str:
    """What to do with a final when `ner-service` is unreachable and redaction is on.

    `drop` loses the record; `regex_only` persists it with the deterministic layer applied and
    `transcript_chunks.partially_redacted` set. Persisting raw is not representable, having
    been rejected outright by ADR-0020.
    """
    row = (
        await session.execute(
            text("SELECT ner_unavailable_policy FROM tenant_settings WHERE org_id = :org_id"),
            {"org_id": org_id},
        )
    ).first()
    if row is None or not row[0]:
        return _DEFAULT_NER_UNAVAILABLE_POLICY
    return str(row[0])
