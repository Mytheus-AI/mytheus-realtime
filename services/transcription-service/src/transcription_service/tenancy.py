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


async def meeting_belongs_to_org(session: AsyncSession, org_id: str, meeting_id: str) -> bool:
    """Tenant check for `/internal/audio`: the `meeting_id` the sender named must be a meeting
    of the `org_id` it named. `meetings` is owned by `mytheus-backend` and RLS'd like every
    tenant table; the explicit `org_id` predicate keeps the check correct even on a connection
    that bypasses RLS (the local superuser, see `db.py`)."""
    row = (
        await session.execute(
            text("SELECT 1 FROM meetings WHERE id = :meeting_id AND org_id = :org_id"),
            {"meeting_id": meeting_id, "org_id": org_id},
        )
    ).first()
    return row is not None
