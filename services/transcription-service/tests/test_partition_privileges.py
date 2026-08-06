"""Fix round 1, item 3, the exceptionless guard: no application role may hold ANY direct
privilege on ANY `transcript_chunks` partition. Mirrors `mytheus-backend`'s own
`tests/test_roles_and_policies.py::test_no_application_role_can_reach_a_partition_directly`
(same defect class, `audit_logs` was the precedent there) but scoped to the partitions THIS
repo's migration creates, so it runs in THIS repo's CI rather than depending on
`mytheus-backend`'s.

Scoped via `pg_inherits`/`relispartition` against `transcript_chunks` specifically (not a
blanket scan of the whole `public` schema, which includes tables owned by
`mytheus-backend`'s migration history that this repo has no business asserting about, and
whose own CI already covers) -- this also means the assertion automatically covers any FUTURE
`transcript_chunks` partition (the next monthly premake, or the later-band
`workflow-service` nightly job), not just the four that exist today, without the test itself
needing to be updated when one is added.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from migrations._partition_privileges import APPLICATION_ROLES


@pytest.mark.asyncio
async def test_no_application_role_can_reach_a_transcript_chunks_partition_directly(
    engine: AsyncEngine,
) -> None:
    async with engine.connect() as conn:
        leaked = (
            (
                await conn.execute(
                    text(
                        "SELECT c.relname || ' -> ' || g.grantee || ':' || g.privilege_type "
                        "FROM pg_inherits i "
                        "JOIN pg_class c ON c.oid = i.inhrelid "
                        "JOIN information_schema.role_table_grants g "
                        "  ON g.table_name = c.relname AND g.table_schema = 'public' "
                        "WHERE i.inhparent = 'transcript_chunks'::regclass "
                        "  AND g.grantee = ANY(:roles) "
                        "ORDER BY 1"
                    ),
                    {"roles": list(APPLICATION_ROLES)},
                )
            )
            .scalars()
            .all()
        )
    assert leaked == [], f"application roles hold direct privileges on partitions: {leaked}"


@pytest.mark.asyncio
async def test_every_transcript_chunks_partition_has_row_security_disabled_and_no_grants(
    engine: AsyncEngine,
) -> None:
    """The flip side of the guard above, stated positively so a regression that grants nothing
    but also silently re-enables a permissive RLS state on a partition (which would make the
    REVOKE moot in a different way) is still caught: every partition's OWN `relrowsecurity`
    must also be disabled -- this table's isolation model relies entirely on the PARENT's
    FORCE RLS policy plus the partitions being unreachable directly, not on partitions each
    carrying a redundant copy of the policy (which nothing in this migration sets up)."""
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT c.relname, c.relrowsecurity "
                    "FROM pg_inherits i "
                    "JOIN pg_class c ON c.oid = i.inhrelid "
                    "WHERE i.inhparent = 'transcript_chunks'::regclass"
                )
            )
        ).all()
    assert rows, "expected at least one transcript_chunks partition to exist"
    for relname, relrowsecurity in rows:
        assert relrowsecurity is False, f"{relname} unexpectedly has row security enabled"
