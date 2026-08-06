"""Shared partition-privilege lock-down for `transcript_chunks`.

Fix round 1, item 3. Postgres checks privileges against the relation NAMED in a query: reached
through the partitioned PARENT (`SELECT ... FROM transcript_chunks`), the parent's RLS policy
governs; named directly (`SELECT ... FROM transcript_chunks_2026_08`), the PARTITION's own
privileges and row-security state govern instead, and RLS enabled on the parent does NOT
propagate to partitions the way `CREATE POLICY`/`CREATE INDEX` do.

`mytheus-backend`'s migration `92ae826` (`ALTER DEFAULT PRIVILEGES`) grants the full DML set
on every newly created table in the `public` schema to `mytheus_app`/`mytheus_admin` — and a
partition created via `CREATE TABLE ... PARTITION OF` is a newly created table, so every
`transcript_chunks_*` partition silently inherited that grant with no corresponding RLS. An
application role could `SELECT`/`UPDATE`/`DELETE` any tenant's transcript rows by naming the
partition directly, bypassing `tenant_isolation` entirely — verified live before this fix.

`mytheus-backend`'s own `audit_logs` migration (`c3e8b1d47f92`) hit the identical defect and
fixed it the same way: `REVOKE ALL` on each partition, immediately after creating it, for
every application role. This module exists so that fix is ONE function call, not a comment to
remember: `services/transcription-service/tests/test_partition_privileges.py` asserts against
the live database that this has been applied to `transcript_chunks`'s partitions — CURRENT
ones and any future one (the next premake, or the later-band `workflow-service` nightly job) —
so a future partition that forgets to call this fails CI rather than silently reopening the
hole, instead of relying on whoever adds the next partition to remember a convention.
"""

from __future__ import annotations

# Mirrors mytheus-backend's `_TENANT_TABLES`/role set exactly (migration `a1c4e7f20b18`):
# `mytheus_app` (request path), `mytheus_relay` (outbox drain only), `mytheus_admin` (operator
# seam). None of the three should ever reach a `transcript_chunks` partition directly.
APPLICATION_ROLES = ("mytheus_app", "mytheus_relay", "mytheus_admin")


def lock_down_partition(op, partition_name: str) -> None:  # type: ignore[no-untyped-def]
    """Revoke the `ALTER DEFAULT PRIVILEGES` grant a newly created partition inherits
    automatically. Call this immediately after every `CREATE TABLE ... PARTITION OF
    transcript_chunks ...`, current or future."""
    roles = ", ".join(APPLICATION_ROLES)
    op.execute(f"REVOKE ALL ON {partition_name} FROM {roles};")
