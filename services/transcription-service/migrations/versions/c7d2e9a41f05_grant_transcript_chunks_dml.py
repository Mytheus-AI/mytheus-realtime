"""fix: grant DML on transcript_chunks to the application roles (Plan 3-4 gap 28)

`be66698dbefb` created `transcript_chunks` with no GRANT at all: it relied entirely on the
schema-wide `ALTER DEFAULT PRIVILEGES ... GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO
mytheus_app, mytheus_admin` from `mytheus-backend`'s `a1c4e7f20b18`. `mytheus-backend`'s
`9b2c4d1e6f30` (0005) retires that default privilege (closing a proven cross-tenant
read/write/delete through partitions), and its docstring discloses the consequence for this
repo: on a rebuild from an EMPTY cluster where `be66698dbefb` runs after 0005, the parent
table is born with an empty ACL and `mytheus_app` gets "permission denied for table
transcript_chunks". That path is disaster recovery and a new developer's first `make dev`.

This revision applies, verbatim, the one-line fix 0005's docstring names, as a NEW follow-up
revision rather than an edit to `be66698dbefb` (already applied on existing databases; an
edited, already-applied migration never re-runs there, and rewriting history would desync
every database from the source).

Grant on the PARENT only. Partitions stay locked down (`_partition_privileges.
lock_down_partition`; `tests/test_partition_privileges.py`): reached through the parent, the
`tenant_isolation` RLS policy governs every row; a partition named directly must remain
unreachable. Idempotent on a database where the grant already exists (re-granting a held
privilege is a no-op), so it is safe on every existing database too.

Revision ID: c7d2e9a41f05
Revises: be66698dbefb
Create Date: 2026-11-06
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "c7d2e9a41f05"
down_revision: str | None = "be66698dbefb"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON transcript_chunks "
        "TO mytheus_app, mytheus_admin;"
    )


def downgrade() -> None:
    op.execute(
        "REVOKE SELECT, INSERT, UPDATE, DELETE ON transcript_chunks "
        "FROM mytheus_app, mytheus_admin;"
    )
