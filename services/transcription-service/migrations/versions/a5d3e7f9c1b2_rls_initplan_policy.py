"""fix: transcript_chunks' tenant policy in InitPlan form

Journal plan Part 3 fix 1, the transcription-service share of it (mytheus-backend `b8e2c4f6a1d3`
and mytheus-intelligence `f2a6c8e1b3d5` rewrite their own tables' policies).

`tenant_isolation` on `transcript_chunks` is rewritten from
`org_id = NULLIF(current_setting('app.current_org', true), '')::uuid` to
`org_id = (SELECT NULLIF(current_setting('app.current_org', true), '')::uuid)`: the same
fail-closed NULLIF, evaluated once per statement as an InitPlan instead of once per row.
ALTER POLICY changes only USING and WITH CHECK; the command, roles, ENABLE/FORCE ROW LEVEL
SECURITY and the presence of WITH CHECK are untouched. The policy lives on the partitioned
parent only, so no partition is touched.

Revision ID: a5d3e7f9c1b2
Revises: e4a7c91b3d28
Create Date: 2026-11-15
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "a5d3e7f9c1b2"
down_revision: str | None = "e4a7c91b3d28"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_PER_ROW = "NULLIF(current_setting('app.current_org', true), '')::uuid"
_INITPLAN = f"(SELECT {_PER_ROW})"


def upgrade() -> None:
    op.execute(
        f"ALTER POLICY tenant_isolation ON transcript_chunks "
        f"USING (org_id = {_INITPLAN}) WITH CHECK (org_id = {_INITPLAN});"
    )


def downgrade() -> None:
    op.execute(
        f"ALTER POLICY tenant_isolation ON transcript_chunks "
        f"USING (org_id = {_PER_ROW}) WITH CHECK (org_id = {_PER_ROW});"
    )
