"""feat(transcription-service): transcript_chunks.partially_redacted, plus the missing GRANT

TWO CHANGES, ONE OF WHICH IS A LATENT BREAK THIS MIGRATION IS THE RIGHT PLACE TO FIX.

1. `partially_redacted boolean NOT NULL DEFAULT false`.

   ADR-0020 § The unavailable-NER decision offers Security/Compliance two acceptable answers
   for a tenant with `pii_redaction_enabled` when `ner-service` is unreachable: drop the final,
   or "persisted regex-only, flagged - structured PII is still removed by the dependency-free
   layer; unstructured entities are not, and the row records that it was only partially
   redacted." This column IS that record. Without it the second option is not actually
   available, because "flagged" has nowhere to live.

   It is a strictly weaker claim than `redacted`, and the two are independent rather than
   ordered: `redacted` says at least one span was replaced, `partially_redacted` says no model
   ever looked at this text. A row can be `redacted=true, partially_redacted=true` (the regex
   layer found an email while NER was down), or `redacted=false, partially_redacted=true` (NER
   was down and the regex layer found nothing, so nothing is known about names in this row).
   That second combination is the one the flag exists for, and it is exactly the one a
   `redacted`-only schema cannot express.

   DEFAULT false is correct for the backfill and is not a guess: every row written before this
   migration was written when NER did not exist as a caller at all, but also when the tenant's
   redaction posture was fully satisfied by the deterministic layer, which was the whole of the
   pass. Those rows are not "partially redacted relative to the pass that ran on them". Rows
   written from now on get the real answer from `pii_pipeline.py`.

2. The GRANT that `9b2c4d1e6f30` in `mytheus-backend` predicted this table would need.

   That migration retired the schema-wide `ALTER DEFAULT PRIVILEGES` on TABLES in `public`, and
   its docstring names this repo's `be66698dbefb` as the one cross-repo consequence: it creates
   `transcript_chunks` in the shared `public` schema with no grant of its own, relying entirely
   on the default privilege that is now gone. Nothing breaks where the table already exists,
   because grants made before that migration are untouched. What breaks is a rebuild from an
   EMPTY CLUSTER where `be66698dbefb` runs after it: the parent table comes out with an empty
   ACL and `mytheus_app` gets "permission denied for table transcript_chunks". That path is
   disaster recovery and a new developer's first `make dev`, so it is real.

   `mytheus-backend` deliberately did not apply it from there - reaching across a repo boundary
   to grant on another service's table is the mistake canon ownership rules exist to prevent,
   and it would be invisible to anyone reading this repo. It was recorded as an open gap for
   this repo to close. This is that repo, and this is a migration that already touches this
   table, so it closes here rather than accruing.

   The grant is on the PARENT only. Partitions are reached through the parent, where the grants
   and the `tenant_isolation` policy live; a direct grant on a partition is the defect that
   whole episode was about, and `lock_down_partition` in `be66698dbefb` exists to strip them.

Revision ID: c7a41e0d59b3
Revises: be66698dbefb
Create Date: 2026-08-29 11:48:02.114905

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "c7a41e0d59b3"
down_revision: str | None = "be66698dbefb"
branch_labels: str | None = None
depends_on: str | None = None

TABLE = "transcript_chunks"

# The two application roles `mytheus-backend`'s `migrations/_grants.py` grants on a tenant
# table. `mytheus_relay` is correctly absent: it holds a grant on `outbox` alone, paired with
# the one permissive policy that lets it drain across tenants.
APP_ROLE = "mytheus_app"
ADMIN_ROLE = "mytheus_admin"


def upgrade() -> None:
    op.add_column(
        TABLE,
        sa.Column(
            "partially_redacted",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )
    op.execute(
        f"""
        COMMENT ON COLUMN {TABLE}.partially_redacted IS
        'True when this row was persisted without ner-service having inspected it, so its '
        'unstructured entities (PERSON/ORG/LOC) were never detected and only the '
        'deterministic layer applied. Independent of `redacted`, which says only that at '
        'least one span was replaced. ADR-0020 option 2.';
        """
    )

    # See the module docstring, item 2. IF EXISTS on the roles is not available in GRANT, and
    # the roles are created by mytheus-backend's `a1c4e7f20b18` which is a prerequisite of any
    # database this service runs against - so a missing role here is a genuinely broken
    # environment and should fail loudly rather than be swallowed.
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {TABLE} TO {APP_ROLE}, {ADMIN_ROLE};")


def downgrade() -> None:
    op.execute(f"REVOKE SELECT, INSERT, UPDATE, DELETE ON {TABLE} FROM {APP_ROLE}, {ADMIN_ROLE};")
    op.drop_column(TABLE, "partially_redacted")
