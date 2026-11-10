"""Compliance gate acceptance check (plan 3.5): "an audit-log entry records the
redaction-toggle and any cross-border decision." (Fix round 1, item 2 — this was entirely
missing from the first pass and, worse, not disclosed as missing; both are fixed here.)

Writes into `audit_logs` (owned/migrated by `mytheus-backend`, `c3e8b1d47f92`), the same kind
of deliberate cross-service write as `outbox.py` and for the same reason: it is cross-cutting
compliance infrastructure every service writes to, not a domain table this service reaches
into. `mytheus_app` (and this service, via the same `DATABASE_URL`/superuser connection used
throughout this repo — see `db.py`'s docstring) holds `SELECT, INSERT` on it, append-only by
design (UPDATE/DELETE are revoked from every application role at the database level).

One entry is written per session-open (`SessionTracker.on_participant_joined`, the point
where `region` and `pii_redaction_enabled` are already read for that meeting), not per
utterance: it records the DECISION made for that stream — which redaction/residency posture
this session ran under — not every individual redaction event (`transcript_chunks.redacted`
already records that per-row; duplicating it into the audit trail at utterance granularity
would make `audit_logs` grow at the same rate as `transcript_chunks` for no additional
compliance value). `actor_type='system'`: this is an automated decision made by the service
reading tenant configuration, not a user or agent action.
"""

from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

ACTION_SESSION_OPENED = "transcription.session_opened"


async def record_session_decision(
    session: AsyncSession,
    *,
    org_id: str,
    meeting_id: str,
    region: str,
    pii_redaction_enabled: bool,
) -> None:
    """Must be called inside the same transaction `app.current_org` was set on for `org_id`
    (RLS on `audit_logs` is `tenant_isolation`, identical predicate shape to every other
    tenant table in this database)."""
    await session.execute(
        text(
            "INSERT INTO audit_logs (org_id, actor_type, action, target_type, target_id, metadata) "
            "VALUES (:org_id, 'system', :action, 'meeting', :meeting_id, CAST(:metadata AS jsonb))"
        ),
        {
            "org_id": org_id,
            "action": ACTION_SESSION_OPENED,
            "meeting_id": meeting_id,
            "metadata": json.dumps(
                {"pii_redaction_enabled": pii_redaction_enabled, "region": region}
            ),
        },
    )


ACTION_CAPTURE_CONSENT = "transcription.capture_consent"


async def record_capture_consent(
    session: AsyncSession,
    *,
    org_id: str,
    meeting_id: str,
    user_id: str,
    source: str,
    channel: str,
    consent_at: str,
) -> None:
    """The capture companion (`capture_in.py`): the person capturing a call held elsewhere
    confirmed, in the extension, that they told the other participants it is being transcribed.
    One append-only row per capture connection (mic and tab each), `actor_type='user'` because
    the confirmation is that user's statement, recorded the moment their audio is accepted.
    Same transaction rule as `record_session_decision`."""
    await session.execute(
        text(
            "INSERT INTO audit_logs (org_id, actor_id, actor_type, action, target_type, "
            "target_id, metadata) VALUES (:org_id, :actor_id, 'user', :action, 'meeting', "
            ":meeting_id, CAST(:metadata AS jsonb))"
        ),
        {
            "org_id": org_id,
            "actor_id": user_id,
            "action": ACTION_CAPTURE_CONSENT,
            "meeting_id": meeting_id,
            "metadata": json.dumps(
                {
                    "source": source,
                    "channel": channel,
                    "participants_told_at": consent_at,
                }
            ),
        },
    )
