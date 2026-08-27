"""DEV-ONLY seed path for `transcript_chunks` and the `transcript-chunks` topic.

Why this exists. `transcription-service` is the single writer of `transcript_chunks`, and the
only way into it is a live Deepgram stream opened from a `meeting-room-events` `joined`. That
stream cannot open at all today: `COMPLIANCE_GATE_GREEN = False` blocks `DeepgramProvider`
before any network I/O, and there is no Deepgram account behind it regardless. The service
exposes no REST, gRPC or event write path, so the sibling repos that consume this data -
`embedding-service` (Qdrant), `search-service` (Postgres FTS) and `agent-runtime-service`, all
in `mytheus-intelligence` - had no way to get a single realistic row or message to work
against. This closes that, without opening a write path in the running service.

What it deliberately is NOT: a direct INSERT. Every utterance goes through
`persistence.process_utterance`, the same single call site the real provider streams drive, so
seeded data is produced by the real rules rather than resembling them:

  - interims are dropped and never persisted (pass `--with-interims` to prove it);
  - finals are redacted when the tenant has `pii_redaction_enabled`;
  - each final writes ONE `transcript_chunks` row and ONE `outbox` row in one transaction;
  - the outbox row is what `mytheus-backend`'s relay publishes to `transcript-chunks` as Avro,
    so consumers get a real message off a real topic, not a hand-rolled one.

A direct INSERT would have been shorter and would have produced rows that never went through
redaction, never got an outbox row, and so never reached the topic the consuming services
actually subscribe to - which is most of what those services need to be exercised at all.

Idempotent by construction. `chunk_id` is `uuid5(NAMESPACE_URL, "devseed:{meeting_id}:{i}")`,
deterministic in the same way and for the same reason `DeepgramProvider` derives its own from
the vendor's ids: re-running the seeder produces the same `chunk:{chunk_id}` idempotency keys,
so the outbox's `(topic, idempotency_key)` guard makes a second run a no-op for both tables
rather than a pile of duplicates. Seeding twice is safe and is the expected way to top up.

SAFETY. This refuses to run unless `TRANSCRIPTION_DEV_SEED=1` is set explicitly. The risk is
not corruption but plausible-looking fabricated transcripts landing in a real tenant's history,
where nothing downstream could distinguish them from a real meeting. An env var is a weaker
control than `compliance.py`'s source constant, deliberately: the gate there exists to stop a
vendor stream opening in ANY environment, whereas this must actually be runnable by a developer
against their own stack. The `--org-id` must already exist in `organizations`; the seeder never
creates one, because `organizations` belongs to `identity-tenant-service` and one writer per
store holds for dev tooling too.

Usage:

    export TRANSCRIPTION_DEV_SEED=1
    python -m transcription_service.devseed --org-id <uuid> --meeting-id <uuid> --finals 20

Get an `--org-id` from `mytheus-backend`'s own seed (`make seed`), which creates the
organizations, users and meetings this hangs off.

Picking a `--meeting-id`. `transcript_chunks` is range-partitioned, and declarative
partitioning restricts foreign keys FROM the partitioned table, so it has none: any UUID is
accepted and omitting the flag mints a fresh one, which is enough to exercise a consumer.
Pointing at a REAL seeded meeting is better whenever the consumer joins back to `meetings`
(search-service's FTS, anything rendering a transcript against its meeting). Note that
mytheus-backend's seeder does NOT mint deterministic meeting ids the way it does org and user
ids: meetings are created through the real `create_meeting` path and located by title, so
there is no id to hardcode here. Look one up:

    select id, title from meetings where org_id = '<org-id>' and deleted_at is null;
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid

from sqlalchemy import text

from .config import load_settings_from_env
from .db import make_engine, make_session_factory
from .persistence import process_utterance
from .providers.base import Utterance
from .tenancy import get_pii_redaction_enabled

_ENV_GUARD = "TRANSCRIPTION_DEV_SEED"

# Deliberately mundane, meeting-shaped lines. Two of them carry PII precisely so a tenant with
# `pii_redaction_enabled` exercises `redaction.redact` on the seeded path rather than only on
# the synthetic strings in the test suite - a consumer downstream then sees a real `redacted`
# flag and a real placeholder, which is part of what they need to be able to handle.
_LINES = [
    "Okay, let's start with the migration status.",
    "The staging cutover finished last night with no rollback.",
    "I still owe you the capacity numbers for the EU cell.",
    "Ping me at dana.whitfield@example.com and I'll forward the thread.",
    "We agreed to defer the reranker upgrade until after the audit.",
    "The p95 is sitting just under four hundred milliseconds now.",
    "Can you confirm the vendor call is on my calendar for Thursday?",
    "My direct line is 415-555-0148 if it becomes urgent.",
    "Let's write that up as a decision rather than leaving it in chat.",
    "Nothing else from me - I'll take the action on the runbook.",
]


def _chunk_id(meeting_id: str, index: int) -> str:
    """Deterministic per (meeting, index): see the module docstring on idempotency."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"devseed:{meeting_id}:{index}"))


def build_utterances(
    meeting_id: str, *, finals: int, speakers: int, with_interims: bool
) -> list[Utterance]:
    """One final per line, cycling through `_LINES` and round-robining speakers so diarization
    downstream has more than one speaker to map. Timestamps advance monotonically so
    `(meeting_id, start_ms)` - the table's index and the natural replay order - is meaningful
    rather than every row landing on the same millisecond."""
    utterances: list[Utterance] = []
    for i in range(finals):
        start_ms = i * 5_000
        end_ms = start_ms + 4_200
        chunk_id = _chunk_id(meeting_id, i)
        speaker_id = str(i % max(speakers, 1))
        body = _LINES[i % len(_LINES)]
        if with_interims:
            # Same chunk_id as the final it revises, exactly as a real provider emits: this is
            # what makes "interims never persist" observable rather than asserted, since a
            # duplicate-looking chunk_id that DID persist would show up immediately.
            head = body[: max(len(body) // 2, 1)]
            utterances.append(
                Utterance(
                    chunk_id=chunk_id,
                    speaker_id=speaker_id,
                    start_ms=start_ms,
                    end_ms=start_ms + 1_500,
                    text=head,
                    is_final=False,
                    confidence=0.55,
                )
            )
        utterances.append(
            Utterance(
                chunk_id=chunk_id,
                speaker_id=speaker_id,
                start_ms=start_ms,
                end_ms=end_ms,
                text=body,
                is_final=True,
                confidence=0.94,
            )
        )
    return utterances


async def seed(
    *, org_id: str, meeting_id: str, finals: int, speakers: int, with_interims: bool
) -> tuple[int, int]:
    """Returns `(written, skipped)`. `skipped` counts finals the outbox recognised as already
    seeded - a re-run reports every final skipped, which is the intended, safe outcome."""
    settings = load_settings_from_env()
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)

    try:
        async with engine.connect() as conn:
            exists = (
                await conn.execute(
                    text("SELECT 1 FROM organizations WHERE id = :id"), {"id": org_id}
                )
            ).scalar_one_or_none()
        if exists is None:
            raise SystemExit(
                f"org {org_id} does not exist in `organizations`. This seeder never creates "
                "one: `organizations` is owned by identity-tenant-service, and one writer per "
                "store applies to dev tooling too. Run `make seed` in mytheus-backend and use "
                "an org id from there."
            )

        # Read the tenant's real redaction setting rather than assuming one, so seeded rows
        # carry the same `redacted` flag a live meeting for this tenant would produce.
        async with session_factory() as session:
            async with session.begin():
                await session.execute(
                    text("SELECT set_config('app.current_org', :org_id, true)"),
                    {"org_id": org_id},
                )
                pii_redaction_enabled = await get_pii_redaction_enabled(session, org_id)

        written = 0
        skipped = 0
        for utterance in build_utterances(
            meeting_id, finals=finals, speakers=speakers, with_interims=with_interims
        ):
            # redis=None: captions are a live-plane concern with no subscriber during a seed,
            # and process_utterance treats None as "do not publish". The persistence and outbox
            # behaviour under test here is identical either way.
            result = await process_utterance(
                session_factory=session_factory,
                redis=None,
                org_id=org_id,
                meeting_id=meeting_id,
                utterance=utterance,
                pii_redaction_enabled=pii_redaction_enabled,
            )
            if utterance.is_final:
                if result is None:
                    skipped += 1
                else:
                    written += 1
        return written, skipped
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="transcription_service.devseed",
        description="DEV-ONLY: seed transcript_chunks + the transcript-chunks topic.",
    )
    parser.add_argument("--org-id", required=True, help="must already exist in `organizations`")
    parser.add_argument("--meeting-id", default=None, help="defaults to a fresh uuid4")
    parser.add_argument("--finals", type=int, default=10, help="number of final utterances")
    parser.add_argument("--speakers", type=int, default=2, help="distinct diarized speakers")
    parser.add_argument(
        "--with-interims",
        action="store_true",
        help="also feed interims, which must NOT persist (useful for proving the split)",
    )
    args = parser.parse_args(argv)

    if os.environ.get(_ENV_GUARD) != "1":
        print(
            f"refusing to run: set {_ENV_GUARD}=1 to confirm this is a development database.\n"
            "This writes plausible-looking transcripts that nothing downstream can tell apart "
            "from a real meeting's.",
            file=sys.stderr,
        )
        return 2

    meeting_id = args.meeting_id or str(uuid.uuid4())
    written, skipped = asyncio.run(
        seed(
            org_id=args.org_id,
            meeting_id=meeting_id,
            finals=args.finals,
            speakers=args.speakers,
            with_interims=args.with_interims,
        )
    )
    print(
        f"meeting_id={meeting_id}\n"
        f"  transcript_chunks rows written: {written}\n"
        f"  finals skipped as already seeded: {skipped}\n"
        f"  outbox rows enqueued: {written} (the relay publishes these to `transcript-chunks`; "
        "run `python -m services.relay.main` in mytheus-backend if nothing is draining it)"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
