"""`devseed`: the dev-only seed path for sibling repos.

These cover the parts that need no infrastructure - utterance construction, determinism, and
the safety guard. The persistence behaviour they feed into is already the subject of
`test_persistence_finals_only.py`, which is the point of the seeder routing through
`process_utterance` instead of INSERTing: there is no second write path here to test twice.
"""

from __future__ import annotations

import uuid

from transcription_service.devseed import _ENV_GUARD, build_utterances, main


def test_chunk_ids_are_deterministic_so_reseeding_is_a_no_op() -> None:
    """The whole idempotency story rests on this: a second run must derive the SAME
    `chunk_id`s, so `chunk:{chunk_id}` hits the outbox's `(topic, idempotency_key)` guard and
    the re-run writes nothing rather than duplicating every row."""
    meeting_id = str(uuid.uuid4())
    first = build_utterances(meeting_id, finals=5, speakers=2, with_interims=False)
    second = build_utterances(meeting_id, finals=5, speakers=2, with_interims=False)

    assert [u.chunk_id for u in first] == [u.chunk_id for u in second]
    assert all(uuid.UUID(u.chunk_id) for u in first), "chunk_id must be a valid UUID string"


def test_different_meetings_do_not_collide() -> None:
    a = build_utterances(str(uuid.uuid4()), finals=5, speakers=2, with_interims=False)
    b = build_utterances(str(uuid.uuid4()), finals=5, speakers=2, with_interims=False)
    assert not ({u.chunk_id for u in a} & {u.chunk_id for u in b})


def test_finals_only_by_default_and_interims_share_the_final_chunk_id() -> None:
    meeting_id = str(uuid.uuid4())
    plain = build_utterances(meeting_id, finals=3, speakers=1, with_interims=False)
    assert [u.is_final for u in plain] == [True, True, True]

    paired = build_utterances(meeting_id, finals=3, speakers=1, with_interims=True)
    assert [u.is_final for u in paired] == [False, True, False, True, False, True]
    # Each interim revises the final that follows it, exactly as a real provider emits.
    assert paired[0].chunk_id == paired[1].chunk_id
    assert paired[2].chunk_id == paired[3].chunk_id


def test_speakers_round_robin_and_timestamps_advance() -> None:
    """Downstream diarization and speaker-mapping need more than one speaker, and
    `(meeting_id, start_ms)` is the table's index and natural replay order - every row landing
    on the same millisecond would make seeded data useless for exercising either."""
    finals = build_utterances(str(uuid.uuid4()), finals=4, speakers=2, with_interims=False)
    assert [u.speaker_id for u in finals] == ["0", "1", "0", "1"]
    starts = [u.start_ms for u in finals]
    assert starts == sorted(starts) and len(set(starts)) == len(starts)
    assert all(u.end_ms > u.start_ms for u in finals)


def test_seeded_text_includes_pii_so_redaction_is_exercised() -> None:
    """A tenant with `pii_redaction_enabled` should see the seeder produce genuinely redacted
    rows, so a consumer downstream meets a real `redacted=true` row and a real placeholder
    rather than only ever seeing clean text."""
    joined = " ".join(
        u.text
        for u in build_utterances(str(uuid.uuid4()), finals=10, speakers=1, with_interims=False)
    )
    assert "@" in joined, "expected an email-shaped line among the seeded utterances"
    assert any(ch.isdigit() for ch in joined), "expected a phone-shaped line"


def test_refuses_to_run_without_the_explicit_env_guard(monkeypatch, capsys) -> None:
    """The guard is the only thing standing between this and fabricated transcripts in a real
    tenant's history, so its absence must stop the run before any database work happens."""
    monkeypatch.delenv(_ENV_GUARD, raising=False)
    rc = main(["--org-id", str(uuid.uuid4()), "--finals", "1"])
    assert rc == 2
    assert _ENV_GUARD in capsys.readouterr().err
