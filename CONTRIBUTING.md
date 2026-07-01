# Contributing to mytheus-realtime

## Naming comes from canon — never invent
The `transcript_chunks` table, topic names, and the Redis channel
`nexus:{org_id}:pubsub:meeting:{meeting_id}` are frozen in `mytheus-docs/_canon/`
(`02-data-model.md`, `03-event-taxonomy.md`) and published in `24-glossary-and-naming.md`. Use them
verbatim (build Redis keys via `services/realtime-gateway/src/keys.ts`, never inline). A rename goes
**Glossary + ADR first** in `mytheus-docs`, then propagates here — never the reverse.

**Canon location (recorded decision):** canon authority lives in `mytheus-docs`. For MVP the
contract-binding repos consume it as a **sibling checkout** — clone all Mytheus repos under one
parent directory. Revisit git-submodule / vendoring post-W36.

## Isolation invariants
`org_id` rides every event header and scopes every Redis channel/key. Only Deepgram **finals**
persist to `transcript_chunks` (interims never do); the Deepgram key stays server-side and audio
never crosses the backend trust boundary. The two services stay decoupled — no shared process, no
shared in-memory state.

## Every PR includes an `org_id` authz review
Confirm new fan-out channels, presence sets, and `transcript_chunks` writes carry `org_id` and
re-assert it from the event header before any write. Shift-left security from day one.

## Standards
- **Conventional Commits**; **no `Co-Authored-By` footer** (per this repo's CLAUDE.md).
- **Trunk-based**: short-lived branches, frequent PRs, mandatory review.
- CI green before merge: `make ci` — gateway `tsc` + `npm run build`, transcription-service
  ruff + black + mypy + pytest. Install hooks once: `pre-commit install`.
