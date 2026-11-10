/**
 * Agent output bridge: `agent-stream` and `agent-results` from Redpanda to the participant's
 * own sockets (PersonalListener) or to the whole meeting (decision/action candidates, the
 * MeetingAgent's agenda updates). Plan 7-9 section 3.4 steps 2 and 4; mytheus-docs ADR-0030 D5;
 * Phase 8 Part A section 3; ADR-0038 D9; ADR-0039 D2 (the new alert and nudge types).
 *
 * Delivery path:
 *   1. Every gateway pod runs this consumer in ONE consumer group, so each record is handled by
 *      exactly one pod.
 *   2. That pod turns it into a small JSON message (tagged with the target `userId` for
 *      PersonalListener output) and publishes it on the meeting's canonical channel
 *      `nexus:{org_id}:pubsub:meeting:{meeting_id}` (the same channel captions use; no new key).
 *   3. Every pod with local sockets on that channel receives it. PersonalListener output goes
 *      ONLY to sockets whose core-api-resolved internal user id equals `userId` (server.ts): a
 *      note is private to its participant; the meeting room would leak it to everyone.
 *      "agent-candidate" and "agent-agenda" go to every local socket on the channel (the channel
 *      is org-scoped).
 *
 * What is forwarded:
 *   - `agent-stream` deltas as "agent-delta". The agent only streams notes it already knows are
 *     in the surfaced band (ADR-0030 D4). Deltas are throwaway: emitted volatile, shed first.
 *   - `agent-results` `PersonalListenerAgent` notes with `payload_json.surfaced === true` as
 *     "agent-note". A silent-band note is persisted by core-api and never shown live.
 *   - `agent-results` `DecisionAgent`/`decision_candidate` and `ActionAgent`/`action_candidate`
 *     records as "agent-candidate", carrying only the candidates with `surfaced === true`, a
 *     UUID `id` and a string `content` (none left: skipped). These are meeting-scoped: the
 *     message has no `userId` and server.ts delivers it to every local socket on the channel.
 *     Candidates are proposals only; nothing is confirmed here (core-api owns confirm).
 *   - `agent-results` `QuestionAgent`/`suggestion` records as "agent-suggestion", carrying only
 *     the suggestions with `surfaced === true`, a UUID `id`, a string `question` and a priority
 *     of 1, 2 or 3 (none left: skipped). PRIVATE like notes (ADR-0034 D6): tagged with the
 *     owner's `userId`, delivered only to that user's sockets, never to the room.
 *   - `agent-results` `ContextGapAgent`/`context_fill` records (Phase P10, plan 3.5) as
 *     "agent-context", ONLY when `payload_json.fill` is an object with `surfaced === true`, a UUID
 *     `id`, a non-empty string `content`, a known `hop` and a `responder` of "rag" or
 *     "user:{uuid}". PRIVATE like suggestions: tagged with the requester's `userId` (the
 *     record's `user_id`), delivered only to that user's sockets. A null fill, a silent fill,
 *     the `collaborations` log and the responder-side security entries are never forwarded
 *     (they are core-api's to persist and audit).
 *   - `agent-results` `ProactiveAgent`/`alert` records (Phase P11, plan 3.6/3.7; P11 contract
 *     sections 1 and 4) as "agent-alert", ONLY when `payload_json.alert` is surfaced and well
 *     formed. PRIVATE like suggestions: tagged with the recipient's `userId` (the record's
 *     `user_id`; ProactiveAgent emits one record per recipient). Unlike every other kind these are
 *     NOT published straight to the channel: the consumer hands them to `onAlert`, which queues
 *     them in the recipient's priority queue `nexus:{org_id}:queue:agent:{user_id}` and nudges
 *     the meeting channel so a pod holding that user's sockets drains it (alertQueue.ts).
 *     `alert_type` is one of the P11 four ("conflict", "workload", "dependency",
 *     "missing_stakeholder") or, from mytheus-docs ADR-0039 C7, "already_decided" or
 *     "topic_watch", which carry their own fields as siblings in the alert object:
 *       - "already_decided": a UUID `decision_id` and a non-empty `decision_content` (cut to
 *         500), plus `decided_at` and `meeting_title` (strings or null), forwarded as
 *         `decisionId`, `decisionContent`, `decidedAt`, `meetingTitle`;
 *       - "topic_watch": a non-empty `topic` (cut to 80), plus `snippet` (cut to 200) and
 *         `meeting_title`, forwarded as `topic`, `snippet`, `meetingTitle`.
 *     Either one missing its required fields is dropped, and so is any alert whose own
 *     `meeting_id`, when present, is not the record's meeting (an alert is only ever delivered
 *     into its own meeting). Same queue, same delivery as the other alerts: a topic watcher is by
 *     definition not in the meeting, so live delivery happens only if they join it within 24 h;
 *     the durable copy is notification-service's.
 *   - `agent-results` `MeetingAgent`/`meeting_update` records (mytheus-docs ADR-0038 D7, D9),
 *     at most one per meeting per window, SPLIT into several messages by `routeAgentRecords`,
 *     and only when `payload_json` parses to an object with `surfaced === true` (otherwise the
 *     whole record is skipped). Each entry is validated on its own: a bad entry is dropped, the
 *     good ones still go out.
 *       - every entry of `updates` with a UUID `item_id`, a `status` of "in_progress" or
 *         "covered" and a string `note` (empty allowed, cut to 300 chars) becomes one
 *         "agent-agenda" message. MEETING-SCOPED like candidates: the agenda is shared, so the
 *         message has no `userId` and server.ts delivers it to every local socket on the
 *         channel. The live status is a hint only; core-api applies it to
 *         `meeting_agenda_items` (a host-set status wins).
 *       - a non-null `nudge` with a UUID `id`, a `type` of "off_agenda", "over_time",
 *         "unaddressed" or "proxy_update" (mytheus-docs ADR-0039 C5), an `item_id` that is a
 *         UUID or null, and a non-empty string `content` (cut to 200 chars) becomes one
 *         "agent-nudge" message. A "proxy_update" nudge carries the update of a participant who
 *         sent their agent instead, so it must name its agenda item (`item_id` a UUID, never
 *         null), its `content` is cut to 2000 chars (the update's own limit, ADR-0039 B5)
 *         rather than 200, and an optional UUID `user_id` (the proxied participant) is
 *         forwarded as `userId`. PRIVATE to the host: tagged with the record's `user_id` (the
 *         MeetingAgent sets it to the host's id), delivered only to that user's sockets, never
 *         to the room.
 *   Anything else (other agents, other kinds, a header without a UUID tenant) is skipped.
 *
 * The moment (polish program, Phase 1: cards tied to the moment, one card per moment). Each
 * candidate item, suggestion item and surfaced note may carry where it came from: a UUID
 * `moment_id` shared by every result of one moment, a `why` line (cut to 200), the verbatim
 * `quote` (cut to 300), `speaker_id` (UUID) and `speaker_label` (cut to 80), and `start_ms` /
 * `end_ms` on the meeting timeline; a candidate may also name `updates_candidate_id` (UUID: it
 * re-emits an existing candidate's id with new content, so the room replaces that card in
 * place). They are forwarded only when present and well formed, never as null, so a record from
 * before them is forwarded exactly as before: snake_case inside candidate and suggestion items,
 * camelCase on the note payload (`momentId`, `why`, `quote`, `speakerId`, `speakerLabel`,
 * `startMs`, `endMs`). A note also forwards its `commitments` (ADR-0039 B6) as
 * `[{ content, dueHint, momentId? }]` (at most five; private like the note), so the room can show
 * a promise on the card of the moment it was made in.
 *
 * Decoding: the consumer calls `registry.decode` with no reader schema, so each record is read
 * with the WRITER schema its registry id names. A symbol added to the `AgentClass` or
 * `AgentResultKind` enum (ADR-0038 D8: `MeetingAgent`, `meeting_update`) decodes
 * as a plain string the moment a producer registers the schema that carries it; nothing here has
 * to be redeployed first, and a symbol this file does not know is simply skipped.
 *
 * Nothing is buffered or replayed: a client that reconnects re-hydrates notes from
 * `GET /api/v1/meetings/:id/worklogs?user_id=me` and candidates from
 * `GET /api/v1/meetings/:id/candidates`, suggestions from `GET /api/v1/suggestions?meeting_id=`,
 * context fills from `GET /api/v1/meetings/:id/context-fills`, the agenda (statuses and agent
 * notes) from `GET /api/v1/meetings/:id`. Nudges are not persisted and never replayed: a nudge
 * the host's socket missed is gone. Alerts are the one exception:
 * an alert for a user not in its meeting stays queued (24 h) and is drained when they join that
 * meeting; the
 * durable copy is re-read from notification-service's `GET /notifications?meeting_id=`.
 */
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { Kafka, type Consumer } from "kafkajs";
import type { Logger } from "pino";

import { pubsubChannel } from "../../../libs/keys";

export const AGENT_TOPICS = ["agent-stream", "agent-results"] as const;
export const AGENT_OUTPUT_GROUP = "realtime-gateway-agent-output";

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export interface AgentDeltaPayload {
  windowId: string;
  seq: number;
  delta: string;
  done: boolean;
}

/**
 * Where an agent result came from in the meeting (polish program, Phase 1: "cards tied to the
 * moment", "one card per moment"). Every field is optional and present only when the producer
 * sent a well-formed value, so a record from before the moment fields existed is forwarded
 * byte-for-byte as it was. snake_case inside candidate and suggestion items (their existing
 * style), camelCase on a note payload (its existing style).
 */
export interface MomentFields {
  /** One id shared by every result the agents produced from one moment (UUID). */
  moment_id?: string;
  /** "Why you're seeing this": one line, at most 200 chars. */
  why?: string;
  /** Candidates only: this result updates the existing candidate with this id (UUID). */
  updates_candidate_id?: string;
  /** The words that were said, verbatim, at most 300 chars. */
  quote?: string;
  /** The speaker's internal `users.id` (UUID), when known. */
  speaker_id?: string;
  /** The speaker's display name as the agent substituted it, at most 80 chars. */
  speaker_label?: string;
  /** The quoted turn on the meeting timeline (ms), the clock `transcript_chunks.start_ms` uses. */
  start_ms?: number;
  end_ms?: number;
}

/** A note's moment, camelCase like the rest of the note payload. */
export interface NoteMomentFields {
  momentId?: string;
  why?: string;
  quote?: string;
  speakerId?: string;
  speakerLabel?: string;
  startMs?: number;
  endMs?: number;
}

/** A promise the note's owner made in that window (ADR-0039 B6), private like the note. */
export interface NoteCommitment {
  content: string;
  dueHint: string | null;
  momentId?: string;
}

export interface AgentNotePayload extends NoteMomentFields {
  windowId: string;
  agent: string;
  content: string;
  confidence: number;
  relevanceScore: number;
  sourceChunkIds: string[];
  /** Present only when the record carried well-formed commitments. */
  commitments?: NoteCommitment[];
}

/** A surfaced decision candidate, fields as in the Phase 8 Part A contract section 1. */
export interface DecisionCandidate extends MomentFields {
  id: string;
  content: string;
  confidence: number;
  surfaced: true;
  rationale: string | null;
  stakeholder_ids: string[];
}

/** A surfaced action candidate, fields as in the Phase 8 Part A contract section 1. */
export interface ActionCandidate extends MomentFields {
  id: string;
  content: string;
  confidence: number;
  surfaced: true;
  assignee_id: string | null;
  due_date: string | null;
  depends_on: string[];
}

export type AgentCandidatePayload =
  | {
      windowId: string;
      agent: "DecisionAgent";
      kind: "decision_candidate";
      candidates: DecisionCandidate[];
    }
  | {
      windowId: string;
      agent: "ActionAgent";
      kind: "action_candidate";
      candidates: ActionCandidate[];
    };

/** One grounding source of a suggestion (mytheus-docs ADR-0033 D3: four kinds). */
export interface SuggestionCitation {
  kind: "transcript" | "document" | "decision" | "action";
  [field: string]: string | number;
}

/** A surfaced QuestionAgent suggestion (mytheus-docs ADR-0034 D3). */
export interface QuestionSuggestion extends MomentFields {
  id: string;
  question: string;
  priority: 1 | 2 | 3;
  confidence: number;
  surfaced: true;
  rationale: string | null;
  citations: SuggestionCitation[];
}

export interface AgentSuggestionPayload {
  windowId: string;
  agent: "QuestionAgent";
  kind: "suggestion";
  suggestions: QuestionSuggestion[];
}

/**
 * A surfaced ContextGapAgent fill (P10 contract section 7). Only the fill travels; the gap and
 * the collaboration log stay server-side.
 */
export interface AgentContextPayload {
  id: string;
  meetingId: string;
  content: string;
  confidence: number;
  rationale: string | null;
  hop: "peer" | "related_meeting" | "rag";
  /** "user:{uuid}" for a peer or related-meeting agent, or "rag" for the requester's own KB. */
  responder: string;
  citations: SuggestionCitation[];
  /** ISO 8601, from the record header's `occurred_at`. */
  createdAt: string;
}

export type AlertType =
  | "conflict"
  | "workload"
  | "dependency"
  | "missing_stakeholder"
  // mytheus-docs ADR-0039 C7
  | "already_decided"
  | "topic_watch";

/** What an alert is about (P11 contract section 1). */
export interface AlertSubject {
  kind: "decision" | "action" | "meeting";
  id: string;
}

/** Something an alert points at, rendered as a chip (P11 contract section 1). */
export interface AlertRelated {
  kind: "decision" | "action" | "user";
  id: string;
  label: string;
}

/**
 * A surfaced ProactiveAgent alert, the socket event "agent-alert" (P11 contract section 4). This
 * exact JSON is also the member stored in the recipient's priority queue, so a replayed record
 * maps to the identical member and ZADD stays idempotent.
 */
export interface AgentAlertPayload {
  id: string;
  /** The meeting the alert's subject belongs to; it is only ever delivered into that room. */
  meetingId: string;
  alertType: AlertType;
  /** 1 = red (conflict), 2 = yellow (workload, dependency), 3 = blue (missing stakeholder). */
  priority: 1 | 2 | 3;
  title: string;
  body: string;
  confidence: number;
  rationale: string | null;
  subject: AlertSubject | null;
  related: AlertRelated[];
  /** A relative app path ("/decisions/{id}"); anything else is dropped to null. */
  link: string | null;
  /** ISO 8601, from the record header's `occurred_at`. */
  createdAt: string;
  // Present only on the ADR-0039 C7 types, absent (not null) on the others so the P11 alerts'
  // queue members are byte-for-byte what they were.
  /** "already_decided": the confirmed decision the room re-opened. */
  decisionId?: string;
  /** "already_decided": that decision's content, at most 500 chars. */
  decisionContent?: string;
  /** "already_decided": when it was decided (as the producer sent it), or null. */
  decidedAt?: string | null;
  /** "topic_watch": the watched topic, at most 80 chars. */
  topic?: string;
  /** "topic_watch": what was said, at most 200 chars; may be empty. */
  snippet?: string;
  /** Both C7 types: the meeting's title, at most 200 chars; may be empty. */
  meetingTitle?: string;
}

/** A MeetingAgent agenda status hint, the socket event "agent-agenda" (ADR-0038 D9). */
export interface AgentAgendaPayload {
  itemId: string;
  status: "in_progress" | "covered";
  /** The agent's note on the item, at most 300 chars; may be empty. */
  note: string;
}

export type NudgeType = "off_agenda" | "over_time" | "unaddressed" | "proxy_update";

/** A MeetingAgent nudge for the host, the socket event "agent-nudge" (ADR-0038 D9). */
export interface AgentNudgePayload {
  id: string;
  type: NudgeType;
  /** The agenda item the nudge is about, or null (an off-agenda nudge names none). Never null
   * for "proxy_update". */
  itemId: string | null;
  /** Never empty; at most 200 chars, or 2000 for "proxy_update" (the proxied update itself). */
  content: string;
  /** "proxy_update" only, when the producer names them: the participant who sent their agent. */
  userId?: string;
}

export type AgentPubsubMessage =
  | { type: "agent-delta"; userId: string; payload: AgentDeltaPayload }
  | { type: "agent-note"; userId: string; payload: AgentNotePayload }
  // Private to its owner, like notes (ADR-0034 D6).
  | { type: "agent-suggestion"; userId: string; payload: AgentSuggestionPayload }
  // Private to the requester, like suggestions (P10 contract section 7).
  | { type: "agent-context"; userId: string; payload: AgentContextPayload }
  // Private to the recipient (P11 contract section 4). Published only by a pod that drained the
  // recipient's priority queue; every pod delivers it to that user's local sockets.
  | { type: "agent-alert"; userId: string; payload: AgentAlertPayload }
  // Private to the meeting's host (ADR-0038 D9): userId is the record's user_id, the host's id.
  | { type: "agent-nudge"; userId: string; payload: AgentNudgePayload }
  // Meeting-scoped: no userId, delivered to every local socket on the meeting channel.
  | { type: "agent-candidate"; payload: AgentCandidatePayload }
  // Meeting-scoped like candidates: the agenda is shared by the whole room (ADR-0038 D9).
  | { type: "agent-agenda"; payload: AgentAgendaPayload }
  // Live translation (polish wave 3): delivered only to the meeting's sockets that chose
  // `payload.language` (privacy.ts).
  | { type: "caption-translation"; payload: CaptionTranslationPayload };

/**
 * One live caption final in one language (agent-stream kind=caption_translation, mytheus-events
 * 490fc46). `text` is set only when `status` is "translated"; "budget_denied" and "unavailable"
 * are calm notices (the caption stays in its original language).
 */
export interface CaptionTranslationPayload {
  chunkId: string;
  language: string;
  status: CaptionTranslationStatus;
  text: string;
}

export type CaptionTranslationStatus = "translated" | "budget_denied" | "unavailable";
const TRANSLATION_STATUSES: readonly string[] = ["translated", "budget_denied", "unavailable"];
const LANGUAGE_RE = /^[a-z]{2}$/;
/** A caption line is short; a translation longer than this is not a caption. */
const MAX_TRANSLATION_CHARS = 4000;

/**
 * The caption translation one agent-stream record carries, or null when the record is not a
 * well-formed `caption_translation` (an unknown status, a bad language, a non-uuid chunk, or a
 * translated record with no text).
 */
export function captionTranslationRecord(value: Decoded): CaptionTranslationPayload | null {
  if (value.kind !== "caption_translation") return null;
  const chunkId = str(value.chunk_id);
  const language = str(value.language);
  const status = str(value.status);
  const text = str(value.delta) ?? "";
  if (!chunkId || !UUID_RE.test(chunkId)) return null;
  if (!language || !LANGUAGE_RE.test(language)) return null;
  if (!status || !TRANSLATION_STATUSES.includes(status)) return null;
  if (status === "translated" && text.trim().length === 0) return null;
  return {
    chunkId,
    language,
    status: status as CaptionTranslationStatus,
    text: status === "translated" ? text.slice(0, MAX_TRANSLATION_CHARS) : "",
  };
}

/**
 * The cross-pod nudge "this user's priority queue has something": published on the meeting
 * channel after an enqueue. Carries no alert content; a pod holding the user's sockets on that
 * channel drains the queue and publishes the popped alerts as "agent-alert" messages.
 */
export interface AlertQueuedNudge {
  type: "agent-alert-queued";
  userId: string;
}

export interface RoutedAgentMessage {
  channel: string;
  message: AgentPubsubMessage;
}

type Decoded = Record<string, unknown>;

function str(v: unknown): string | null {
  return typeof v === "string" ? v : null;
}

function strList(v: unknown): string[] {
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
}

const WHY_MAX = 200;
const QUOTE_MAX = 300;
const SPEAKER_LABEL_MAX = 80;
const COMMITMENT_MAX = 300;
const DUE_HINT_MAX = 80;
const COMMITMENTS_MAX = 5;

/** A trimmed, non-empty string cut to `max` chars, or null. */
function text(v: unknown, max: number): string | null {
  if (typeof v !== "string") return null;
  const t = v.trim();
  return t ? t.slice(0, max) : null;
}

function uuid(v: unknown): string | null {
  return typeof v === "string" && UUID_RE.test(v) ? v : null;
}

/** A whole, non-negative millisecond offset, or null. */
function offsetMs(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) && v >= 0 ? Math.floor(v) : null;
}

/**
 * The moment fields of one candidate or suggestion item, only those present and well formed
 * (absent, not null, otherwise: an item from before these fields existed is unchanged).
 */
export function momentFields(c: Decoded, withUpdates: boolean): MomentFields {
  const out: MomentFields = {};
  const momentId = uuid(c.moment_id);
  if (momentId) out.moment_id = momentId;
  const why = text(c.why, WHY_MAX);
  if (why) out.why = why;
  if (withUpdates) {
    const updates = uuid(c.updates_candidate_id);
    if (updates) out.updates_candidate_id = updates;
  }
  const quote = text(c.quote, QUOTE_MAX);
  if (quote) out.quote = quote;
  const speakerId = uuid(c.speaker_id);
  if (speakerId) out.speaker_id = speakerId;
  const speakerLabel = text(c.speaker_label, SPEAKER_LABEL_MAX);
  if (speakerLabel) out.speaker_label = speakerLabel;
  const start = offsetMs(c.start_ms);
  if (start !== null) out.start_ms = start;
  const end = offsetMs(c.end_ms);
  if (end !== null && (start === null || end >= start)) out.end_ms = end;
  return out;
}

/** The same fields for a note, camelCase like the note payload. */
function noteMoment(p: Decoded): NoteMomentFields {
  const m = momentFields(p, false);
  const out: NoteMomentFields = {};
  if (m.moment_id) out.momentId = m.moment_id;
  if (m.why) out.why = m.why;
  if (m.quote) out.quote = m.quote;
  if (m.speaker_id) out.speakerId = m.speaker_id;
  if (m.speaker_label) out.speakerLabel = m.speaker_label;
  if (m.start_ms !== undefined) out.startMs = m.start_ms;
  if (m.end_ms !== undefined) out.endMs = m.end_ms;
  return out;
}

/** A note's well-formed commitments (at most five), or null when there are none. */
function noteCommitments(p: Decoded): NoteCommitment[] | null {
  if (!Array.isArray(p.commitments)) return null;
  const out: NoteCommitment[] = [];
  for (const raw of p.commitments as unknown[]) {
    if (raw === null || typeof raw !== "object") continue;
    const c = raw as Decoded;
    const content = text(c.content, COMMITMENT_MAX);
    if (!content) continue;
    const entry: NoteCommitment = { content, dueHint: text(c.due_hint, DUE_HINT_MAX) };
    const momentId = uuid(c.moment_id);
    if (momentId) entry.momentId = momentId;
    out.push(entry);
    if (out.length >= COMMITMENTS_MAX) break;
  }
  return out.length ? out : null;
}

function parsePayload(raw: unknown): Decoded | null {
  try {
    const parsed: unknown = JSON.parse(str(raw) ?? "{}");
    return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Decoded)
      : null;
  } catch {
    return null;
  }
}

/** Surfaced candidates with a UUID id and a string content; unknown fields are stripped. */
function surfacedCandidates(payload: Decoded): Decoded[] {
  if (!Array.isArray(payload.candidates)) return [];
  return (payload.candidates as unknown[]).filter(
    (c): c is Decoded =>
      c !== null &&
      typeof c === "object" &&
      (c as Decoded).surfaced === true &&
      typeof (c as Decoded).id === "string" &&
      UUID_RE.test((c as Decoded).id as string) &&
      typeof (c as Decoded).content === "string",
  );
}

function candidateRecord(windowId: string, value: Decoded): AgentCandidatePayload | null {
  const isDecision = value.agent === "DecisionAgent" && value.kind === "decision_candidate";
  const isAction = value.agent === "ActionAgent" && value.kind === "action_candidate";
  if (!isDecision && !isAction) return null;
  const payload = parsePayload(value.payload_json);
  if (!payload) return null;
  const kept = surfacedCandidates(payload);
  if (kept.length === 0) return null;
  const base = (c: Decoded) => ({
    id: c.id as string,
    content: c.content as string,
    confidence: typeof c.confidence === "number" ? c.confidence : 0,
    surfaced: true as const,
    ...momentFields(c, true),
  });
  if (isDecision) {
    return {
      windowId,
      agent: "DecisionAgent",
      kind: "decision_candidate",
      candidates: kept.map((c) => ({
        ...base(c),
        rationale: str(c.rationale),
        stakeholder_ids: strList(c.stakeholder_ids),
      })),
    };
  }
  return {
    windowId,
    agent: "ActionAgent",
    kind: "action_candidate",
    candidates: kept.map((c) => ({
      ...base(c),
      assignee_id: str(c.assignee_id),
      due_date: str(c.due_date),
      depends_on: strList(c.depends_on),
    })),
  };
}

const CITATION_KINDS = new Set(["transcript", "document", "decision", "action"]);

function citations(raw: unknown): SuggestionCitation[] {
  if (!Array.isArray(raw)) return [];
  const out: SuggestionCitation[] = [];
  for (const c of raw as unknown[]) {
    if (c === null || typeof c !== "object" || !CITATION_KINDS.has((c as Decoded).kind as string)) {
      continue;
    }
    const kept: Record<string, string | number> = {};
    for (const [k, v] of Object.entries(c as Decoded)) {
      if (typeof v === "string" || typeof v === "number") kept[k] = v;
    }
    out.push(kept as SuggestionCitation);
  }
  return out;
}

function suggestionRecord(windowId: string, value: Decoded): AgentSuggestionPayload | null {
  if (value.agent !== "QuestionAgent" || value.kind !== "suggestion") return null;
  const payload = parsePayload(value.payload_json);
  if (!payload || !Array.isArray(payload.suggestions)) return null;
  const kept: QuestionSuggestion[] = [];
  for (const raw of payload.suggestions as unknown[]) {
    if (raw === null || typeof raw !== "object") continue;
    const s = raw as Decoded;
    const priority = s.priority;
    if (
      s.surfaced !== true ||
      typeof s.id !== "string" ||
      !UUID_RE.test(s.id) ||
      typeof s.question !== "string" ||
      (priority !== 1 && priority !== 2 && priority !== 3)
    ) {
      continue;
    }
    kept.push({
      id: s.id,
      question: s.question,
      priority,
      confidence: typeof s.confidence === "number" ? s.confidence : 0,
      surfaced: true,
      rationale: str(s.rationale),
      citations: citations(s.citations),
      ...momentFields(s, false),
    });
  }
  if (kept.length === 0) return null;
  return { windowId, agent: "QuestionAgent", kind: "suggestion", suggestions: kept };
}

const HOPS = new Set(["peer", "related_meeting", "rag"]);
const RESPONDER_RE = /^(rag|user:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i;

/**
 * `occurred_at` is timestamp-micros, decoded as a number (or bigint). A value too small to be
 * micros in this century is read as milliseconds (some test producers send `Date.now()`).
 * Anything unusable falls back to now.
 */
function occurredAtIso(header: Decoded): string {
  const raw = header.occurred_at;
  const n = typeof raw === "bigint" ? Number(raw) : typeof raw === "number" ? raw : NaN;
  const ms = n >= 1e14 ? n / 1000 : n;
  const date = Number.isFinite(ms) && ms > 0 ? new Date(ms) : new Date();
  return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString();
}

function contextFillRecord(
  meetingId: string,
  header: Decoded,
  value: Decoded,
): AgentContextPayload | null {
  if (value.agent !== "ContextGapAgent" || value.kind !== "context_fill") return null;
  const fill = parsePayload(value.payload_json)?.fill;
  if (fill === null || fill === undefined || typeof fill !== "object" || Array.isArray(fill)) {
    return null;
  }
  const f = fill as Decoded;
  if (
    f.surfaced !== true ||
    typeof f.id !== "string" ||
    !UUID_RE.test(f.id) ||
    typeof f.content !== "string" ||
    f.content.length === 0 ||
    typeof f.hop !== "string" ||
    !HOPS.has(f.hop) ||
    typeof f.responder !== "string" ||
    !RESPONDER_RE.test(f.responder)
  ) {
    return null;
  }
  return {
    id: f.id,
    meetingId,
    content: f.content,
    confidence: typeof f.confidence === "number" ? f.confidence : 0,
    rationale: str(f.rationale),
    hop: f.hop as AgentContextPayload["hop"],
    responder: f.responder,
    citations: citations(f.citations),
    createdAt: occurredAtIso(header),
  };
}

const ALERT_TYPES = new Set([
  "conflict",
  "workload",
  "dependency",
  "missing_stakeholder",
  "already_decided",
  "topic_watch",
]);
const MAX_DECISION_CONTENT = 500;
const MAX_TOPIC = 80;
const MAX_SNIPPET = 200;
const MAX_MEETING_TITLE = 200;
const MAX_DECIDED_AT = 64;
const SUBJECT_KINDS = new Set(["decision", "action", "meeting"]);
const RELATED_KINDS = new Set(["decision", "action", "user"]);
const MAX_RELATED = 10;

function alertSubject(raw: unknown): AlertSubject | null {
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) return null;
  const r = raw as Decoded;
  if (typeof r.kind !== "string" || !SUBJECT_KINDS.has(r.kind)) return null;
  if (typeof r.id !== "string" || !UUID_RE.test(r.id)) return null;
  return { kind: r.kind as AlertSubject["kind"], id: r.id };
}

function alertRelated(raw: unknown): AlertRelated[] {
  if (!Array.isArray(raw)) return [];
  const out: AlertRelated[] = [];
  for (const item of raw as unknown[]) {
    if (item === null || typeof item !== "object") continue;
    const r = item as Decoded;
    if (typeof r.kind !== "string" || !RELATED_KINDS.has(r.kind)) continue;
    if (typeof r.id !== "string" || !UUID_RE.test(r.id)) continue;
    out.push({
      kind: r.kind as AlertRelated["kind"],
      id: r.id,
      label: typeof r.label === "string" ? r.label.slice(0, 120) : "",
    });
    if (out.length >= MAX_RELATED) break;
  }
  return out;
}

/** Only a same-origin relative path ("/x"), never "//host" or a scheme, can reach a client href. */
function relativeLink(raw: unknown): string | null {
  if (typeof raw !== "string" || !raw.startsWith("/") || raw.startsWith("//")) return null;
  if (raw.includes("\\") || /[\s<>"]/.test(raw)) return null;
  return raw.slice(0, 300);
}

/**
 * The ADR-0039 C7 fields of an "already_decided" or "topic_watch" alert, or null when a required
 * one is missing (the alert is then dropped). `{}` for the P11 types, which carry none.
 */
function alertExtras(alertType: string, a: Decoded): Partial<AgentAlertPayload> | null {
  const meetingTitle = () => (typeof a.meeting_title === "string" ? a.meeting_title : "");
  if (alertType === "already_decided") {
    if (typeof a.decision_id !== "string" || !UUID_RE.test(a.decision_id)) return null;
    if (typeof a.decision_content !== "string" || a.decision_content.trim().length === 0) {
      return null;
    }
    return {
      decisionId: a.decision_id,
      decisionContent: a.decision_content.trim().slice(0, MAX_DECISION_CONTENT),
      decidedAt: typeof a.decided_at === "string" ? a.decided_at.slice(0, MAX_DECIDED_AT) : null,
      meetingTitle: meetingTitle().slice(0, MAX_MEETING_TITLE),
    };
  }
  if (alertType === "topic_watch") {
    if (typeof a.topic !== "string" || a.topic.trim().length === 0) return null;
    return {
      topic: a.topic.trim().slice(0, MAX_TOPIC),
      snippet: typeof a.snippet === "string" ? a.snippet.slice(0, MAX_SNIPPET) : "",
      meetingTitle: meetingTitle().slice(0, MAX_MEETING_TITLE),
    };
  }
  return {};
}

function alertRecord(meetingId: string, header: Decoded, value: Decoded): AgentAlertPayload | null {
  if (value.agent !== "ProactiveAgent" || value.kind !== "alert") return null;
  const alert = parsePayload(value.payload_json)?.alert;
  if (alert === null || alert === undefined || typeof alert !== "object" || Array.isArray(alert)) {
    return null;
  }
  const a = alert as Decoded;
  const priority = a.priority;
  if (
    a.surfaced !== true ||
    typeof a.id !== "string" ||
    !UUID_RE.test(a.id) ||
    typeof a.alert_type !== "string" ||
    !ALERT_TYPES.has(a.alert_type) ||
    (priority !== 1 && priority !== 2 && priority !== 3) ||
    typeof a.title !== "string" ||
    a.title.trim().length === 0
  ) {
    return null;
  }
  // An alert is only ever delivered into its own meeting: a payload naming another is dropped.
  if (
    a.meeting_id !== undefined &&
    a.meeting_id !== null &&
    (typeof a.meeting_id !== "string" || a.meeting_id.toLowerCase() !== meetingId.toLowerCase())
  ) {
    return null;
  }
  const extras = alertExtras(a.alert_type, a);
  if (extras === null) return null;
  return {
    id: a.id,
    meetingId,
    alertType: a.alert_type as AlertType,
    priority,
    title: a.title.trim().slice(0, 120),
    body: typeof a.body === "string" ? a.body.slice(0, 400) : "",
    confidence: typeof a.confidence === "number" ? a.confidence : 0,
    rationale: str(a.rationale),
    subject: alertSubject(a.subject),
    related: alertRelated(a.related),
    link: relativeLink(a.link),
    createdAt: occurredAtIso(header),
    ...extras,
  };
}

const AGENDA_STATUSES = new Set(["in_progress", "covered"]);
const NUDGE_TYPES = new Set(["off_agenda", "over_time", "unaddressed", "proxy_update"]);
const MAX_AGENDA_NOTE = 300;
const MAX_NUDGE_CONTENT = 200;
/** A proxied participant's update is up to 2000 chars (ADR-0039 B5); the nudge carries it whole. */
const MAX_PROXY_UPDATE_CONTENT = 2000;

function agendaUpdate(raw: unknown): AgentAgendaPayload | null {
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) return null;
  const u = raw as Decoded;
  if (
    typeof u.item_id !== "string" ||
    !UUID_RE.test(u.item_id) ||
    typeof u.status !== "string" ||
    !AGENDA_STATUSES.has(u.status) ||
    typeof u.note !== "string"
  ) {
    return null;
  }
  return {
    itemId: u.item_id,
    status: u.status as AgentAgendaPayload["status"],
    note: u.note.slice(0, MAX_AGENDA_NOTE),
  };
}

function meetingNudge(raw: unknown): AgentNudgePayload | null {
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) return null;
  const n = raw as Decoded;
  const itemId = n.item_id;
  if (
    typeof n.id !== "string" ||
    !UUID_RE.test(n.id) ||
    typeof n.type !== "string" ||
    !NUDGE_TYPES.has(n.type) ||
    !(itemId === null || (typeof itemId === "string" && UUID_RE.test(itemId))) ||
    typeof n.content !== "string" ||
    n.content.trim().length === 0
  ) {
    return null;
  }
  if (n.type === "proxy_update") {
    // Fired when a proxied participant's item goes in progress: it always names the item.
    if (itemId === null) return null;
    const proxied = typeof n.user_id === "string" && UUID_RE.test(n.user_id) ? n.user_id : null;
    return {
      id: n.id,
      type: "proxy_update",
      itemId,
      content: n.content.slice(0, MAX_PROXY_UPDATE_CONTENT),
      ...(proxied ? { userId: proxied } : {}),
    };
  }
  return {
    id: n.id,
    type: n.type as NudgeType,
    itemId,
    content: n.content.slice(0, MAX_NUDGE_CONTENT),
  };
}

/**
 * Splits one MeetingAgent `meeting_update` record into its "agent-agenda" messages (one per
 * valid `updates` entry, in order) followed by at most one "agent-nudge" for the host.
 */
function meetingUpdateMessages(
  channel: string,
  userId: string,
  value: Decoded,
): RoutedAgentMessage[] {
  if (value.agent !== "MeetingAgent" || value.kind !== "meeting_update") return [];
  const p = parsePayload(value.payload_json);
  if (!p || p.surfaced !== true) return [];
  const out: RoutedAgentMessage[] = [];
  const seen = new Set<string>();
  for (const raw of Array.isArray(p.updates) ? (p.updates as unknown[]) : []) {
    const update = agendaUpdate(raw);
    // at most one update per item per window (ADR-0038 D7): a repeated item keeps its first entry
    if (!update || seen.has(update.itemId)) continue;
    seen.add(update.itemId);
    out.push({ channel, message: { type: "agent-agenda", payload: update } });
  }
  const nudge = meetingNudge(p.nudge);
  if (nudge) out.push({ channel, message: { type: "agent-nudge", userId, payload: nudge } });
  return out;
}

/** The org-scoped meeting channel of a record whose tenant, meeting and user are UUIDs. */
function recordTarget(value: Decoded): { channel: string; userId: string } | null {
  const header = (value.header ?? {}) as Decoded;
  const orgId = str(header.org_id);
  const meetingId = str(value.meeting_id);
  const userId = str(value.user_id);
  if (!orgId || !meetingId || !userId) return null;
  if (!UUID_RE.test(orgId) || !UUID_RE.test(meetingId) || !UUID_RE.test(userId)) return null;
  return { channel: pubsubChannel(orgId, meetingId), userId };
}

/**
 * Every pub/sub message one decoded Avro value maps to, in publish order; empty to skip it. A
 * MeetingAgent `meeting_update` record maps to several (its agenda updates, then its nudge);
 * every other record maps to at most one, exactly as `routeAgentRecord`.
 */
export function routeAgentRecords(topic: string, value: Decoded): RoutedAgentMessage[] {
  if (topic === "agent-results" && value.agent === "MeetingAgent") {
    const target = recordTarget(value);
    return target ? meetingUpdateMessages(target.channel, target.userId, value) : [];
  }
  const routed = routeAgentRecord(topic, value);
  return routed ? [routed] : [];
}

/**
 * Pure mapping from one decoded Avro value to the pub/sub message, or null to skip it. A
 * MeetingAgent record is never single: it returns null here, use `routeAgentRecords`.
 */
export function routeAgentRecord(topic: string, value: Decoded): RoutedAgentMessage | null {
  const header = (value.header ?? {}) as Decoded;
  const target = recordTarget(value);
  if (!target) return null;
  const { channel, userId } = target;
  const meetingId = str(value.meeting_id) as string;
  const windowId = str(value.window_id) ?? "";

  if (topic === "agent-stream") {
    // Live translation shares agent-stream (kind=caption_translation, user_id the nil UUID):
    // never a token delta for a person. Anything else is routed as before, by topic.
    if (value.kind === "caption_translation") {
      const translation = captionTranslationRecord(value);
      return translation
        ? { channel, message: { type: "caption-translation", payload: translation } }
        : null;
    }
    return {
      channel,
      message: {
        type: "agent-delta",
        userId,
        payload: {
          windowId,
          seq: Number(value.seq ?? 0),
          delta: str(value.delta) ?? "",
          done: value.done === true,
        },
      },
    };
  }

  if (topic === "agent-results") {
    const candidate = candidateRecord(windowId, value);
    if (candidate) return { channel, message: { type: "agent-candidate", payload: candidate } };
    const suggestion = suggestionRecord(windowId, value);
    if (suggestion) {
      return { channel, message: { type: "agent-suggestion", userId, payload: suggestion } };
    }
    if (value.agent === "ProactiveAgent") {
      const alert = alertRecord(meetingId, header, value);
      return alert ? { channel, message: { type: "agent-alert", userId, payload: alert } } : null;
    }
    if (value.agent === "ContextGapAgent") {
      const fill = contextFillRecord(meetingId, header, value);
      return fill ? { channel, message: { type: "agent-context", userId, payload: fill } } : null;
    }
    if (value.agent !== "PersonalListenerAgent" || value.kind !== "note") return null;
    const payload = parsePayload(value.payload_json);
    if (payload?.surfaced !== true) return null;
    const commitments = noteCommitments(payload);
    return {
      channel,
      message: {
        type: "agent-note",
        userId,
        payload: {
          windowId,
          agent: "PersonalListenerAgent",
          content: str(value.content) ?? "",
          confidence: Number(value.confidence ?? 0),
          relevanceScore: Number(value.relevance_score ?? 0),
          sourceChunkIds: strList(value.source_chunk_ids),
          ...noteMoment(payload),
          ...(commitments ? { commitments } : {}),
        },
      },
    };
  }
  return null;
}

/** The subset of ioredis this module calls. */
export interface Publisher {
  publish(channel: string, message: string): Promise<number>;
}

export interface AgentOutputConsumer {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export function createAgentOutputConsumer(params: {
  brokers: string[];
  schemaRegistryUrl: string;
  /** Defaults to AGENT_OUTPUT_GROUP. */
  groupId?: string;
  publisher: Publisher;
  /**
   * Receives every routed "agent-alert" instead of the publisher: alerts go through the
   * recipient's priority queue (alertQueue.ts), never straight onto the channel. Without it an
   * alert is skipped (logged), never published unqueued.
   */
  onAlert?: (routed: RoutedAgentMessage) => Promise<void>;
  logger: Logger;
}): AgentOutputConsumer {
  const kafka = new Kafka({ clientId: "realtime-gateway", brokers: params.brokers });
  const registry = new SchemaRegistry({ host: params.schemaRegistryUrl });
  let consumer: Consumer | undefined;

  return {
    async start() {
      consumer = kafka.consumer({ groupId: params.groupId ?? AGENT_OUTPUT_GROUP });
      await consumer.connect();
      for (const topic of AGENT_TOPICS) {
        await consumer.subscribe({ topic, fromBeginning: false });
      }
      await consumer.run({
        eachMessage: async ({ topic, message }) => {
          if (!message.value) return;
          let value: Decoded;
          try {
            value = (await registry.decode(message.value)) as Decoded;
          } catch (err) {
            params.logger.warn({ err, topic }, "undecodable agent output record skipped");
            return;
          }
          for (const routed of routeAgentRecords(topic, value)) {
            if (routed.message.type === "agent-alert") {
              if (params.onAlert) await params.onAlert(routed);
              else params.logger.warn({ topic }, "agent alert skipped: no alert queue configured");
              continue;
            }
            await params.publisher.publish(routed.channel, JSON.stringify(routed.message));
          }
        },
      });
    },
    async stop() {
      await consumer?.disconnect();
    },
  };
}
