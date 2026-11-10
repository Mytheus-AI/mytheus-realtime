/**
 * Agent output bridge: `agent-stream` and `agent-results` from Redpanda to the participant's
 * own sockets (PersonalListener) or to the whole meeting (decision/action candidates).
 * Plan 7-9 section 3.4 steps 2 and 4; mytheus-docs ADR-0030 D5; Phase 8 Part A section 3.
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
 *      "agent-candidate" goes to every local socket on the channel (the channel is org-scoped).
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
 *   Anything else (other agents, other kinds, a header without a UUID tenant) is skipped.
 *
 * Nothing is buffered or replayed: a client that reconnects re-hydrates notes from
 * `GET /api/v1/meetings/:id/worklogs?user_id=me` and candidates from
 * `GET /api/v1/meetings/:id/candidates`, suggestions from `GET /api/v1/suggestions?meeting_id=`,
 * context fills from `GET /api/v1/meetings/:id/context-fills`. Alerts are the one exception:
 * an alert for a user with no socket stays queued (24 h) and is drained when they join; the
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

export interface AgentNotePayload {
  windowId: string;
  agent: string;
  content: string;
  confidence: number;
  relevanceScore: number;
  sourceChunkIds: string[];
}

/** A surfaced decision candidate, fields as in the Phase 8 Part A contract section 1. */
export interface DecisionCandidate {
  id: string;
  content: string;
  confidence: number;
  surfaced: true;
  rationale: string | null;
  stakeholder_ids: string[];
}

/** A surfaced action candidate, fields as in the Phase 8 Part A contract section 1. */
export interface ActionCandidate {
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
export interface QuestionSuggestion {
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

export type AlertType = "conflict" | "workload" | "dependency" | "missing_stakeholder";

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
  /** The meeting the alert's subject belongs to (may differ from the room it is delivered in). */
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
  // Meeting-scoped: no userId, delivered to every local socket on the meeting channel.
  | { type: "agent-candidate"; payload: AgentCandidatePayload };

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

const ALERT_TYPES = new Set(["conflict", "workload", "dependency", "missing_stakeholder"]);
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
  };
}

/** Pure mapping from one decoded Avro value to the pub/sub message, or null to skip it. */
export function routeAgentRecord(topic: string, value: Decoded): RoutedAgentMessage | null {
  const header = (value.header ?? {}) as Decoded;
  const orgId = str(header.org_id);
  const meetingId = str(value.meeting_id);
  const userId = str(value.user_id);
  if (!orgId || !meetingId || !userId) return null;
  if (!UUID_RE.test(orgId) || !UUID_RE.test(meetingId) || !UUID_RE.test(userId)) return null;
  const channel = pubsubChannel(orgId, meetingId);
  const windowId = str(value.window_id) ?? "";

  if (topic === "agent-stream") {
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
          const routed = routeAgentRecord(topic, value);
          if (!routed) return;
          if (routed.message.type === "agent-alert") {
            if (params.onAlert) await params.onAlert(routed);
            else params.logger.warn({ topic }, "agent alert skipped: no alert queue configured");
            return;
          }
          await params.publisher.publish(routed.channel, JSON.stringify(routed.message));
        },
      });
    },
    async stop() {
      await consumer?.disconnect();
    },
  };
}
