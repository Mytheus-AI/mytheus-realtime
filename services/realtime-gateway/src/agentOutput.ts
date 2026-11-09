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
 *   Anything else (other agents, other kinds, a header without a UUID tenant) is skipped.
 *
 * Nothing is buffered or replayed: a client that reconnects re-hydrates notes from
 * `GET /api/v1/meetings/:id/worklogs?user_id=me` and candidates from
 * `GET /api/v1/meetings/:id/candidates`.
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

export type AgentPubsubMessage =
  | { type: "agent-delta"; userId: string; payload: AgentDeltaPayload }
  | { type: "agent-note"; userId: string; payload: AgentNotePayload }
  // Meeting-scoped: no userId, delivered to every local socket on the meeting channel.
  | { type: "agent-candidate"; payload: AgentCandidatePayload };

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
  publisher: Publisher;
  logger: Logger;
}): AgentOutputConsumer {
  const kafka = new Kafka({ clientId: "realtime-gateway", brokers: params.brokers });
  const registry = new SchemaRegistry({ host: params.schemaRegistryUrl });
  let consumer: Consumer | undefined;

  return {
    async start() {
      consumer = kafka.consumer({ groupId: AGENT_OUTPUT_GROUP });
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
          await params.publisher.publish(routed.channel, JSON.stringify(routed.message));
        },
      });
    },
    async stop() {
      await consumer?.disconnect();
    },
  };
}
