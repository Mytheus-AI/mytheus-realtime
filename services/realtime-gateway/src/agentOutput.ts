/**
 * Agent output bridge: `agent-stream` and `agent-results` from Redpanda to the participant's
 * own sockets. Plan 7-9 section 3.4 steps 2 and 4; mytheus-docs ADR-0030 D5.
 *
 * Delivery path:
 *   1. Every gateway pod runs this consumer in ONE consumer group, so each record is handled by
 *      exactly one pod.
 *   2. That pod turns it into a small JSON message tagged with the target `userId` and
 *      publishes it on the meeting's canonical channel `nexus:{org_id}:pubsub:meeting:{meeting_id}`
 *      (the same channel captions use; no new Redis key).
 *   3. Every pod with local sockets on that channel receives it and emits it ONLY to sockets
 *      whose core-api-resolved internal user id equals `userId` (server.ts). A PersonalListener
 *      note is private to its participant; the meeting room would leak it to everyone.
 *
 * What is forwarded:
 *   - `agent-stream` deltas as "agent-delta". The agent only streams notes it already knows are
 *     in the surfaced band (ADR-0030 D4). Deltas are throwaway: emitted volatile, shed first.
 *   - `agent-results` `PersonalListenerAgent` notes with `payload_json.surfaced === true` as
 *     "agent-note". A silent-band note is persisted by core-api and never shown live.
 *   Anything else (other agents, other kinds, a header without a UUID tenant) is skipped.
 *
 * Nothing is buffered or replayed: a client that reconnects re-hydrates from
 * `GET /api/v1/meetings/:id/worklogs?user_id=me`.
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

export type AgentPubsubMessage =
  | { type: "agent-delta"; userId: string; payload: AgentDeltaPayload }
  | { type: "agent-note"; userId: string; payload: AgentNotePayload };

export interface RoutedAgentMessage {
  channel: string;
  message: AgentPubsubMessage;
}

type Decoded = Record<string, unknown>;

function str(v: unknown): string | null {
  return typeof v === "string" ? v : null;
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
    if (value.agent !== "PersonalListenerAgent" || value.kind !== "note") return null;
    let payload: Decoded;
    try {
      payload = JSON.parse(str(value.payload_json) ?? "{}") as Decoded;
    } catch {
      return null;
    }
    if (payload.surfaced !== true) return null;
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
          sourceChunkIds: Array.isArray(value.source_chunk_ids)
            ? (value.source_chunk_ids as unknown[]).filter(
                (c): c is string => typeof c === "string",
              )
            : [],
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
