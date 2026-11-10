/**
 * Off the record and live caption translation in the room (polish program wave 3).
 *
 * OFF THE RECORD. core-api owns the state: the host (or the meeting's creator) starts and ends a
 * segment over REST, core-api sets or deletes `nexus:{org_id}:off-record:{meeting_id}` and
 * publishes `{type: "off-record", payload}` on the meeting's fan-out channel. This module:
 *   - forwards that message to every local socket on the channel ("off-record");
 *   - sends the current state to a socket right after it joins (read from the flag), so a late
 *     joiner or a reconnect sees "Off the record since 12:04" without waiting for a change;
 *   - carries a participant's REQUEST to go off the record to the room ("off-record-request"),
 *     at most once per `REQUEST_MIN_INTERVAL_MS` per socket, and the host's "Not now"
 *     ("off-record-answer"), announced only when core-api confirms the answerer steers the
 *     meeting. A request stores nothing; the host grants it by starting a segment with
 *     `requested_by`. Who may do what is core-api's rule (routes/off_record.py); this file only
 *     carries messages.
 *
 * LIVE TRANSLATION. A viewer picks a caption language ("caption-language"); the choice is kept on
 * the socket and in the Redis hash `nexus:{org_id}:caption-langs:{meeting_id}` (field = socket id),
 * which the agent runtime reads to know which languages to translate finals into. A translation
 * arrives as `{type: "caption-translation", payload: {chunkId, language, status, text}}` on the
 * channel (agentOutput.ts) and is delivered only to the local sockets that chose that language.
 *
 * Client -> server (namespace `/v1`; each acks `{ ok: true, ... } | { ok: false, error }`):
 *   "caption-language"   ({ meetingId, language })   language: one of CAPTION_LANGUAGES, or null
 *                                                     for the original (stops translating for it)
 *   "off-record-request" ({ meetingId })              ack error RATE_LIMITED | ALREADY_OFF_RECORD
 *   "off-record-answer"  ({ meetingId, userId })      the host's "Not now" to userId's request
 * Server -> client (`{ meetingId, payload }`):
 *   "off-record"           { active, segmentId, startedAt, startedBy, requestedBy, endedAt,
 *                            endedBy, totalMs, byUserId, at }   to everyone; also to a joiner
 *   "off-record-request"   { userId, at }                        to everyone in the meeting
 *   "off-record-answer"    { userId, answer: "declined", byUserId, at }   to everyone
 *   "caption-translation"  { chunkId, language, status, text }   only to sockets on `language`
 */
import type { Redis } from "ioredis";
import type { Logger } from "pino";
import type { Namespace, Socket } from "socket.io";

import { captionLangsKey, offRecordKey, roomName } from "../../../libs/keys";
import type { CoreApiClient } from "./coreApiClient";
import { steers } from "./roomEvents";

export const CAPTION_LANGUAGES = [
  "en",
  "es",
  "fr",
  "de",
  "pt",
  "it",
  "nl",
  "hi",
  "ja",
  "ko",
  "zh",
  "ar",
] as const;
export type CaptionLanguage = (typeof CAPTION_LANGUAGES)[number];
export const LANGS_TTL_MS = 6 * 60 * 60 * 1000;
export const REQUEST_MIN_INTERVAL_MS = 20_000;

export type PrivacyError =
  | "INVALID_PAYLOAD"
  | "NOT_JOINED"
  | "NOT_HOST"
  | "RATE_LIMITED"
  | "ALREADY_OFF_RECORD"
  | "UNAVAILABLE";

type Ack = (
  res: ({ ok: true } & Record<string, unknown>) | { ok: false; error: PrivacyError },
) => void;

export interface PrivacySocketData {
  bearerToken: string;
  joinedMeetings: Set<string>;
  presenceOrgId: string;
  presenceUserId: string;
  /** meetingId -> the caption language this socket chose (absent: the original). */
  captionLangs?: Map<string, CaptionLanguage>;
  lastOffRecordRequestAt?: number;
}

export interface OffRecordPayload {
  active: boolean;
  segmentId: string | null;
  startedAt: string | null;
  startedBy: string | null;
  requestedBy: string | null;
  endedAt: string | null;
  endedBy: string | null;
  totalMs: number | null;
  byUserId: string | null;
  at: string;
}

const ID = /^[A-Za-z0-9_-]{1,80}$/;

export function isCaptionLanguage(v: unknown): v is CaptionLanguage {
  return typeof v === "string" && (CAPTION_LANGUAGES as readonly string[]).includes(v);
}

function strOrNull(v: unknown): string | null {
  return typeof v === "string" && v.length > 0 && v.length <= 80 ? v : null;
}

/**
 * A core-api "off-record" payload, re-shaped field by field so nothing but the known fields
 * reaches a client; null when it is not one (no boolean `active`).
 */
export function sanitizeOffRecord(raw: unknown, nowIso: string): OffRecordPayload | null {
  if (typeof raw !== "object" || raw === null) return null;
  const p = raw as Record<string, unknown>;
  if (typeof p.active !== "boolean") return null;
  return {
    active: p.active,
    segmentId: strOrNull(p.segmentId),
    startedAt: strOrNull(p.startedAt),
    startedBy: strOrNull(p.startedBy),
    requestedBy: strOrNull(p.requestedBy),
    endedAt: strOrNull(p.endedAt),
    endedBy: strOrNull(p.endedBy),
    totalMs: typeof p.totalMs === "number" && Number.isFinite(p.totalMs) ? p.totalMs : null,
    byUserId: strOrNull(p.byUserId),
    at: strOrNull(p.at) ?? nowIso,
  };
}

/** The state a joiner gets, from the flag core-api keeps (null raw: on the record). */
export function offRecordFromFlag(raw: string | null, nowIso: string): OffRecordPayload {
  const base: OffRecordPayload = {
    active: false,
    segmentId: null,
    startedAt: null,
    startedBy: null,
    requestedBy: null,
    endedAt: null,
    endedBy: null,
    totalMs: null,
    byUserId: null,
    at: nowIso,
  };
  if (raw === null) return base;
  let flag: Record<string, unknown> = {};
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (typeof parsed === "object" && parsed !== null) flag = parsed as Record<string, unknown>;
  } catch {
    // A flag that is set but unreadable is still a flag: off the record, details unknown.
  }
  return {
    ...base,
    active: true,
    segmentId: strOrNull(flag.segment_id),
    startedAt: strOrNull(flag.started_at),
    startedBy: strOrNull(flag.started_by),
  };
}

export interface PrivacyDeps {
  nsp: Namespace;
  redis: Redis;
  coreApi: CoreApiClient;
  logger: Logger;
  now?: () => number;
}

export function createPrivacy(deps: PrivacyDeps) {
  const { nsp, redis, coreApi, logger } = deps;
  const now = deps.now ?? (() => Date.now());
  const iso = () => new Date(now()).toISOString();

  function joined(data: PrivacySocketData, meetingId: unknown, ack: Ack): meetingId is string {
    if (typeof meetingId !== "string" || meetingId.length === 0) {
      ack({ ok: false, error: "INVALID_PAYLOAD" });
      return false;
    }
    if (!data.joinedMeetings.has(meetingId)) {
      ack({ ok: false, error: "NOT_JOINED" });
      return false;
    }
    return true;
  }

  function attach(socket: Socket): void {
    const data = socket.data as PrivacySocketData;
    const safeAck = (raw: unknown): Ack => (typeof raw === "function" ? (raw as Ack) : () => {});

    socket.on(
      "caption-language",
      async (payload: { meetingId?: unknown; language?: unknown }, rawAck?: unknown) => {
        const ack = safeAck(rawAck);
        const { meetingId, language } = payload ?? {};
        if (!joined(data, meetingId, ack)) return;
        const original = language === null || language === undefined || language === "";
        if (!original && !isCaptionLanguage(language)) {
          ack({ ok: false, error: "INVALID_PAYLOAD" });
          return;
        }
        const key = captionLangsKey(data.presenceOrgId, meetingId);
        try {
          if (original) {
            await redis.hdel(key, socket.id);
          } else {
            await redis.hset(key, socket.id, language);
            await redis.pexpire(key, LANGS_TTL_MS);
          }
        } catch (err) {
          logger.error({ err, meetingId }, "caption language not stored");
          ack({ ok: false, error: "UNAVAILABLE" });
          return;
        }
        data.captionLangs ??= new Map();
        if (original) data.captionLangs.delete(meetingId);
        else data.captionLangs.set(meetingId, language);
        ack({ ok: true, language: original ? null : language });
      },
    );

    socket.on("off-record-request", async (payload: { meetingId?: unknown }, rawAck?: unknown) => {
      const ack = safeAck(rawAck);
      const meetingId = payload?.meetingId;
      if (!joined(data, meetingId, ack)) return;
      const at = now();
      if (
        data.lastOffRecordRequestAt !== undefined &&
        at - data.lastOffRecordRequestAt < REQUEST_MIN_INTERVAL_MS
      ) {
        ack({ ok: false, error: "RATE_LIMITED" });
        return;
      }
      try {
        if (await redis.exists(offRecordKey(data.presenceOrgId, meetingId))) {
          ack({ ok: false, error: "ALREADY_OFF_RECORD" });
          return;
        }
      } catch (err) {
        logger.warn({ err, meetingId }, "off-record flag unreadable on request; asking anyway");
      }
      data.lastOffRecordRequestAt = at;
      nsp.to(roomName(data.presenceOrgId, meetingId)).emit("off-record-request", {
        meetingId,
        payload: { userId: data.presenceUserId, at: new Date(at).toISOString() },
      });
      ack({ ok: true });
    });

    socket.on(
      "off-record-answer",
      async (payload: { meetingId?: unknown; userId?: unknown }, rawAck?: unknown) => {
        const ack = safeAck(rawAck);
        const { meetingId, userId } = payload ?? {};
        if (!joined(data, meetingId, ack)) return;
        if (typeof userId !== "string" || !ID.test(userId)) {
          ack({ ok: false, error: "INVALID_PAYLOAD" });
          return;
        }
        const meeting = await coreApi.getMeeting({ meetingId, bearerToken: data.bearerToken });
        if (meeting === null) {
          ack({ ok: false, error: "UNAVAILABLE" });
          return;
        }
        if (!steers(meeting, data.presenceUserId)) {
          ack({ ok: false, error: "NOT_HOST" });
          return;
        }
        nsp.to(roomName(data.presenceOrgId, meetingId)).emit("off-record-answer", {
          meetingId,
          payload: { userId, answer: "declined", byUserId: data.presenceUserId, at: iso() },
        });
        ack({ ok: true });
      },
    );
  }

  /** The current state, to this socket only, right after its join. Never throws. */
  async function sendState(socket: Socket, meetingId: string): Promise<void> {
    const data = socket.data as PrivacySocketData;
    let raw: string | null;
    try {
      raw = await redis.get(offRecordKey(data.presenceOrgId, meetingId));
    } catch (err) {
      logger.warn({ err, meetingId }, "off-record state not sent on join: flag unreadable");
      return;
    }
    socket.emit("off-record", { meetingId, payload: offRecordFromFlag(raw, iso()) });
  }

  /** Forget this socket's caption language for the meeting. Never throws. */
  async function releaseOnDisconnect(
    data: PrivacySocketData,
    socketId: string,
    meetingId: string,
  ): Promise<void> {
    data.captionLangs?.delete(meetingId);
    try {
      await redis.hdel(captionLangsKey(data.presenceOrgId, meetingId), socketId);
    } catch (err) {
      logger.error({ err, meetingId }, "caption language release failed on disconnect");
    }
  }

  /**
   * Deliver one channel message this module owns to this pod's sockets on the channel. Returns
   * false for any other message type. `meetingId` comes from the channel (the authenticated
   * fact), never from the body.
   */
  function deliver(
    meetingId: string,
    localSockets: Set<Socket>,
    msg: { type?: string; payload?: unknown },
  ): boolean {
    if (msg.type === "off-record") {
      const payload = sanitizeOffRecord(msg.payload, iso());
      if (payload === null) return true;
      for (const socket of localSockets) socket.emit("off-record", { meetingId, payload });
      return true;
    }
    if (msg.type === "caption-translation") {
      const payload = msg.payload as { language?: unknown } | undefined;
      const language = payload?.language;
      if (!isCaptionLanguage(language)) return true;
      for (const socket of localSockets) {
        const chosen = (socket.data as PrivacySocketData).captionLangs?.get(meetingId);
        if (chosen === language) socket.emit("caption-translation", { meetingId, payload });
      }
      return true;
    }
    return false;
  }

  return { attach, sendState, releaseOnDisconnect, deliver };
}

export type Privacy = ReturnType<typeof createPrivacy>;
