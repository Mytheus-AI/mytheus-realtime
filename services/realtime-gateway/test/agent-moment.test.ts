/**
 * Moment fields on agent output (polish program, Phase 1: cards tied to the moment, one card per
 * moment). Candidate items, suggestion items and surfaced notes carry where they came from; the
 * gateway forwards those fields only when present and well formed, and a record from before they
 * existed is forwarded exactly as before.
 */
import { randomUUID } from "node:crypto";

import { describe, expect, it } from "vitest";

import { momentFields, routeAgentRecord } from "../src/agentOutput";

const org = randomUUID();
const meeting = randomUUID();
const user = randomUUID();
const moment = randomUUID();
const speaker = randomUUID();

function record(
  agent: string,
  kind: string,
  payload: unknown,
  extra: Record<string, unknown> = {},
) {
  return {
    header: { org_id: org, idempotency_key: "k", event_id: randomUUID() },
    meeting_id: meeting,
    window_id: `${meeting}:61200`,
    user_id: user,
    agent,
    kind,
    relevance_score: 0.8,
    confidence: 0.86,
    content: "- You send the pricing deck by Friday.",
    payload_json: JSON.stringify(payload),
    source_chunk_ids: [],
    ...extra,
  };
}

const fullMoment = {
  moment_id: moment,
  why: "  You own pricing, and this changes the launch date.  ",
  quote: "Let's ship the beta on the second, and I'll send the deck Friday.",
  speaker_id: speaker,
  speaker_label: "Maya Chen",
  start_ms: 61_200,
  end_ms: 66_900,
};

describe("momentFields", () => {
  it("keeps only well-formed fields, trimmed and cut, never null", () => {
    expect(momentFields(fullMoment, true)).toEqual({
      moment_id: moment,
      why: "You own pricing, and this changes the launch date.",
      quote: fullMoment.quote,
      speaker_id: speaker,
      speaker_label: "Maya Chen",
      start_ms: 61_200,
      end_ms: 66_900,
    });
    expect(
      momentFields(
        {
          moment_id: "nope",
          why: "   ",
          quote: 42,
          speaker_id: "user_2x",
          speaker_label: null,
          start_ms: -5,
          end_ms: "10",
          updates_candidate_id: "x",
        },
        true,
      ),
    ).toEqual({});
    expect(momentFields({ why: "w".repeat(500), quote: "q".repeat(900) }, false)).toEqual({
      why: "w".repeat(200),
      quote: "q".repeat(300),
    });
    // an end before the start is dropped; fractional ms are floored
    expect(momentFields({ start_ms: 9000.7, end_ms: 100 }, false)).toEqual({ start_ms: 9000 });
  });

  it("names the updated candidate only for candidates", () => {
    const id = randomUUID();
    expect(momentFields({ updates_candidate_id: id }, true)).toEqual({ updates_candidate_id: id });
    expect(momentFields({ updates_candidate_id: id }, false)).toEqual({});
  });
});

describe("routeAgentRecord with moments", () => {
  it("forwards a decision candidate's moment and the candidate it updates", () => {
    const id = randomUUID();
    const routed = routeAgentRecord(
      "agent-results",
      record("DecisionAgent", "decision_candidate", {
        candidates: [
          {
            id,
            content: "Ship the beta on 2 December",
            confidence: 0.86,
            surfaced: true,
            rationale: null,
            updates_candidate_id: id,
            ...fullMoment,
          },
        ],
      }),
    );
    expect(routed?.message).toMatchObject({
      type: "agent-candidate",
      payload: {
        candidates: [
          {
            id,
            moment_id: moment,
            updates_candidate_id: id,
            quote: fullMoment.quote,
            speaker_id: speaker,
            speaker_label: "Maya Chen",
            start_ms: 61_200,
            end_ms: 66_900,
          },
        ],
      },
    });
  });

  it("forwards an action candidate without moment fields exactly as before", () => {
    const id = randomUUID();
    const routed = routeAgentRecord(
      "agent-results",
      record("ActionAgent", "action_candidate", {
        candidates: [{ id, content: "Raj drafts notes", confidence: 0.9, surfaced: true }],
      }),
    );
    expect(routed?.message).toEqual({
      type: "agent-candidate",
      payload: {
        windowId: `${meeting}:61200`,
        agent: "ActionAgent",
        kind: "action_candidate",
        candidates: [
          {
            id,
            content: "Raj drafts notes",
            confidence: 0.9,
            surfaced: true,
            assignee_id: null,
            due_date: null,
            depends_on: [],
          },
        ],
      },
    });
  });

  it("forwards a suggestion's moment and why, never an updates id", () => {
    const id = randomUUID();
    const routed = routeAgentRecord(
      "agent-results",
      record("QuestionAgent", "suggestion", {
        suggestions: [
          {
            id,
            question: "Who signs off on the date?",
            priority: 2,
            confidence: 0.8,
            surfaced: true,
            updates_candidate_id: randomUUID(),
            ...fullMoment,
          },
        ],
      }),
    );
    expect(routed?.message.type).toBe("agent-suggestion");
    const item = (routed?.message.payload as unknown as { suggestions: Record<string, unknown>[] })
      .suggestions[0];
    expect(item).toMatchObject({ moment_id: moment, why: fullMoment.why.trim(), start_ms: 61_200 });
    expect(item).not.toHaveProperty("updates_candidate_id");
  });

  it("forwards a note's moment camelCase, with its well-formed commitments", () => {
    const routed = routeAgentRecord(
      "agent-results",
      record("PersonalListenerAgent", "note", {
        surfaced: true,
        ...fullMoment,
        commitments: [
          { content: "Send the pricing deck", due_hint: "Friday", moment_id: moment },
          { content: "  " },
          { content: "Book the room", due_hint: null, moment_id: "bad" },
          "nope",
        ],
      }),
    );
    expect(routed?.message).toMatchObject({
      type: "agent-note",
      userId: user,
      payload: {
        windowId: `${meeting}:61200`,
        momentId: moment,
        quote: fullMoment.quote,
        speakerId: speaker,
        speakerLabel: "Maya Chen",
        startMs: 61_200,
        endMs: 66_900,
        commitments: [
          { content: "Send the pricing deck", dueHint: "Friday", momentId: moment },
          { content: "Book the room", dueHint: null },
        ],
      },
    });
  });

  it("leaves a note from before moments unchanged", () => {
    const routed = routeAgentRecord(
      "agent-results",
      record("PersonalListenerAgent", "note", { surfaced: true, commitments: [] }),
    );
    expect(routed?.message).toEqual({
      type: "agent-note",
      userId: user,
      payload: {
        windowId: `${meeting}:61200`,
        agent: "PersonalListenerAgent",
        content: "- You send the pricing deck by Friday.",
        confidence: 0.86,
        relevanceScore: 0.8,
        sourceChunkIds: [],
      },
    });
  });
});
