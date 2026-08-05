import { describe, expect, it } from "vitest";

import { presenceKey, pubsubChannel, roomName } from "../../../libs/keys";

describe("libs/keys canonical forms", () => {
  it("builds the verbatim pubsub fan-out channel", () => {
    expect(pubsubChannel("org-1", "meeting-1")).toBe("nexus:org-1:pubsub:meeting:meeting-1");
  });

  it("builds the verbatim presence set key", () => {
    expect(presenceKey("org-1", "meeting-1")).toBe("nexus:org-1:presence:meeting-1");
  });

  it("builds the Socket.io room name from meeting_id alone", () => {
    // matches the canon's literal "rooms keyed by meeting_id" — see roomName's doc comment for
    // why org_id was deliberately dropped (it forced every caller to agree on which org_id
    // REPRESENTATION to use, which the presence-reconciliation sweep structurally cannot).
    expect(roomName("meeting-1")).toBe("meeting:meeting-1");
    expect(roomName("meeting-2")).not.toBe(roomName("meeting-1"));
  });
});
