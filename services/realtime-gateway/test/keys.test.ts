import { describe, expect, it } from "vitest";

import { presenceKey, pubsubChannel, roomName } from "../../../libs/keys";

describe("libs/keys canonical forms", () => {
  it("builds the verbatim pubsub fan-out channel", () => {
    expect(pubsubChannel("org-1", "meeting-1")).toBe("nexus:org-1:pubsub:meeting:meeting-1");
  });

  it("builds the verbatim presence set key", () => {
    expect(presenceKey("org-1", "meeting-1")).toBe("nexus:org-1:presence:meeting-1");
  });

  it("scopes the Socket.io room name by both org_id and meeting_id", () => {
    expect(roomName("org-1", "meeting-1")).toBe("meeting:org-1:meeting-1");
    // different orgs with the same meeting_id must never collide on a room name
    expect(roomName("org-2", "meeting-1")).not.toBe(roomName("org-1", "meeting-1"));
  });
});
