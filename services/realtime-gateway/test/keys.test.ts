import { describe, expect, it } from "vitest";

import { presenceKey, pubsubChannel, roomName } from "../../../libs/keys";

describe("libs/keys canonical forms", () => {
  it("builds the verbatim pubsub fan-out channel", () => {
    expect(pubsubChannel("org-1", "meeting-1")).toBe("nexus:org-1:pubsub:meeting:meeting-1");
  });

  it("builds the verbatim presence set key", () => {
    expect(presenceKey("org-1", "meeting-1")).toBe("nexus:org-1:presence:meeting-1");
  });

  it("builds the Socket.io room name org-scoped, not meeting_id alone", () => {
    // Load-bearing tenancy boundary (fix round 2 — see roomName's doc comment): two DIFFERENT
    // orgs with the same literal meeting_id must land in DIFFERENT rooms, or presence/chat
    // fan-out (via `nsp.to(room)`, which crosses every gateway pod through the Redis adapter)
    // would deliver one org's presence/chat to the other's sockets.
    expect(roomName("org-1", "meeting-1")).toBe("meeting:org-1:meeting-1");
    expect(roomName("org-2", "meeting-1")).not.toBe(roomName("org-1", "meeting-1"));
    expect(roomName("org-1", "meeting-2")).not.toBe(roomName("org-1", "meeting-1"));
  });
});
