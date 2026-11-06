import { createServer } from "node:http";
import type { AddressInfo } from "node:net";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { handleMetricsRequest, setClientCountSource } from "../src/metrics";

describe("/metrics", () => {
  const server = createServer((req, res) => {
    if (handleMetricsRequest(req, res)) return;
    res.writeHead(404);
    res.end();
  });
  let base = "";
  beforeAll(async () => {
    setClientCountSource(() => 3);
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });
  afterAll(() => new Promise<void>((r) => server.close(() => r())));

  it("serves prometheus text with the socket gauge", async () => {
    const res = await fetch(`${base}/metrics`);
    expect(res.status).toBe(200);
    const body = await res.text();
    expect(body).toContain('mytheus_realtime_connected_sockets{service="realtime-gateway"} 3');
    expect(body).not.toMatch(/org_id|user_id/);
  });

  it("hides /metrics from requests that came through ingress", async () => {
    const res = await fetch(`${base}/metrics`, { headers: { "x-forwarded-for": "1.2.3.4" } });
    expect(res.status).toBe(404);
  });
});
