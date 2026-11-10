/**
 * Prometheus metrics for the realtime gateway. Names are documented in
 * mytheus-backend/design/metrics.md. No org_id / user id / meeting id is ever a label value.
 *
 * `/metrics` is for in-network scraping only: a request carrying an ingress header
 * (x-forwarded-*, cf-connecting-ip, forwarded) is answered 404, and the api-gateway has no
 * route for it.
 */
import type { IncomingMessage, ServerResponse } from "node:http";

import { collectDefaultMetrics, Counter, Gauge, Registry } from "prom-client";

export const registry = new Registry();
registry.setDefaultLabels({ service: "realtime-gateway" });
collectDefaultMetrics({ register: registry });

let clientCountSource: () => number = () => 0;

export const connectedSockets = new Gauge({
  name: "mytheus_realtime_connected_sockets",
  help: "Currently connected Socket.io clients on this pod.",
  registers: [registry],
  collect() {
    this.set(clientCountSource());
  },
});

/** Backpressure shed order: an "agent-delta" dropped because its socket was backed up. */
export const deltasShed = new Counter({
  name: "mytheus_realtime_deltas_shed_total",
  help: "agent-delta emits dropped because the socket's outbound buffer was over threshold.",
  registers: [registry],
});

/** ProactiveAgent alerts emitted to a recipient socket (one per socket, so tabs count twice). */
export const alertsDelivered = new Counter({
  name: "mytheus_realtime_agent_alerts_delivered_total",
  help: "agent-alert emits to recipient sockets on this pod.",
  registers: [registry],
});

export function setClientCountSource(fn: () => number): void {
  clientCountSource = fn;
}

const INGRESS_HEADERS = ["x-forwarded-for", "x-forwarded-host", "cf-connecting-ip", "forwarded"];

/** Returns true if it handled the request. Register in the HTTP server's request listener. */
export function handleMetricsRequest(req: IncomingMessage, res: ServerResponse): boolean {
  if (req.method !== "GET" || (req.url ?? "").split("?")[0] !== "/metrics") return false;
  if (INGRESS_HEADERS.some((h) => req.headers[h] !== undefined)) {
    res.writeHead(404);
    res.end();
    return true;
  }
  void registry.metrics().then((body) => {
    res.writeHead(200, { "content-type": registry.contentType });
    res.end(body);
  });
  return true;
}
