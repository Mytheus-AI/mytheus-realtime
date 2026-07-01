/**
 * realtime-gateway: the Socket.io WebSocket fan-out plane (scaffold).
 *
 * Boots an HTTP server with `/healthz` and a Socket.io server wired to the Redis adapter for
 * cross-instance fan-out. A bus event lands on one pod; that pod publishes on the
 * `nexus:{org_id}:pubsub:meeting:{meeting_id}` Redis channel and every subscribed pod emits to
 * its local sockets in the meeting room. Redpanda consume of `agent-stream`/`agent-results`/
 * `notifications` and the `presence-events` producer land in the realtime band.
 *
 * Run: npm run build && npm start
 */

import { createServer } from "node:http";

import { createAdapter } from "@socket.io/redis-adapter";
import { Redis } from "ioredis";
import { Server } from "socket.io";

import { pubsubChannel } from "./keys";

const PORT = Number(process.env.PORT ?? 8090);
const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

const httpServer = createServer((req, res) => {
  if (req.url === "/healthz") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ status: "ok", service: "realtime-gateway" }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const io = new Server(httpServer, { path: "/socket.io" });

// Cross-instance fan-out via Redis. lazyConnect + error handlers keep the process (and /healthz)
// alive even when Redis is unreachable in dev; the adapter reconnects when Redis comes up.
const pubClient = new Redis(REDIS_URL, { lazyConnect: true });
const subClient = pubClient.duplicate();
pubClient.on("error", (err) => console.error("[redis:pub]", err.message));
subClient.on("error", (err) => console.error("[redis:sub]", err.message));
io.adapter(createAdapter(pubClient, subClient, { key: "nexus" }));

io.on("connection", (socket) => {
  // Room join, presence sets, and the three WS flows land in the realtime band.
  // Scaffold only accepts the connection so the transport contract exists.
  socket.on("disconnect", () => {
    /* presence cleanup lands in the realtime band */
  });
});

httpServer.listen(PORT, () => {
  const example = pubsubChannel("<org_id>", "<meeting_id>");
  console.log(`realtime-gateway listening on :${PORT} (fan-out channel form: ${example})`);
});
