/**
 * Spins up one in-process `App` (real HTTP + Socket.io + real Redis) wired to a real fake-JWKS
 * server and a real fake-core-api server, with an in-memory `PresenceEventsProducer` double so
 * these tests don't need Kafka round-trips for every assertion. The real-broker assertion lives
 * in test/presence-events-kafka.test.ts.
 */
import type { AddressInfo } from "node:net";

import { io as ioClient, type Socket as ClientSocket } from "socket.io-client";

import { createClerkJwtVerifier } from "../../../../libs/auth";
import { createApp, type App } from "../../src/server";
import type {
  PresenceEventsProducer,
  ProducePresenceEventParams,
} from "../../src/presenceEventsProducer";
import { startFakeCoreApi, type FakeCoreApi } from "./fakeCoreApi";
import { startFakeJwksServer, type FakeJwksServer } from "./fakeJwks";

export interface FakePresenceEventsProducer extends PresenceEventsProducer {
  calls: ProducePresenceEventParams[];
}

export function createFakePresenceEventsProducer(): FakePresenceEventsProducer {
  const calls: ProducePresenceEventParams[] = [];
  return {
    calls,
    async connect() {},
    async disconnect() {},
    async produce(params) {
      calls.push(params);
    },
  };
}

export interface TestApp {
  app: App;
  baseUrl: string;
  jwks: FakeJwksServer;
  coreApi: FakeCoreApi;
  presenceEventsProducer: FakePresenceEventsProducer;
  stop(): Promise<void>;
}

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6390";

export async function startTestApp(): Promise<TestApp> {
  const jwks = await startFakeJwksServer();
  const coreApi = await startFakeCoreApi();
  const presenceEventsProducer = createFakePresenceEventsProducer();

  const jwtVerifier = createClerkJwtVerifier({ jwksUrl: jwks.jwksUrl });
  const app = createApp(
    {
      port: 0,
      redisUrl: REDIS_URL,
      coreApiUrl: coreApi.baseUrl,
      redpandaBrokers: ["localhost:9092"],
      schemaRegistryUrl: "http://localhost:8085",
    },
    { jwtVerifier, presenceEventsProducer },
  );

  await app.start();
  const { port } = app.httpServer.address() as AddressInfo;

  return {
    app,
    baseUrl: `http://127.0.0.1:${port}`,
    jwks,
    coreApi,
    presenceEventsProducer,
    async stop() {
      await app.stop();
      await jwks.close();
      await coreApi.close();
    },
  };
}

export function connectClient(baseUrl: string, token?: string): Promise<ClientSocket> {
  return new Promise((resolve, reject) => {
    const socket: ClientSocket = ioClient(`${baseUrl}/v1`, {
      path: "/socket.io",
      auth: token ? { token } : {},
      reconnection: false,
      forceNew: true,
      transports: ["websocket"],
    });
    socket.on("connect", () => resolve(socket));
    socket.on("connect_error", (err: Error) => reject(err));
  });
}

export function expectConnectRejected(baseUrl: string, token?: string): Promise<Error> {
  return new Promise((resolve, reject) => {
    const socket: ClientSocket = ioClient(`${baseUrl}/v1`, {
      path: "/socket.io",
      auth: token ? { token } : {},
      reconnection: false,
      forceNew: true,
      transports: ["websocket"],
    });
    socket.on("connect", () => {
      socket.close();
      reject(new Error("expected connection to be rejected, but it succeeded"));
    });
    socket.on("connect_error", (err: Error) => {
      socket.close();
      resolve(err);
    });
  });
}
