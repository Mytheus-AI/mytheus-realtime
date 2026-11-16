import { describe, expect, it } from "vitest";
import { kafkaSasl, registryAuth } from "../src/kafkaAuth.js";

describe("kafkaAuth", () => {
  it("adds nothing when no principal is configured", () => {
    expect(kafkaSasl({})).toBeUndefined();
    expect(registryAuth({})).toBeUndefined();
  });

  it("builds SCRAM-SHA-256 SASL and registry basic auth from the Kafka principal", () => {
    const env = { KAFKA_SASL_USERNAME: "rt", KAFKA_SASL_PASSWORD: "pw" };
    expect(kafkaSasl(env)).toEqual({ mechanism: "scram-sha-256", username: "rt", password: "pw" });
    expect(registryAuth(env)).toEqual({ username: "rt", password: "pw" });
  });

  it("prefers a dedicated registry principal", () => {
    const env = {
      KAFKA_SASL_USERNAME: "rt",
      KAFKA_SASL_PASSWORD: "pw",
      SCHEMA_REGISTRY_USERNAME: "sr",
      SCHEMA_REGISTRY_PASSWORD: "srpw",
    };
    expect(registryAuth(env)).toEqual({ username: "sr", password: "srpw" });
  });

  it("fails closed on a half-set secret or an unknown mechanism", () => {
    expect(() => kafkaSasl({ KAFKA_SASL_USERNAME: "rt" })).toThrow();
    expect(() => kafkaSasl({ KAFKA_SASL_PASSWORD: "pw" })).toThrow();
    expect(() =>
      kafkaSasl({
        KAFKA_SASL_USERNAME: "rt",
        KAFKA_SASL_PASSWORD: "pw",
        KAFKA_SASL_MECHANISM: "plain",
      }),
    ).toThrow();
  });
});
