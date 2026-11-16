/**
 * Broker and Schema Registry credentials for the gateway's kafkajs clients (Redpanda SASL/SCRAM).
 *
 * Mirrors `kafka_auth.py` in the Python services: with no credential in the environment both
 * helpers return nothing extra, so local compose and tests keep using a PLAINTEXT broker.
 *
 *   KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD   this service's SCRAM principal
 *   KAFKA_SASL_MECHANISM                        scram-sha-256 (default) or scram-sha-512
 *   SCHEMA_REGISTRY_USERNAME / _PASSWORD        registry basic auth; default to the Kafka pair
 *
 * A username without a password (or the reverse) throws, so a half-set secret fails at startup
 * instead of silently connecting without authentication.
 */
import type { SASLOptions } from "kafkajs";

type Env = Record<string, string | undefined>;

function pair(env: Env, userVar: string, passVar: string): [string, string] | undefined {
  const user = (env[userVar] ?? "").trim();
  const password = env[passVar] ?? "";
  if (!user && !password) return undefined;
  if (!user || !password) throw new Error(`${userVar} and ${passVar} must be set together`);
  return [user, password];
}

/** The kafkajs `sasl` option, or undefined when no principal is configured. */
export function kafkaSasl(env: Env = process.env): SASLOptions | undefined {
  const creds = pair(env, "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD");
  if (!creds) return undefined;
  const mechanism = (env.KAFKA_SASL_MECHANISM ?? "scram-sha-256").toLowerCase();
  if (mechanism !== "scram-sha-256" && mechanism !== "scram-sha-512") {
    throw new Error(`unsupported KAFKA_SASL_MECHANISM ${mechanism}`);
  }
  return { mechanism, username: creds[0], password: creds[1] };
}

/** The Schema Registry `auth` option, or undefined when no principal is configured. */
export function registryAuth(
  env: Env = process.env,
): { username: string; password: string } | undefined {
  const creds =
    pair(env, "SCHEMA_REGISTRY_USERNAME", "SCHEMA_REGISTRY_PASSWORD") ??
    pair(env, "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD");
  return creds ? { username: creds[0], password: creds[1] } : undefined;
}
