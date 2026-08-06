import js from "@eslint/js";
import tseslint from "typescript-eslint";
import eslintConfigPrettier from "eslint-config-prettier";

export default tseslint.config(
  { ignores: ["dist/**"] },
  js.configs.recommended,
  {
    files: ["src/**/*.ts", "test/**/*.ts"],
    extends: [...tseslint.configs.recommendedTypeChecked],
    languageOptions: {
      parserOptions: {
        project: "./tsconfig.json",
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      // The codebase logs exclusively through `pino` (see `logger` everywhere in src/). The two
      // places that use `console.*` directly (the top-level start/failure messages in
      // `main()`, before there's a request/handshake to attach structured context to, and a perf
      // test's diagnostic printout) were already wrapped in `eslint-disable-next-line no-console`
      // comments anticipating this rule — enabling it here is what makes those comments meaningful
      // instead of dead, and stops a stray `console.log` from creeping into the logger-only src/.
      "no-console": "error",
      // `nsp.use(async (socket, next) => {...})` is Socket.io's documented async-middleware
      // pattern: the handler's own try/catch always resolves via `next()`/`next(err)`, so there's
      // no unhandled-rejection risk, but the Socket.io middleware type is declared to return
      // `void`, not `Promise<void>`. This is the same false-positive typescript-eslint's own docs
      // call out for Express-style middleware; disabling the argument-position check (not the
      // whole rule) keeps the rule live for every other misused-promise shape.
      "@typescript-eslint/no-misused-promises": [
        "error",
        { checksVoidReturn: { arguments: false } },
      ],
    },
  },
  {
    // Type-aware "unsafe" rules are tuned for production code handling untrusted input; in tests
    // they mostly fire on deliberately-loose JSON.parse'd fixtures and captured mock-call
    // arguments (e.g. asserting on a decoded DLQ envelope), where the looser typing is the point,
    // not a defect. `src/`'s own `JSON.parse` sites stay typed (see the caption-message handler in
    // server.ts) since that's real inbound data, not a test fixture.
    files: ["test/**/*.ts"],
    rules: {
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
      "@typescript-eslint/no-unsafe-argument": "off",
    },
  },
  {
    // Fakes here implement Promise-returning interfaces (`MinimalKafkaProducer`,
    // `MinimalSchemaRegistry`, `PresenceEventsProducer`) that real implementations fulfil with a
    // network round-trip; the fakes don't need an internal `await` but must stay `async` to keep
    // returning a real `Promise` and satisfy those interfaces. Manually wrapping every return in
    // `Promise.resolve(...)` instead would be pure boilerplate for the same runtime behavior.
    files: ["test/helpers/**/*.ts"],
    rules: {
      "@typescript-eslint/require-await": "off",
    },
  },
  eslintConfigPrettier,
);
