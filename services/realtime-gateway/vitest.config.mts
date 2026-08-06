import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Only source tests under test/**; `npm run build` no longer emits test files into dist/
    // (see tsconfig.build.json), but this is defense in depth against vitest's default include
    // glob also matching compiled .test.js output if that ever changes.
    include: ["test/**/*.test.ts"],
    exclude: ["dist/**", "node_modules/**"],
  },
});
