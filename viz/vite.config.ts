/// <reference types="vitest/config" />
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import path from "node:path";

// App root is viz/ itself. publicDir points at the committed sample traces so
// they are fetchable at /raft-partition.jsonl etc. from the dev server and the
// built bundle.
export default defineConfig({
  plugins: [react()],
  publicDir: "sample-traces",
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
});
