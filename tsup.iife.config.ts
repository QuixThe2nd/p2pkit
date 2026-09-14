// IIFE bundle config: produces dist/p2pkit.iife.js, a self-contained browser
// bundle exposing globalThis.P2PKIT_IIFE. Built by CI
// (.github/workflows/build-iife.yml) and committed back to master, or locally
// via `npm run build:iife`. Everything reachable from src/iife.ts must stay pure
// TypeScript with no Node-only imports.
import { defineConfig } from "tsup"

export default defineConfig({
  entry: { p2pkit: "src/iife.ts" },
  format: ["iife"],
  target: "es2022",
  globalName: "P2PKIT_IIFE",
  outDir: "dist",
  outExtension: () => ({ js: ".iife.js" }),
  sourcemap: false,
  clean: false,
  treeshake: true,
  // The browser graph must stay self-contained: everything reachable from
  // src/iife.ts is pure TypeScript with no Node-only imports.
  footer: { js: "globalThis.P2PKIT_IIFE = P2PKIT_IIFE;" },
})
