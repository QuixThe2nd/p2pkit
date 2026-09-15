// Build for the bootstrapping server.
//
// 1. Syncs the shared lobby wire types: ../src/signalling/lobby.ts (the single
//    source of truth, shared with the client library) is copied to src/lobby.ts
//    so the emitted declarations only reference files inside this package.
// 2. Bundles src/server.ts -> dist/server.js with esbuild (node platform, ESM,
//    packages external — `ws` resolves from node_modules at runtime).
// 3. Emits dist/*.d.ts declarations with tsc.
//
// `node build.mjs --sync-types` performs only step 1; `--typecheck` performs
// steps 1 + `tsc --noEmit`. Both tsc invocations resolve the compiler through
// Node's module resolution (works with this package's node_modules or a
// parent's, e.g. when developed inside the p2pkit checkout).

import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { cpSync, mkdirSync, rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';

const require = createRequire(import.meta.url);
const HERE = dirname(fileURLToPath(import.meta.url));
const SHARED_LOBBY = join(HERE, '..', 'src', 'signalling', 'lobby.ts');
const LOCAL_LOBBY = join(HERE, 'src', 'lobby.ts');
const DIST = join(HERE, 'dist');

cpSync(SHARED_LOBBY, LOCAL_LOBBY); // idempotent overwrite of the generated sibling

const runTsc = (...flags) => {
  const tsc = spawnSync(
    process.execPath,
    [require.resolve('typescript/bin/tsc'), '--project', join(HERE, 'tsconfig.json'), ...flags],
    { stdio: 'inherit' },
  );
  if (tsc.status !== 0) process.exit(tsc.status ?? 1);
};

const mode = process.argv[2] ?? '';
if (mode === '--typecheck') {
  runTsc('--noEmit');
} else if (mode !== '--sync-types') {
  rmSync(DIST, { recursive: true, force: true });
  mkdirSync(DIST, { recursive: true });

  await build({
    entryPoints: [join(HERE, 'src', 'server.ts')],
    outfile: join(DIST, 'server.js'),
    bundle: true,
    platform: 'node',
    format: 'esm',
    target: 'node20',
    packages: 'external',
  });

  runTsc('--declaration', '--emitDeclarationOnly', '--outDir', DIST);
}
