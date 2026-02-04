import commonjs from '@rollup/plugin-commonjs';
import json from '@rollup/plugin-json';
import nodeResolve from '@rollup/plugin-node-resolve';
import replace from '@rollup/plugin-replace';
import typescript from '@rollup/plugin-typescript';
import { statSync } from 'fs';
import { cp, rm } from 'fs/promises';
import path from 'path';
import { exit } from 'process';
import { rollup } from 'rollup';
import ts from 'typescript';

const tsconfigPath = ts.findConfigFile('./', ts.sys.fileExists, 'tsconfig.json');
if (!tsconfigPath) {
  throw new Error('Could not find a valid tsconfig.json.');
}

// --- Clean dist directory ---

console.log('Cleaning dist directory...');
await rm('dist', { recursive: true, force: true });

const { config } = ts.readConfigFile(tsconfigPath, ts.sys.readFile);
const parsed = ts.parseJsonConfigFileContent(config, ts.sys, path.dirname(tsconfigPath));

// --- Emit type declarations ---

console.log('Emitting type declarations...');

const program = ts.createProgram({
  rootNames: parsed.fileNames,
  options: parsed.options,
});
const emitResult = program.emit();

// --- Log type diagnostics ---
const allDiagnostics = ts.getPreEmitDiagnostics(program).concat(emitResult.diagnostics);
allDiagnostics.forEach((d) => {
  console.error(ts.flattenDiagnosticMessageText(d.messageText, '\n'));
});

if (allDiagnostics.length > 0) {
  exit(1);
}

// --- Bundle with rollup ---

try {
  const sharedPlugins = [
    typescript({ tsconfig: './tsconfig.json', declaration: false }),
    nodeResolve(),
    commonjs(),
    json(),
  ];

  // js_cols expects a global js_cols variable to already exist for some reason
  const banner = 'var js_cols = {};\n';

  const manualChunks = (id: string) => {
    if (id.includes('node_modules/epsg-index')) {
      return 'epsg-index';
    }
  };

  // --- Bundling for Node.js (ESM & CommonJS) ---
  console.log('Bundling for Node.js...');

  const nodeBundle = await rollup({
    input: 'lib/entry-node.ts',
    plugins: [
      ...sharedPlugins,
      replace({
        preventAssignment: true,
        values: { 'process.env.TARGET': JSON.stringify('node') },
      }),
    ],
  });

  console.log('Writing bundle for Node.js (ESM)...');
  await nodeBundle.write({
    format: 'esm',
    entryFileNames(chunkInfo) {
      return chunkInfo.name === 'entry-node' ? 'dist/node/esm/index.js' : 'dist/node/esm/[name]-[hash].js';
    },
    chunkFileNames(chunkInfo) {
      return 'dist/node/esm/[name]-[hash].js';
    },
    dir: 'dist',
    sourcemap: 'inline',
    banner,
    manualChunks,
  });

  console.log('Writing bundle for Node.js (CommonJS)...');
  await nodeBundle.write({
    format: 'cjs',
    entryFileNames(chunkInfo) {
      return chunkInfo.name === 'entry-node' ? 'dist/node/cjs/index.js' : 'dist/node/cjs/[name]-[hash].js';
    },
    chunkFileNames(chunkInfo) {
      return 'dist/node/cjs/[name]-[hash].js';
    },
    dir: 'dist',
    sourcemap: 'inline',
    banner,
    manualChunks,
  });

  // --- Bundling for Browser (ESM & IIFE) ---
  console.log('Bundling for Browser...');

  const browserBundle = await rollup({
    input: 'lib/entry-browser.ts',
    // @ngageoint/geopackage references these, but we do not need
    // them because we do not read or write actual geopackage files
    external: ['fs', 'path', 'util'],
    plugins: [
      ...sharedPlugins,
      replace({
        preventAssignment: true,
        values: { 'process.env.TARGET': JSON.stringify('browser') },
      }),
    ],
  });

  console.log('Writing bundle for Browser (ESM)...');
  await browserBundle.write({
    format: 'esm',
    entryFileNames(chunkInfo) {
      return chunkInfo.name === 'entry-browser'
        ? 'dist/browser/esm/index.js'
        : 'dist/browser/esm/[name]-[hash].js';
    },
    chunkFileNames(chunkInfo) {
      return 'dist/browser/esm/[name]-[hash].js';
    },
    dir: 'dist',
    sourcemap: 'inline',
    banner,
    manualChunks,
  });

  console.log('Writing bundle for Browser (IIFE)...');
  await browserBundle.write({
    format: 'iife',
    name: 'kart',
    file: 'dist/dist/browser/iife/index.js',
    sourcemap: 'inline',
    inlineDynamicImports: true,
    banner,
  });
} catch (error) {
  console.error('Build failed:', error);
  exit(1);
}

// --- Copy of all typescript files (./lib/**/*{.d.ts|.ts}) to dist ---

console.log('Copying TypeScript files to dist...');

await cp('lib', 'dist', {
  recursive: true,
  filter: (src) => {
    const stat = statSync(src);
    if (stat.isDirectory()) {
      return true;
    }
    const { ext } = path.parse(src);
    return ext === '.ts' || ext === '.d.ts';
  },
});

console.log('Build complete.');
