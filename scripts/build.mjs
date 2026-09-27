import { cp, mkdir, rm, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
const root = fileURLToPath(new URL('../', import.meta.url));
const output = resolve(root, '.vercel/output');
await rm(output, { recursive: true, force: true });
const fn = resolve(output, 'functions/index.func');
await mkdir(fn, { recursive: true });
for (const name of ['index.mjs', 'src', 'public', 'examples']) await cp(resolve(root, name), resolve(fn, name), { recursive: true });
const matrix = resolve(fn, 'grokbot-matrix');
await mkdir(matrix, { recursive: true });
for (const name of ['mini-card.css', 'square-animation.js', 'dot-matrix.js', 'mini-core.js', 'mini-card.js']) {
  await cp(resolve(root, 'grokbot-matrix', name), resolve(matrix, name));
}
await writeFile(resolve(fn, '.vc-config.json'), JSON.stringify({ runtime: 'nodejs22.x', handler: 'index.mjs', launcherType: 'Nodejs', shouldAddHelpers: false, maxDuration: 30 }, null, 2));
// All paths enter the same implemented router. No deployment per card or rewrite per ID.
await writeFile(resolve(output, 'config.json'), JSON.stringify({ version: 3, routes: [{ src: '/(.*)', dest: '/index?__path=/$1' }] }, null, 2));
console.log('Built .vercel/output (one Node function, no credentials or live sends).');
