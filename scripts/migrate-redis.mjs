import { open } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { loadConfig } from '../src/config.mjs';
import { makeStore } from '../src/store.mjs';
import { assert } from '../src/errors.mjs';

// Operator-only local tool. Never expose storage migration through a card URL.
try {
  const [backupPath, ...extra] = process.argv.slice(2);
  assert(backupPath && extra.length === 0, 'BACKUP_REQUIRED', 'Provide a new private backup file path; stop legacy writers first.');
  const config = loadConfig();
  assert(config.store === 'redis', 'CONFIG_ERROR', 'This migration only supports the configured legacy Redis registry, not Blob.');
  const result = await makeStore(config).migrate(async raw => {
    const path = resolve(backupPath);
    const file = await open(path, 'wx', 0o600);
    try { await file.writeFile(raw); await file.sync(); } finally { await file.close(); }
    const directory = await open(dirname(path), 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  });
  console.log(JSON.stringify(result));
} catch (error) {
  console.error(JSON.stringify({ error: { code: error.code || 'MIGRATION_FAILED', message: error.message } }));
  process.exitCode = 1;
}
