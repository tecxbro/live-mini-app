import { mkdir, readFile, rename, rm, open } from 'node:fs/promises';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { CardError, assert } from './errors.mjs';
import { decode, encode } from './store-state.mjs';
import { RedisStore } from './redis-store.mjs';
export { RedisStore } from './redis-store.mjs';

/** A local, cross-process lock + atomic rename. Never use this on Vercel. */
export class FileStore {
  constructor(path, { lockTimeoutMs = 5000 } = {}) { this.path = path; this.lock = `${path}.lock`; this.lockTimeoutMs = lockTimeoutMs; }
  async readRaw() {
    try { return await readFile(this.path, 'utf8'); }
    catch (e) { if (e.code === 'ENOENT') return null; throw e; }
  }
  async read() { return decode(await this.readRaw()); }
  async readCard() { return this.read(); }
  async transactionCard(id, change) { return this.transaction(change); }
  async transaction(change) {
    await mkdir(dirname(this.path), { recursive: true, mode: 0o700 });
    const deadline = Date.now() + this.lockTimeoutMs;
    for (;;) {
      try { await mkdir(this.lock, { mode: 0o700 }); break; }
      catch (e) {
        if (e.code !== 'EEXIST') throw e;
        if (Date.now() >= deadline) throw new CardError('STORE_BUSY', 'Registry is locked. A crashed local owner needs explicit recovery.', 503);
        await sleep(10 + Math.random() * 20);
      }
    }
    let tmp;
    try {
      const state = await this.read();
      const result = change(state);
      assert(!(result instanceof Promise), 'INVALID_TRANSACTION', 'Transactions must not perform asynchronous side effects.', 500);
      state.version++;
      const raw = encode(state);
      tmp = `${this.path}.${randomUUID()}.tmp`;
      const file = await open(tmp, 'wx', 0o600);
      try { await file.writeFile(raw); await file.sync(); } finally { await file.close(); }
      await rename(tmp, this.path);
      const directory = await open(dirname(this.path), 'r');
      try { await directory.sync(); } finally { await directory.close(); }
      return result;
    } finally {
      if (tmp) await rm(tmp, { force: true });
      await rm(this.lock, { recursive: true, force: true });
    }
  }
}

export function makeStore(config) {
  return config.store === 'redis'
    ? new RedisStore({ url: config.redisUrl, token: config.redisToken, key: config.redisKey })
    : new FileStore(config.dataFile);
}
