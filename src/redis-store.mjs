import { setTimeout as sleep } from 'node:timers/promises';
import { CardError, assert } from './errors.mjs';
import { emptyState, identifier, checkState } from './model.mjs';
import { decode, encode } from './store-state.mjs';
import { FORMAT, READ_SCRIPT, GLOBAL_CAS_SCRIPT, CARD_CAS_SCRIPT, MIGRATE_SCRIPT } from './redis-scripts.mjs';

const corrupt = () => new CardError('STORE_CORRUPT', 'Stored Redis layout is invalid; preserve it for recovery.', 503);
function parse(raw) {
  try { return JSON.parse(raw); } catch { throw corrupt(); }
}
function fields(state) {
  const { version, cards, loaderAssets, ...meta } = state;
  const result = { meta: JSON.stringify(meta) };
  for (const [id, record] of Object.entries(cards)) result[`card:${id}`] = JSON.stringify(record);
  for (const [id, asset] of Object.entries(loaderAssets ?? {})) result[`asset:${id}`] = JSON.stringify(asset);
  // Preserve whether optional loaderAssets existed when reconstructing state.
  result.assetsPresent = loaderAssets === undefined ? '0' : '1';
  return result;
}
function fromFields(entries) {
  assert(Array.isArray(entries) && entries.length % 2 === 0, 'STORE_CORRUPT', 'Invalid Redis fields.', 503);
  const data = Object.create(null);
  for (let i = 0; i < entries.length; i += 2) data[entries[i]] = entries[i + 1];
  if (!data.meta || !/^\d+$/.test(data.version ?? '') || !/^\d+$/.test(data.bytes ?? '') ||
    !['0', '1'].includes(data.assetsPresent)) throw corrupt();
  const state = { ...parse(data.meta), version: Number(data.version), cards: {} };
  if (data.assetsPresent === '1') state.loaderAssets = {};
  for (const [name, value] of Object.entries(data)) {
    if (name.startsWith('card:')) state.cards[name.slice(5)] = parse(value);
    if (name.startsWith('asset:')) (state.loaderAssets ??= {})[name.slice(6)] = parse(value);
  }
  return checkState(state);
}
function cardState(id, raw, asset) {
  const state = emptyState();
  if (!raw) return state;
  const record = parse(raw);
  if (!record || record.id !== id) throw corrupt();
  state.cards[id] = record;
  if (!record.archivedAt) state.slots[record.slot] = id;
  if (record.loaderAssetId) {
    if (!asset) throw corrupt();
    state.loaderAssets = { [record.loaderAssetId]: parse(asset) };
  }
  return checkState(state);
}
function checked(result) {
  if (result === -2) throw new CardError('STORE_MIGRATION_REQUIRED', 'Back up and migrate the legacy Redis registry before writing.', 503);
  if (result === -3) throw new CardError('STORE_FULL', 'Registry exceeds its V1 bound; archive policy needs review.', 503);
  if (result !== 0 && result !== 1) throw corrupt();
  return result === 1;
}

/** Per-card Redis hash storage, with explicit legacy-string migration. */
export class RedisStore {
  constructor({ url, token, key, fetchImpl = fetch, maxRetries = 12 }) {
    this.url = url; this.token = token; this.key = key; this.fetch = fetchImpl; this.maxRetries = maxRetries;
  }
  async command(args) {
    let r;
    try {
      r = await this.fetch(this.url, { method: 'POST', headers: {
        Authorization: `Bearer ${this.token}`, 'Content-Type': 'application/json',
      }, body: JSON.stringify(args), signal: AbortSignal.timeout(10_000), redirect: 'error' });
    } catch {
      throw new CardError('STORE_UNAVAILABLE', 'Storage response unavailable; reconcile before issuing different work.', 503);
    }
    assert(r.ok, 'STORE_UNAVAILABLE', 'Storage rejected the request.', 503);
    let body;
    try { body = await r.json(); } catch { throw new CardError('STORE_UNAVAILABLE', 'Invalid storage response.', 503); }
    assert(body && !body.error && Object.hasOwn(body, 'result'), 'STORE_UNAVAILABLE', 'Storage command failed.', 503);
    return body.result;
  }
  eval(script, ...args) { return this.command(['EVAL', script, '1', this.key, FORMAT, ...args]); }
  async snapshot(id) {
    const result = await this.eval(READ_SCRIPT, id === undefined ? 'all' : 'card', id ?? '');
    if (!Array.isArray(result)) throw corrupt();
    const [layout, raw, asset] = result;
    if (layout === 'empty') return { layout, state: emptyState() };
    if (layout === 'legacy') return { layout, raw, state: decode(raw) };
    if (layout === 'hash') return { layout, state: fromFields(raw) };
    if (layout === 'card') return { layout, raw, state: cardState(id, raw, asset) };
    throw corrupt();
  }
  async read() { return (await this.snapshot()).state; }
  async readCard(id) { identifier(id); return (await this.snapshot(id)).state; }
  async retry(operation) {
    for (let attempt = 0; attempt < this.maxRetries; attempt++) {
      const { committed, result } = await operation();
      if (committed) return result;
      if (attempt + 1 < this.maxRetries) await sleep(Math.min(200, 10 * 2 ** attempt) + Math.random() * 15);
    }
    throw new CardError('STORE_BUSY', 'Concurrent writes exhausted the retry budget. Read current state before retrying.', 503);
  }
  async transaction(change) {
    return this.retry(async () => {
      const snapshot = await this.snapshot();
      if (snapshot.layout === 'legacy') checked(-2);
      const state = snapshot.state, before = fields(state), version = state.version;
      const result = change(state);
      assert(!(result instanceof Promise), 'INVALID_TRANSACTION', 'Transactions must be synchronous and side-effect free.', 500);
      state.version++;
      const bytes = Buffer.byteLength(encode(state)), after = fields(state);
      const puts = Object.fromEntries(Object.entries(after).filter(([name, value]) => snapshot.layout === 'empty' || before[name] !== value));
      const deletes = Object.keys(before).filter(name => !Object.hasOwn(after, name));
      const committed = checked(await this.eval(GLOBAL_CAS_SCRIPT, String(version), String(state.version), String(bytes), JSON.stringify(puts), JSON.stringify(deletes)));
      return { committed, result };
    });
  }
  async transactionCard(id, change) {
    identifier(id);
    return this.retry(async () => {
      const snapshot = await this.snapshot(id);
      if (snapshot.layout === 'legacy') checked(-2);
      const before = snapshot.state.cards[id];
      // Missing records are handled by the service's normal CARD_NOT_FOUND check.
      const original = before ? structuredClone(before) : null;
      const result = change(snapshot.state);
      assert(!(result instanceof Promise), 'INVALID_TRANSACTION', 'Transactions must be synchronous and side-effect free.', 500);
      const record = snapshot.state.cards[id];
      assert(original && record && record.id === id && record.slot === original.slot && record.archivedAt === original.archivedAt &&
        record.ownerRef === original.ownerRef && record.loaderAssetId === original.loaderAssetId && Object.keys(snapshot.state.cards).length === 1,
      'INVALID_TRANSACTION', 'Card transactions cannot change membership, ownership or loaders.', 500);
      checkState(snapshot.state);
      const next = JSON.stringify(record);
      if (next === snapshot.raw) return { committed: true, result };
      const committed = checked(await this.eval(CARD_CAS_SCRIPT, id, snapshot.raw, next));
      return { committed, result };
    });
  }
  async migrate(backup) {
    const snapshot = await this.snapshot();
    if (snapshot.layout !== 'legacy') return { migrated: false, layout: snapshot.layout };
    assert(typeof backup === 'function', 'BACKUP_REQUIRED', 'Save the exact legacy registry before migration.', 400);
    const raw = encode(snapshot.state);
    await backup(snapshot.raw);
    const ok = await this.eval(MIGRATE_SCRIPT, snapshot.raw, JSON.stringify(fields(snapshot.state)), String(snapshot.state.version), String(Buffer.byteLength(raw)));
    assert(ok === 1, 'MIGRATION_CONFLICT', 'Registry changed after backup. Preserve the backup and retry with writers stopped.', 409);
    return { migrated: true, layout: 'hash', cards: Object.keys(snapshot.state.cards).length };
  }
}
