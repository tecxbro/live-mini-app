import test from 'node:test';
import assert from 'node:assert/strict';
import { RedisStore } from '../src/store.mjs';
import { CardService } from '../src/service.mjs';
import { READ_SCRIPT, GLOBAL_CAS_SCRIPT, CARD_CAS_SCRIPT } from '../src/redis-scripts.mjs';
import { config, payload, fixture, accept, finished } from './helpers.mjs';
import { redisFixture } from './redis-fixture.mjs';

const update = (record, requestId = 'milestone') => ({ requestId, expectedRevision: record.revision, content: { ...record.content, detail: { title: 'Updated', subtitle: 'Confirmed work' } } });
const asset = { name: 'Portrait', kind: 'static', columns: 8, rows: 8, cells: '0'.repeat(64) };
const setLoader = ownerRef => ({ ownerRef, requestId: 'personalize', action: 'replace', expectedRevision: 0, asset });
async function createPair(service) {
  const b = await payload();
  return [await service.create(b), await service.create({ ...b, requestId: 'second', taskId: 'second', ownerRef: 'other' })];
}
const view = (s, r) => s.view(r.slot, r.id, s.key(r));

test('card refresh reads only the selected record and its loader, with no storage write', async t => {
  const { api, store, service, fields } = await redisFixture(t);
  const [a, b] = await createPair(service); await service.setLoader(setLoader('other'));
  const before = await fields(); api.calls.length = 0;
  const shown = await view(service, a);
  assert.equal(shown.id, a.id); assert.equal(api.calls.length, 1);
  assert.equal(api.calls[0].args[1], READ_SCRIPT); assert.equal(api.calls[0].args[5], 'card');
  assert.equal(api.calls[0].result[0], 'card');
  assert.ok(!JSON.stringify(api.calls[0].result).includes(b.id));
  assert.ok(!JSON.stringify(api.calls[0].result).includes('Portrait'));
  assert.deepEqual(await fields(), before);
  const customized = await view(service, b); assert.equal(customized.loader.name, 'Portrait');
  const raw = await store.readCard(a.id); assert.deepEqual(Object.keys(raw.cards), [a.id]);
});
test('a milestone changes only that card plus shared version and size counters', async t => {
  const { api, service, fields, store } = await redisFixture(t);
  const [a, b] = await createPair(service), before = await fields(); api.calls.length = 0;
  const next = await service.update(a.id, update(a));
  assert.equal(next.revision, 2); assert.equal(next.viewUrl, a.viewUrl);
  assert.deepEqual(api.calls.map(c => c.args[1]), [READ_SCRIPT, CARD_CAS_SCRIPT]);
  assert.ok(api.calls.every(c => !JSON.stringify(c).includes(b.id)));
  const after = await fields();
  assert.deepEqual(Object.keys(after).filter(k => before[k] !== after[k]).sort(), ['bytes', `card:${a.id}`, 'version'].sort());
  assert.equal(Number(after.bytes), Buffer.byteLength(JSON.stringify(await store.read())));
  const replay = await service.update(a.id, update(a)); assert.equal(replay.revision, next.revision);
});
test('card read size stays constant when unrelated retained history grows', async t => {
  const { api, store, service } = await redisFixture(t);
  const a = await service.create(await payload());
  await view(service, a);
  const initial = Buffer.byteLength(JSON.stringify(api.calls.at(-1).result));
  await store.transaction(state => {
    for (let i = 0; i < 90; i++) {
      const id = `archived-${i}`;
      state.cards[id] = { ...structuredClone(state.cards[a.id]), id, archivedAt: new Date().toISOString() };
    }
  });
  await view(service, a);
  assert.equal(Buffer.byteLength(JSON.stringify(api.calls.at(-1).result)), initial);
  assert.equal(Object.keys((await store.read()).cards).length, 91);
});
test('different cards update concurrently without retrying shared registry versions', async t => {
  const { service, api } = await redisFixture(t); const [a, b] = await createPair(service);
  api.calls.length = 0;
  const next = await Promise.all([service.update(a.id, update(a)), service.update(b.id, update(b))]);
  assert.ok(next.every(r => r.revision === 2));
  assert.equal(api.calls.filter(c => c.args[1] === CARD_CAS_SCRIPT).length, 2);
  assert.ok(api.calls.filter(c => c.args[1] === CARD_CAS_SCRIPT).every(c => c.result === 1));
});
test('same-card competing milestones reject a stale revision and retain one winner', async t => {
  const { service } = await redisFixture(t); const a = await service.create(await payload());
  const results = await Promise.allSettled([service.update(a.id, update(a, 'one')), service.update(a.id, update(a, 'two'))]);
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
  assert.equal(results.find(r => r.status === 'rejected').reason.code, 'REVISION_CONFLICT');
  assert.equal((await service.get(a.id)).revision, 2);
});
test('global transaction retries after a card write instead of losing the milestone', async t => {
  const { service, api, fields, store } = await redisFixture(t); const [a, b] = await createPair(service);
  let injected = false;
  api.before = async args => {
    if (args[1] === GLOBAL_CAS_SCRIPT && !injected) { injected = true; await service.update(a.id, update(a)); }
  };
  await service.setLoader(setLoader('other'));
  assert.equal((await service.get(a.id)).revision, 2);
  assert.equal((await view(service, b)).loader.name, 'Portrait');
  assert.ok(api.calls.some(c => c.args[1] === GLOBAL_CAS_SCRIPT && c.result === 0));
  assert.equal(Number((await fields()).bytes), Buffer.byteLength(JSON.stringify(await store.read())));
});
test('personalization racing a milestone cannot be overwritten by the card transaction', async t => {
  const { service, api } = await redisFixture(t);
  const r = await service.create({ ...await payload(), ownerRef: 'owner' });
  let injected = false;
  api.before = async args => {
    if (args[1] === CARD_CAS_SCRIPT && !injected) { injected = true; await service.setLoader(setLoader('owner')); }
  };
  await assert.rejects(service.update(r.id, update(r)), { code: 'REVISION_CONFLICT' });
  assert.equal((await view(service, r)).loader.name, 'Portrait');
});
test('concurrent creates allocate at most ten unique slots', async t => {
  const { service } = await redisFixture(t), b = await payload();
  const results = await Promise.allSettled(Array.from({ length: 12 }, (_, i) => service.create({ ...b, requestId: `r-${i}`, taskId: `t-${i}` })));
  const passed = results.filter(r => r.status === 'fulfilled');
  assert.equal(passed.length, 10); assert.equal(new Set(passed.map(r => r.value.slot)).size, 10);
  assert.equal(results.filter(r => r.reason?.code === 'NO_SLOT_AVAILABLE').length, 2);
});
test('unknown sends survive targeted milestones and still block release until reconciled', async t => {
  const { service, store } = await redisFixture(t); const r = await service.create(await payload());
  const claim = await service.beginPresentation(r.id, { revision: 1 });
  await service.settlePresentation(r.id, { attemptId: claim.attempt.id, outcome: 'unknown' });
  const done = await service.update(r.id, { requestId: 'done', expectedRevision: 1, content: finished(r.content) });
  const restarted = new CardService(store, config());
  await assert.rejects(restarted.release(r.id, done.revision), { code: 'PRESENTATION_PENDING' });
  await assert.rejects(restarted.beginPresentation(r.id, { revision: 2 }), { code: 'PRESENTATION_PENDING' });
  await restarted.settlePresentation(r.id, { attemptId: claim.attempt.id, outcome: 'accepted', messageRef: 'real-message' });
  await restarted.release(r.id, done.revision);
  const next = await restarted.create({ ...await payload(), requestId: 'new', taskId: 'new' });
  assert.equal(next.slot, r.slot); assert.notEqual(next.id, r.id);
  assert.equal((await view(restarted, r)).content.status, 'completed');
  assert.equal((await restarted.get(r.id)).viewUrl, r.viewUrl);
});
test('concurrent send claims produce only one initial-send permission', async t => {
  const { service } = await redisFixture(t); const r = await service.create(await payload());
  const results = await Promise.allSettled([service.beginPresentation(r.id, { revision: 1 }), service.beginPresentation(r.id, { revision: 1 })]);
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1);
  assert.equal(results.find(r => r.status === 'rejected').reason.code, 'PRESENTATION_PENDING');
});
test('lost write acknowledgment can be reconciled without advancing the revision again', async t => {
  const { service, api } = await redisFixture(t); const r = await service.create(await payload());
  let lost = false;
  api.after = (args, result) => { if (!lost && args[1] === CARD_CAS_SCRIPT && result === 1) { lost = true; throw new Error('lost acknowledgment'); } };
  await assert.rejects(service.update(r.id, update(r)), { code: 'STORE_UNAVAILABLE' });
  assert.equal((await service.update(r.id, update(r))).revision, 2);
});
test('legacy migration requires a backup and preserves URLs, history, loaders, and send ledger', async t => {
  const old = await fixture(t), p = await payload();
  const a = await old.service.create({ ...p, ownerRef: 'owner' });
  await old.service.setLoader(setLoader('owner'));
  const claim = await old.service.beginPresentation(a.id, { revision: 2 });
  await old.service.settlePresentation(a.id, { attemptId: claim.attempt.id, outcome: 'unknown' });
  const historical = await old.service.create({ ...p, requestId: 'history', taskId: 'history', ownerRef: 'owner' });
  await accept(old.service, historical);
  const done = await old.service.update(historical.id, { requestId: 'finish-history', expectedRevision: 1, content: finished(historical.content) });
  await old.service.release(historical.id, done.revision);
  const original = await old.store.readRaw();
  const { api, service, store } = await redisFixture(t); await api.exec(['SET', 'test', original]);
  assert.equal((await service.get(a.id)).viewUrl, a.viewUrl);
  await assert.rejects(service.update(a.id, update({ ...a, revision: 2 })), { code: 'STORE_MIGRATION_REQUIRED' });
  await assert.rejects(store.migrate(), { code: 'BACKUP_REQUIRED' });
  let backup;
  const result = await store.migrate(async raw => { backup = raw; });
  assert.equal(backup, original); assert.equal(result.migrated, true);
  assert.deepEqual(await store.read(), JSON.parse(original));
  assert.equal((await service.get(a.id)).viewUrl, a.viewUrl);
  assert.equal((await service.get(a.id)).activeAttempt.state, 'unknown');
  assert.equal((await view(service, a)).loader.name, 'Portrait');
  assert.equal((await view(service, historical)).content.status, 'completed');
  assert.equal((await service.get(historical.id)).viewUrl, historical.viewUrl);
  assert.equal((await store.migrate()).migrated, false);
});
test('a prune racing a retained-card write cannot resurrect the deleted record', async t => {
  const { api, service, store } = await redisFixture(t);
  const a = await service.create(await payload());
  let injected = false;
  api.before = async args => {
    if (args[1] === CARD_CAS_SCRIPT && !injected) {
      injected = true;
      await store.transaction(s => { delete s.cards[a.id]; s.slots[a.slot] = null; });
    }
  };
  await assert.rejects(service.update(a.id, update(a)), { code: 'CARD_NOT_FOUND' });
  assert.equal((await store.read()).cards[a.id], undefined);
});
test('migration refuses a failed backup or concurrent legacy write without changing it', async t => {
  const old = await fixture(t); await old.service.create(await payload()); const raw = await old.store.readRaw();
  const { api, store } = await redisFixture(t); await api.exec(['SET', 'test', raw]);
  await assert.rejects(store.migrate(async () => { throw new Error('disk full'); }), /disk full/);
  assert.equal(await api.exec(['GET', 'test']), raw);
  const changed = JSON.stringify({ ...JSON.parse(raw), version: 99 });
  await assert.rejects(store.migrate(async () => { await api.exec(['SET', 'test', changed]); }), { code: 'MIGRATION_CONFLICT' });
  assert.equal(await api.exec(['GET', 'test']), changed);
});
test('missing/corrupt records fail closed and unknown layouts never become an empty pool', async t => {
  const { api, store, service } = await redisFixture(t);
  await assert.rejects(service.get('missing'), { code: 'CARD_NOT_FOUND' });
  const r = await service.create(await payload());
  await api.exec(['HSET', 'test', `card:${r.id}`, JSON.stringify({ id: 'wrong', slot: r.slot })]);
  await assert.rejects(service.get(r.id), { code: 'STORE_CORRUPT' });
  await api.exec(['HSET', 'test', 'format', 'unknown-layout']);
  await assert.rejects(store.read(), { code: 'STORE_CORRUPT' });
});
test('registry size bound is enforced before a card write', async t => {
  const { api, service, fields } = await redisFixture(t); const r = await service.create(await payload());
  await api.exec(['HSET', 'test', 'bytes', '3000000']); const before = await fields();
  await assert.rejects(service.update(r.id, update(r)), { code: 'STORE_FULL' });
  assert.deepEqual(await fields(), before);
});
test('storage/network failures remain errors, not a fresh empty pool', async () => {
  for (const fetchImpl of [async () => { throw new Error('offline'); }, async () => new Response('{"error":"failure"}'), async () => new Response('not-json')]) {
    const store = new RedisStore({ url: 'https://redis.example.test', token: 'secret', key: 'test', fetchImpl });
    await assert.rejects(store.read(), { code: 'STORE_UNAVAILABLE' });
  }
});
test('conflicts exhaust a bounded retry budget', async t => {
  const { api, store } = await redisFixture(t, { maxRetries: 1 });
  const original = api.fetch;
  store.fetch = async (url, opts) => JSON.parse(opts.body)[1] === GLOBAL_CAS_SCRIPT ? new Response('{"result":0}') : original(url, opts);
  await assert.rejects(store.transaction(() => null), { code: 'STORE_BUSY' });
});
