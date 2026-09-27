import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { createConnection } from 'node:net';
import { once } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import { FORMAT, READ_SCRIPT, GLOBAL_CAS_SCRIPT, CARD_CAS_SCRIPT, MIGRATE_SCRIPT } from '../src/redis-scripts.mjs';
import { RedisStore } from '../src/store.mjs';
import { CardService } from '../src/service.mjs';
import { config } from './helpers.mjs';

// Default tests exercise the REST boundary with a double. Set REDIS_SERVER_BIN
// to run the same behavioral tests against actual Redis and actual Lua scripts.
function memoryBackend() {
  let value = null;
  return async args => {
    const [command, ...rest] = args;
    if (command === 'SET') { value = rest[1]; return 'OK'; }
    if (command === 'GET') { assert.ok(value === null || typeof value === 'string'); return value; }
    if (command === 'TYPE') return value === null ? 'none' : typeof value === 'string' ? 'string' : 'hash';
    if (command === 'HGETALL') return value ? Object.entries(value).flat() : [];
    if (command === 'HGET') return value?.[rest[1]] ?? null;
    if (command === 'HSET') { value[rest[1]] = rest[2]; return 1; }
    assert.equal(command, 'EVAL');
    const [script, count, key, format, ...a] = rest;
    assert.equal(count, '1'); assert.equal(format, FORMAT);
    if (script === READ_SCRIPT) {
      if (value === null) return ['empty'];
      if (typeof value === 'string') return ['legacy', value];
      if (value.format !== format) return ['corrupt'];
      if (a[0] === 'all') return ['hash', Object.entries(value).flat()];
      const raw = value[`card:${a[1]}`] ?? null;
      const record = raw ? JSON.parse(raw) : null;
      return ['card', raw, record?.loaderAssetId ? value[`asset:${record.loaderAssetId}`] ?? null : null];
    }
    if (script === MIGRATE_SCRIPT) {
      if (typeof value !== 'string' || value !== a[0]) return 0;
      value = { format, version: a[2], bytes: a[3], ...JSON.parse(a[1]) }; return 1;
    }
    if (typeof value === 'string') return -2;
    if (value && value.format !== format) return -1;
    if (script === GLOBAL_CAS_SCRIPT) {
      if ((value?.version ?? '0') !== a[0]) return 0;
      value = { ...value, format, version: a[1], bytes: a[2], ...JSON.parse(a[3]) };
      for (const field of JSON.parse(a[4])) delete value[field];
      return 1;
    }
    assert.equal(script, CARD_CAS_SCRIPT);
    if (!value) return -1;
    const [id, old, next] = a, field = `card:${id}`;
    if (!value[field] || value[field] !== old) return 0;
    const version = String(Number(value.version) + 1);
    const bytes = Number(value.bytes) + Buffer.byteLength(next) - Buffer.byteLength(old) + version.length - value.version.length;
    if (bytes > 3000000) return -3;
    value[field] = next; value.version = version; value.bytes = String(bytes); return 1;
  };
}

// Minimal RESP2 transport confined to a task-owned local Unix socket.
function redisCommand(path, args) {
  return new Promise((resolve, reject) => {
    const socket = createConnection(path); let data = Buffer.alloc(0);
    socket.setTimeout(5000, () => socket.destroy(new Error('Local Redis timed out')));
    socket.on('error', reject);
    socket.on('connect', () => {
      const chunks = [Buffer.from(`*${args.length}\r\n`)];
      for (const arg of args) { const bytes = Buffer.from(String(arg)); chunks.push(Buffer.from(`$${bytes.length}\r\n`), bytes, Buffer.from('\r\n')); }
      socket.write(Buffer.concat(chunks));
    });
    function parse(offset = 0) {
      const end = data.indexOf('\r\n', offset); if (end < 0) return null;
      const type = String.fromCharCode(data[offset]), text = data.toString('utf8', offset + 1, end);
      let next = end + 2;
      if (type === '-') throw new Error(text);
      if (type === '+') return { value: text, next };
      if (type === ':') return { value: Number(text), next };
      if (type === '$') {
        const length = Number(text); if (length === -1) return { value: null, next };
        if (data.length < next + length + 2) return null;
        return { value: data.toString('utf8', next, next + length), next: next + length + 2 };
      }
      if (type === '*') {
        const count = Number(text); if (count === -1) return { value: null, next };
        const value = [];
        for (let i = 0; i < count; i++) { const item = parse(next); if (!item) return null; value.push(item.value); next = item.next; }
        return { value, next };
      }
      throw new Error('Unsupported local Redis response');
    }
    socket.on('data', chunk => {
      data = Buffer.concat([data, chunk]);
      try { const result = parse(); if (result) { resolve(result.value); socket.end(); } }
      catch (error) { socket.destroy(); reject(error); }
    });
  });
}
async function realBackend(t) {
  const dir = await mkdtemp('/tmp/lc-redis-'), socket = `${dir}/redis.sock`;
  const child = spawn(process.env.REDIS_SERVER_BIN, ['--port', '0', '--unixsocket', socket, '--unixsocketperm', '700', '--save', '', '--appendonly', 'no', '--dir', dir], { stdio: ['ignore', 'ignore', 'pipe'] });
  let error, log = ''; child.on('error', e => error = e); child.stderr.on('data', b => log += b);
  t.after(async () => {
    if (child.exitCode === null && child.pid) { const exited = once(child, 'exit'); child.kill('SIGTERM'); await exited; }
    await rm(dir, { recursive: true, force: true });
  });
  for (let i = 0; i < 100; i++) {
    if (error) throw error;
    if (child.exitCode !== null) throw new Error(`Redis exited: ${log}`);
    try { await redisCommand(socket, ['PING']); return args => redisCommand(socket, args); } catch { await sleep(10); }
  }
  throw new Error('Local Redis did not start');
}
export async function redisFixture(t, options = {}) {
  const exec = process.env.REDIS_SERVER_BIN ? await realBackend(t) : memoryBackend();
  const calls = [];
  const api = { exec, calls, before: null, after: null,
    async fetch(url, opts) {
      assert.equal(opts.redirect, 'error'); assert.match(opts.headers.Authorization, /^Bearer /);
      const args = JSON.parse(opts.body); await api.before?.(args);
      const result = await exec(args); calls.push({ args, result }); await api.after?.(args, result);
      return new Response(JSON.stringify({ result }));
    },
  };
  const store = new RedisStore({ url: 'https://redis.example.test', token: 'secret', key: 'test', fetchImpl: api.fetch, ...options });
  const service = new CardService(store, config());
  const fields = async () => {
    const entries = await exec(['HGETALL', 'test']); const result = {};
    for (let i = 0; i < entries.length; i += 2) result[entries[i]] = entries[i + 1];
    return result;
  };
  return { api, store, service, fields };
}
