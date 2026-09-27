// All fields share one Redis key, so every script is atomic on one shard.
export const FORMAT = 'live-task-cards:hash:v2';
const kind = `local kind = redis.call('TYPE', KEYS[1]).ok
if kind ~= 'none' and kind ~= 'string' and kind ~= 'hash' then return {'corrupt'} end
`;

export const READ_SCRIPT = kind + `
if kind == 'none' then return {'empty'} end
if kind == 'string' then return {'legacy', redis.call('GET', KEYS[1])} end
if redis.call('HGET', KEYS[1], 'format') ~= ARGV[1] then return {'corrupt'} end
if ARGV[2] == 'all' then return {'hash', redis.call('HGETALL', KEYS[1])} end
local raw = redis.call('HGET', KEYS[1], 'card:' .. ARGV[3])
local asset = false
if raw then
  local record = cjson.decode(raw)
  if type(record.loaderAssetId) == 'string' then
    asset = redis.call('HGET', KEYS[1], 'asset:' .. record.loaderAssetId)
  end
end
return {'card', raw, asset}
`;

// Create, prune, release and personalization use a version-checked transaction.
// Only changed fields are written; unrelated card/asset fields remain untouched.
export const GLOBAL_CAS_SCRIPT = kind + `
if kind == 'string' then return -2 end
if kind == 'hash' and redis.call('HGET', KEYS[1], 'format') ~= ARGV[1] then return -1 end
local version = kind == 'hash' and redis.call('HGET', KEYS[1], 'version') or '0'
if version ~= ARGV[2] then return 0 end
local puts = cjson.decode(ARGV[5])
local deletes = cjson.decode(ARGV[6])
redis.call('HSET', KEYS[1], 'format', ARGV[1], 'version', ARGV[3], 'bytes', ARGV[4])
for field, value in pairs(puts) do redis.call('HSET', KEYS[1], field, value) end
for _, field in ipairs(deletes) do redis.call('HDEL', KEYS[1], field) end
return 1
`;

// Comparing the whole record also covers delivery state, which has its own
// lifecycle and can change without incrementing the public content revision.
export const CARD_CAS_SCRIPT = kind + `
if kind == 'string' then return -2 end
if kind ~= 'hash' or redis.call('HGET', KEYS[1], 'format') ~= ARGV[1] then return -1 end
local field = 'card:' .. ARGV[2]
local old = redis.call('HGET', KEYS[1], field)
if not old or old ~= ARGV[3] then return 0 end
local version = redis.call('HGET', KEYS[1], 'version')
local bytes = tonumber(redis.call('HGET', KEYS[1], 'bytes'))
if not tonumber(version) or not bytes or bytes < 0 or bytes > 3000000 or tonumber(version) >= 9007199254740991 then return -1 end
local nextVersion = string.format('%.0f', tonumber(version) + 1)
local nextBytes = bytes + string.len(ARGV[4]) - string.len(old) + string.len(nextVersion) - string.len(version)
if nextBytes > 3000000 then return -3 end
redis.call('HSET', KEYS[1], field, ARGV[4], 'version', nextVersion, 'bytes', tostring(nextBytes))
return 1
`;

// Explicit migration only: the caller must durably save the original bytes
// before this script runs. A concurrent legacy write makes it fail unchanged.
export const MIGRATE_SCRIPT = kind + `
if kind ~= 'string' or redis.call('GET', KEYS[1]) ~= ARGV[2] then return 0 end
local fields = cjson.decode(ARGV[3])
redis.call('DEL', KEYS[1])
redis.call('HSET', KEYS[1], 'format', ARGV[1], 'version', ARGV[4], 'bytes', ARGV[5])
for field, value in pairs(fields) do redis.call('HSET', KEYS[1], field, value) end
return 1
`;
