-- Adapted from https://redis.io/tutorials/howtos/ratelimiting/#5-leaky-bucket
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local leak_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local data = redis.call('HGETALL', key)
local level = 0
local last_leak = now

if #data > 0 then
  local fields = {}
  for i = 1, #data, 2 do
    fields[data[i]] = data[i + 1]
  end
  level = tonumber(fields['level']) or 0
  last_leak = tonumber(fields['last_leak']) or now
end

-- Drain based on elapsed time.
local elapsed = now - last_leak
local leaked = elapsed * leak_rate
level = math.max(0, level - leaked)

local available = math.max(0, capacity - level)
-- In this scheduler use-case, one call may admit multiple units.
local allowed = math.min(requested, math.floor(available))
level = level + allowed
local remaining = math.max(0, math.floor(capacity - level))

redis.call('HSET', key, 'level', tostring(level), 'last_leak', tostring(now))
redis.call('EXPIRE', key, ttl)

return { allowed, remaining }
