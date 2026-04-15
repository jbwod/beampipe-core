-- Adapted from https://redis.io/tutorials/howtos/ratelimiting/#4-token-bucket
local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local data = redis.call('HGETALL', key)
local tokens = max_tokens
local last_refill = now

if #data > 0 then
  local fields = {}
  for i = 1, #data, 2 do
    fields[data[i]] = data[i + 1]
  end
  tokens = tonumber(fields['tokens']) or max_tokens
  last_refill = tonumber(fields['last_refill']) or now
end

-- Refill tokens based on elapsed time.
local elapsed = now - last_refill
local new_tokens = elapsed * refill_rate
tokens = math.min(max_tokens, tokens + new_tokens)

-- In this scheduler use-case, one call may admit multiple units.
local allowed = math.min(requested, math.floor(tokens))
tokens = tokens - allowed
local remaining = tokens

redis.call('HSET', key, 'tokens', tostring(tokens), 'last_refill', tostring(now))
redis.call('EXPIRE', key, ttl)

return { allowed, math.floor(remaining) }
