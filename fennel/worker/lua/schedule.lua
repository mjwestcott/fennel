-- Tasks which have failed are scheduled for retry according to exponential backoff. We
-- store them in a Redis sorted set where the score is the desried execution time. This
-- can be polled to find tasks whose time has passed, which are added back to the queue.

local schedule = KEYS[1]
local stream = KEYS[2]
local timestamp = ARGV[1]
local val = redis.call('ZRANGEBYSCORE', schedule, '-inf', timestamp)

if #val > 0 then
    redis.call('ZREMRANGEBYSCORE', schedule, '-inf', timestamp)

    for _, v in ipairs(val) do
        redis.call('XADD', stream, '*', 'uuid', v)
    end
end

return val
