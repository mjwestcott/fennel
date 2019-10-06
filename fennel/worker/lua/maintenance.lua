-- Workers (or the Executor processes they manage) could fail at any moment. In order to
-- recover from this, we need to 1. discover that it has occurred, and 2. ensure that
-- any pending tasks (either read from the queue and not executed or failed
-- mid-execution) are processed by another worker.
--
-- To achieve this we publish periodic heartbeats from every Executor. If they are
-- missing for longer than `threshold` seconds, we consider them to have died. Redis
-- Streams keeps track of a pending entry list (PEL) for every consumer: read, but
-- unacknowledged messsages. We simply delete them and add them back to the queue.
--
-- In the worst case scenario, where Executors are not in fact dead, this will lead to
-- duplicate execution.

local stream = KEYS[1]
local beats = KEYS[2]
local group = ARGV[1]
local now = tonumber(ARGV[2])
local threshold = tonumber(ARGV[3])
local heartbeats = redis.call('HGETALL', beats)
local count = redis.call('XINFO', 'GROUPS', stream)[1][6]
local pending = redis.call('XPENDING', stream, group, '-', '+', count or 0)
local consumers = redis.call('XINFO', 'CONSUMERS', stream, group)
local dead_executors, dead_consumers, result = {}, {}, {}

-- Find the dead executors.
for i=1, #heartbeats, 2 do
    local executor_id, last_heartbeat = heartbeats[i], tonumber(heartbeats[i + 1])
    if now - last_heartbeat > threshold then
        dead_executors[executor_id] = true
        redis.call('HDEL', beats, executor_id)
    end
end

-- Find the dead consumers.
for i, v in ipairs(consumers) do
    local consumer_id, executor_id = v[2], string.sub(v[2], 1, 22)
    if dead_executors[executor_id] then
        dead_consumers[consumer_id] = true
        table.insert(result, consumer_id)
    end
end

-- Put the dead consumers' pending messages back in the stream.
for _, v in ipairs(pending) do
    local id, consumer_id = v[1], v[2]
    if dead_consumers[consumer_id] then
        for _, msg in ipairs(redis.call('XRANGE', stream, id, id)) do
            redis.call('XADD', stream, '*', unpack(msg[2]))
            redis.call('XDEL', stream, id)
        end
    end
end

-- Delete the dead consumers. This will also delete the consumer's PEL.
for consumer_id in pairs(dead_consumers) do
    redis.call('XGROUP', 'DELCONSUMER', stream, group, consumer_id)
end

return result
