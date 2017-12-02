package ergoq

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

const (
	RETRY_QUEUE     = "ergoq-retry:"
	MESSAGE_COUNTER = "ergoq-counter:"
)

var (
	popScript           *redis.Script
	ackScript           *redis.Script
	queueNonAckedScript *redis.Script
)

func init() {
	// redis lua script to POP message from queue
	popScript = redis.NewScript(2, fmt.Sprintf(`
		local topic = KEYS[1]
		local ts = KEYS[2]
	    local function add_message_to_retry_queue(topic, message, timestamp)
			local retry_queue = "%s" .. topic
			local queue_message_counter = "%s" .. topic
			local message_id = redis.call("INCR", queue_message_counter)
			local full_message = message_id .. ":"
			full_message = full_message .. message
			redis.call("ZADD", retry_queue, timestamp, full_message)
			return message_id
	    end

		local value = redis.call("RPOP", topic)
		if value == false then
			return redis.error_reply("no message returned")
		end

		local id = add_message_to_retry_queue(topic, value, ts)
		return {topic, value, id .. ""}`, RETRY_QUEUE, MESSAGE_COUNTER))

	// redis lua script to re-queue non acked messages
	queueNonAckedScript = redis.NewScript(3, fmt.Sprintf(`
		local topic = KEYS[1]
		local time = KEYS[2]
		local timeout = KEYS[3]
		local maxRecords = tonumber(ARGV[1])
		if maxRecords == nil then
			maxRecords = %d
		end
		local retryQueue = "%s" .. topic
		local result = redis.call("ZRANGEBYSCORE", retryQueue, 0, time - timeout)
		local num = 0
		
		for _, value in ipairs(result) do

			if num >= maxRecords then break end

			local index = string.find(value, ":")
			local id = value:sub(0, index)
			index = index + 1
			local tempValue = value:sub(index)

			local lex = "[" .. id
			redis.call("ZREM", retryQueue, value)
			redis.call("RPUSH", topic, tempValue)
			num = num + 1
		end
		return num .. ""`, REQUEUE_NON_ACKED_NUM, RETRY_QUEUE))
}
