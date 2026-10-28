package ratelimit

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Sliding window log / token bucket atomic Lua script
const slidingWindowLua = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

-- Clear entries older than current window
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- Count current items in window
local current_requests = redis.call('ZCARD', key)

if current_requests < limit then
    -- Add current request timestamp with a unique member
    redis.call('ZADD', key, now, now .. '-' .. math.random(100000, 999999))
    redis.call('PEXPIRE', key, window)
    return 1
else
    return 0
end
`

type Limiter struct {
	client     *redis.Client
	luaScript  *redis.Script
	failClosed bool // When true, if Redis is down, reject request; if false, fail open
}

func NewLimiter(client *redis.Client, failClosed bool) *Limiter {
	return &Limiter{
		client:     client,
		luaScript:  redis.NewScript(slidingWindowLua),
		failClosed: failClosed,
	}
}

// Allow checks if the given key is within the limit in the specified duration
func (l *Limiter) Allow(ctx context.Context, identifier string, limit int, window time.Duration) (bool, error) {
	if l.client == nil {
		if l.failClosed {
			return false, fmt.Errorf("rate limiter unavailable: failing closed")
		}
		return true, nil // Degrade gracefully / fail open for financial endpoint availability
	}

	key := fmt.Sprintf("ratelimit:%s", identifier)
	nowMs := time.Now().UnixNano() / int64(time.Millisecond)
	windowMs := window.Milliseconds()

	res, err := l.luaScript.Run(ctx, l.client, []string{key}, nowMs, windowMs, limit).Int()
	if err != nil {
		if l.failClosed {
			return false, fmt.Errorf("redis rate limit check failed: %w", err)
		}
		// Degrade: log warning and fail open
		return true, nil
	}

	return res == 1, nil
}
