package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrLockFailed = errors.New("failed to acquire distributed lock")
	ErrLockLost   = errors.New("lock lease expired or released by another party")
)

// Lua script to safely release lock only if the token matches
const releaseLockLua = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`

type DistributedLock struct {
	client    *redis.Client
	key       string
	token     string
	ttl       time.Duration
	releaseFn *redis.Script
}

func NewDistributedLock(client *redis.Client, resource string, ttl time.Duration) *DistributedLock {
	tokenBytes := make([]byte, 16)
	_, _ = rand.Read(tokenBytes)
	token := hex.EncodeToString(tokenBytes)

	return &DistributedLock{
		client:    client,
		key:       fmt.Sprintf("lock:%s", resource),
		token:     token,
		ttl:       ttl,
		releaseFn: redis.NewScript(releaseLockLua),
	}
}

// Acquire attempts to set the lock key atomically using NX and PX
func (l *DistributedLock) Acquire(ctx context.Context) (bool, error) {
	if l.client == nil {
		// If Redis is down, return false or error. In financial operations,
		// PostgreSQL transaction locks (SELECT FOR UPDATE) serve as ultimate ground truth.
		return false, fmt.Errorf("redis unavailable for lock")
	}

	success, err := l.client.SetNX(ctx, l.key, l.token, l.ttl).Result()
	if err != nil {
		return false, err
	}
	return success, nil
}

// Release releases the lock using token-checked atomic lua script
func (l *DistributedLock) Release(ctx context.Context) error {
	if l.client == nil {
		return nil
	}

	res, err := l.releaseFn.Run(ctx, l.client, []string{l.key}, l.token).Int()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrLockLost
	}
	return nil
}

// Token returns the fencing/lease token
func (l *DistributedLock) Token() string {
	return l.token
}
