package redis

import (
	"context"
	"fmt"
	"time"
)

type lockClient interface {
	SetNX(context.Context, string, string, time.Duration) (bool, error)
	CompleteIfValue(context.Context, string, string, time.Duration) (bool, error)
}

// LockStore coordinates Pinger instances through short-lived Redis leases.
// A lease always has a TTL, so a crashed process cannot block a monitor forever.
type LockStore struct {
	client lockClient
}

func NewLockStore(client lockClient) *LockStore {
	return &LockStore{client: client}
}

func (s *LockStore) Acquire(
	ctx context.Context,
	monitorID string,
	owner string,
	ttl time.Duration,
) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("monitor lock TTL must be greater than zero")
	}

	acquired, err := s.client.SetNX(ctx, lockKey(monitorID), owner, ttl)
	if err != nil {
		return false, fmt.Errorf("acquire monitor lock: %w", err)
	}
	return acquired, nil
}

// Complete keeps the claim until the next interval, or deletes it when the
// interval has already elapsed. The owner check and TTL change happen in one
// Lua script, so an old worker cannot modify a newer owner's lock.
func (s *LockStore) Complete(
	ctx context.Context,
	monitorID string,
	owner string,
	holdFor time.Duration,
) (bool, error) {
	completed, err := s.client.CompleteIfValue(ctx, lockKey(monitorID), owner, holdFor)
	if err != nil {
		return false, fmt.Errorf("complete monitor lock: %w", err)
	}
	return completed, nil
}

func lockKey(monitorID string) string {
	return "monitor:" + monitorID + ":lock"
}
