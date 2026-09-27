package redis

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLockStoreAcquireUsesNXAndTTL(t *testing.T) {
	t.Parallel()

	client := &fakeLockClient{acquired: true}
	store := NewLockStore(client)
	acquired, err := store.Acquire(context.Background(), "monitor-id", "worker-id", 15*time.Second)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("Acquire() = false, want true")
	}
	if client.key != "monitor:monitor-id:lock" || client.value != "worker-id" || client.ttl != 15*time.Second {
		t.Errorf("SetNX arguments = key %q, value %q, ttl %s", client.key, client.value, client.ttl)
	}
}

func TestLockStoreReturnsContention(t *testing.T) {
	t.Parallel()

	store := NewLockStore(&fakeLockClient{})
	acquired, err := store.Acquire(context.Background(), "monitor-id", "worker-id", time.Second)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if acquired {
		t.Fatal("Acquire() = true, want false")
	}
}

func TestLockStoreCompleteChecksOwnerAndKeepsInterval(t *testing.T) {
	t.Parallel()

	client := &fakeLockClient{completed: true}
	store := NewLockStore(client)
	completed, err := store.Complete(context.Background(), "monitor-id", "worker-id", 4*time.Second)
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if !completed || client.key != "monitor:monitor-id:lock" || client.value != "worker-id" || client.holdFor != 4*time.Second {
		t.Errorf("Complete() = %v, key %q, value %q, holdFor %s", completed, client.key, client.value, client.holdFor)
	}
}

func TestLockStoreRejectsInvalidTTL(t *testing.T) {
	t.Parallel()

	client := &fakeLockClient{}
	_, err := NewLockStore(client).Acquire(context.Background(), "monitor-id", "worker-id", 0)
	if err == nil {
		t.Fatal("Acquire() error = nil, want validation error")
	}
	if client.setCalls != 0 {
		t.Errorf("SetNX calls = %d, want 0", client.setCalls)
	}
}

func TestLockStoreWrapsClientError(t *testing.T) {
	t.Parallel()

	want := errors.New("Redis unavailable")
	_, err := NewLockStore(&fakeLockClient{err: want}).Acquire(
		context.Background(), "monitor-id", "worker-id", time.Second,
	)
	if !errors.Is(err, want) {
		t.Fatalf("Acquire() error = %v, want wrapped %v", err, want)
	}
}

type fakeLockClient struct {
	acquired  bool
	completed bool
	err       error
	setCalls  int
	key       string
	value     string
	ttl       time.Duration
	holdFor   time.Duration
}

func (c *fakeLockClient) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	c.setCalls++
	c.key = key
	c.value = value
	c.ttl = ttl
	return c.acquired, c.err
}

func (c *fakeLockClient) CompleteIfValue(
	_ context.Context,
	key string,
	value string,
	holdFor time.Duration,
) (bool, error) {
	c.key = key
	c.value = value
	c.holdFor = holdFor
	return c.completed, c.err
}
