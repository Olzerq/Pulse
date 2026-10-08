//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	pulseredis "github.com/Olzerq/Pulse/internal/redis"
	"github.com/google/uuid"
)

func TestRedisLockOwnershipAndTTL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := pulseredis.Open(ctx, environment("PULSE_INTEGRATION_REDIS_ADDR", "localhost:6379"), 3*time.Second)
	if err != nil {
		t.Fatalf("open Redis: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	store := pulseredis.NewLockStore(client)
	monitorID := "integration-" + uuid.NewString()
	firstOwner := "worker-" + uuid.NewString()
	secondOwner := "worker-" + uuid.NewString()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		_, _ = store.Complete(cleanupCtx, monitorID, firstOwner, 0)
		_, _ = store.Complete(cleanupCtx, monitorID, secondOwner, 0)
	})

	acquired, err := store.Acquire(ctx, monitorID, firstOwner, 2*time.Second)
	if err != nil || !acquired {
		t.Fatalf("first Acquire() = %v, %v; want true, nil", acquired, err)
	}
	acquired, err = store.Acquire(ctx, monitorID, secondOwner, 2*time.Second)
	if err != nil || acquired {
		t.Fatalf("contended Acquire() = %v, %v; want false, nil", acquired, err)
	}

	completed, err := store.Complete(ctx, monitorID, secondOwner, 0)
	if err != nil || completed {
		t.Fatalf("wrong-owner Complete() = %v, %v; want false, nil", completed, err)
	}
	completed, err = store.Complete(ctx, monitorID, firstOwner, 250*time.Millisecond)
	if err != nil || !completed {
		t.Fatalf("owner Complete() = %v, %v; want true, nil", completed, err)
	}

	acquired, err = store.Acquire(ctx, monitorID, secondOwner, time.Second)
	if err != nil || acquired {
		t.Fatalf("Acquire() before TTL = %v, %v; want false, nil", acquired, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for !acquired && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
		acquired, err = store.Acquire(ctx, monitorID, secondOwner, time.Second)
		if err != nil {
			t.Fatalf("Acquire() after TTL: %v", err)
		}
	}
	if !acquired {
		t.Fatal("lock was not released after its TTL")
	}

	completed, err = store.Complete(ctx, monitorID, secondOwner, 0)
	if err != nil || !completed {
		t.Fatalf("final Complete() = %v, %v; want true, nil", completed, err)
	}
}
