// Package redis contains Redis-backed runtime state storage for Pulse.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

var errKeyNotFound = errors.New("Redis key not found")

type Client struct {
	inner *goredis.Client
}

func Open(ctx context.Context, address string, operationTimeout time.Duration) (*Client, error) {
	inner := goredis.NewClient(&goredis.Options{
		Addr:         address,
		DialTimeout:  operationTimeout,
		ReadTimeout:  operationTimeout,
		WriteTimeout: operationTimeout,
		PoolTimeout:  operationTimeout,
	})
	client := &Client{inner: inner}

	pingCtx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := inner.Ping(pingCtx).Err(); err != nil {
		_ = inner.Close()
		return nil, fmt.Errorf("ping Redis: %w", err)
	}

	return client, nil
}

func (c *Client) Set(ctx context.Context, key, value string) error {
	if err := c.inner.Set(ctx, key, value, 0).Err(); err != nil {
		return fmt.Errorf("set Redis key: %w", err)
	}
	return nil
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	value, err := c.inner.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		return "", errKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("get Redis key: %w", err)
	}
	return value, nil
}

func (c *Client) Close() error {
	return c.inner.Close()
}
