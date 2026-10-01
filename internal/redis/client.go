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

func (c *Client) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	acquired, err := c.inner.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("set Redis key if absent: %w", err)
	}
	return acquired, nil
}

func (c *Client) CompleteIfValue(
	ctx context.Context,
	key string,
	value string,
	holdFor time.Duration,
) (bool, error) {
	const script = `
if redis.call("get", KEYS[1]) ~= ARGV[1] then
  return 0
end
if tonumber(ARGV[2]) > 0 then
  return redis.call("pexpire", KEYS[1], ARGV[2])
end
return redis.call("del", KEYS[1])
`

	updated, err := c.inner.Eval(ctx, script, []string{key}, value, holdFor.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("complete owned Redis key: %w", err)
	}
	return updated == 1, nil
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

func (c *Client) MGet(ctx context.Context, keys ...string) ([]any, error) {
	values, err := c.inner.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("get multiple Redis keys: %w", err)
	}
	return values, nil
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.inner.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping Redis: %w", err)
	}
	return nil
}

func (c *Client) Close() error {
	return c.inner.Close()
}
