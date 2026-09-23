package cache

import (
	"context"
	"errors"
	"time"
)

func Get[T any](ctx context.Context, c *Cache, key string) (T, error) {
	var value T
	err := c.Get(ctx, key, &value)
	return value, err
}

func GetOr[T any](ctx context.Context, c *Cache, key string, fallback T) (T, error) {
	value, err := Get[T](ctx, c, key)
	if errors.Is(err, ErrNotFound) {
		return fallback, nil
	}
	if err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}

func Remember[T any](ctx context.Context, c *Cache, key string, ttl time.Duration, fn func(context.Context) (T, error)) (T, error) {
	return remember(ctx, c, key, fn, func(v T) error { return c.Set(ctx, key, v, ttl) })
}

func RememberForever[T any](ctx context.Context, c *Cache, key string, fn func(context.Context) (T, error)) (T, error) {
	return remember(ctx, c, key, fn, func(v T) error { return c.Forever(ctx, key, v) })
}

func remember[T any](ctx context.Context, c *Cache, key string, fn func(context.Context) (T, error), store func(T) error) (T, error) {
	var zero T

	value, err := Get[T](ctx, c, key)
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return zero, err
	}

	full, err := c.key(ctx, key)
	if err != nil {
		return zero, err
	}

	result, err := c.group.Do(full, func() (any, error) {

		if v, err := Get[T](ctx, c, key); err == nil {
			return v, nil
		} else if !errors.Is(err, ErrNotFound) {
			return nil, err
		}

		v, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		if err := store(v); err != nil {
			return nil, err
		}
		return v, nil
	})
	if err != nil {
		return zero, err
	}
	return result.(T), nil
}

func Pull[T any](ctx context.Context, c *Cache, key string) (T, error) {
	var value T
	err := c.Pull(ctx, key, &value)
	return value, err
}
