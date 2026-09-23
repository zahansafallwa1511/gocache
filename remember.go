package cache

import (
	"context"
	"errors"
	"time"
)

// Get decodes the value at key into a freshly allocated T. It is the typed
// counterpart to [Cache.Get]:
//
//	user, err := cache.Get[User](ctx, c, "user:1")
//	if errors.Is(err, cache.ErrNotFound) {
//		// not cached
//	}
func Get[T any](ctx context.Context, c *Cache, key string) (T, error) {
	var value T
	err := c.Get(ctx, key, &value)
	return value, err
}

// GetOr behaves like [Get] but returns fallback instead of an error when the
// key is missing. Errors other than a miss are still reported.
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

// Remember returns the value cached at key. On a miss it calls fn, stores the
// result for ttl, and returns it:
//
//	user, err := cache.Remember(ctx, c, "user:1", time.Hour,
//		func(ctx context.Context) (User, error) {
//			return db.FindUser(ctx, 1)
//		})
//
// Concurrent misses for the same key share a single call to fn, so a hot key
// expiring does not stampede the origin. The deduplication is per-process: with
// several application instances, each runs fn at most once. Use [Cache.Lock] if
// the loader must run only once across the whole fleet.
//
// If fn returns an error, nothing is cached and the error is returned unchanged,
// so a failing origin is retried on the next call rather than caching the
// failure.
func Remember[T any](ctx context.Context, c *Cache, key string, ttl time.Duration, fn func(context.Context) (T, error)) (T, error) {
	return remember(ctx, c, key, fn, func(v T) error { return c.Set(ctx, key, v, ttl) })
}

// RememberForever is [Remember] with no expiry. It ignores any default TTL, so
// the value lives until it is forgotten or flushed.
func RememberForever[T any](ctx context.Context, c *Cache, key string, fn func(context.Context) (T, error)) (T, error) {
	return remember(ctx, c, key, fn, func(v T) error { return c.Forever(ctx, key, v) })
}

// remember is the shared body of Remember and RememberForever; store decides
// how the loaded value is written.
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

// Pull decodes the value at key into a new T and removes it from the cache.
func Pull[T any](ctx context.Context, c *Cache, key string) (T, error) {
	var value T
	err := c.Pull(ctx, key, &value)
	return value, err
}
