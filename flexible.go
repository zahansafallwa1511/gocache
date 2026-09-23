package cache

import (
	"context"
	"errors"
	"time"
)

type flexEntry[T any] struct {
	Value    T     `json:"value"`
	FreshFor int64 `json:"fresh_until"`
}

func Flexible[T any](ctx context.Context, c *Cache, key string, fresh, stale time.Duration, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	if fresh >= stale {
		return zero, errors.New("cache: Flexible needs fresh < stale")
	}

	entry, err := Get[flexEntry[T]](ctx, c, key)
	switch {
	case err == nil && time.Now().UnixMilli() < entry.FreshFor:
		return entry.Value, nil

	case err == nil:

		c.refresh(key, func() {
			bg := context.WithoutCancel(ctx)
			if _, err := flexLoad(bg, c, key, fresh, stale, fn); err != nil {
				c.reportRefreshError(bg, key, err)
			}
		})
		return entry.Value, nil

	case errors.Is(err, ErrNotFound):
		return flexLoad(ctx, c, key, fresh, stale, fn)

	default:
		return zero, err
	}
}

func flexLoad[T any](ctx context.Context, c *Cache, key string, fresh, stale time.Duration, fn func(context.Context) (T, error)) (T, error) {
	value, err := c.group.Do("flex:"+key, func() (any, error) {
		v, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		entry := flexEntry[T]{Value: v, FreshFor: time.Now().Add(fresh).UnixMilli()}
		if err := c.Set(ctx, key, entry, stale); err != nil {
			return nil, err
		}
		return v, nil
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return value.(T), nil
}
