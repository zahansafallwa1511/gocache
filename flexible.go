package cache

import (
	"context"
	"errors"
	"time"
)

// flexEntry is the envelope Flexible stores: the value, plus the instant it
// stops counting as fresh. The entry's own TTL enforces the stale bound.
type flexEntry[T any] struct {
	Value    T     `json:"value"`
	FreshFor int64 `json:"fresh_until"`
}

// Flexible implements stale-while-revalidate: it keeps serving a cached value
// past its freshness deadline while a new one is computed in the background, so
// callers never wait on a rebuild.
//
//	stats, err := cache.Flexible(ctx, c, "stats", time.Minute, time.Hour, computeStats)
//
// The value is returned from cache without calling fn while it is younger than
// fresh. Between fresh and stale it is still returned immediately, but a refresh
// is started behind the caller so the next one sees new data. Past stale the
// entry is gone and fn is called inline, and that caller waits.
//
// fresh must be less than stale. Background refreshes are deduplicated per key
// and run on a context detached from the caller's, so they survive the request
// that triggered them; their errors have nowhere to be returned, so observe them
// with [WithRefreshErrorHandler] and wait for them with [Cache.WaitForRefreshes].
//
// Use it for values that are expensive to compute and tolerable slightly stale —
// dashboards, counts, feeds. Use [Remember] when callers must never see a stale
// value.
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

// flexLoad computes a new value, stores it with its freshness deadline, and
// returns it. Concurrent loads of one key are collapsed into a single call.
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
