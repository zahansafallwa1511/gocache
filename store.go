package cache

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("cache: key not found")

var ErrNotStored = errors.New("cache: value not stored")

const Forever time.Duration = 0

type Store interface {
	Get(ctx context.Context, key string) ([]byte, error)

	Put(ctx context.Context, key string, value []byte, ttl time.Duration) error

	Add(ctx context.Context, key string, value []byte, ttl time.Duration) error

	Increment(ctx context.Context, key string, delta int64) (int64, error)

	Forget(ctx context.Context, key string) error

	Flush(ctx context.Context) error
}

type TTLStore interface {
	Store

	TTL(ctx context.Context, key string) (time.Duration, error)
}

type Closer interface {
	Close() error
}

type ManyStore interface {
	Store

	Many(ctx context.Context, keys []string) ([][]byte, error)

	PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error
}

type LockStore interface {
	Store

	Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)

	Release(ctx context.Context, key, owner string) (bool, error)

	ForceRelease(ctx context.Context, key string) error
}
