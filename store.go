// Package cache provides a driver-agnostic caching API for Go.
//
// The package is split in two layers:
//
//   - [Store] is the small interface every driver implements. It deals in raw
//     []byte values and is the only thing a new backend must satisfy.
//   - [Cache] wraps a Store with the ergonomics you want at the call site:
//     Remember, Pull, Add, Forever, tags, locks and typed helpers.
//
// Drivers live in their own packages (memory, file, redis, sql, null) so that
// importing this package never pulls in a third-party dependency.
//
// A minimal read-through cache:
//
//	c := cache.New(memory.New())
//	defer c.Close()
//
//	user, err := cache.Remember(ctx, c, "user:1", time.Hour,
//		func(ctx context.Context) (User, error) {
//			return db.FindUser(ctx, 1)
//		})
//
// Values are encoded with a [Codec], JSON by default, so anything the codec
// handles can be cached. Reads report a miss as [ErrNotFound] rather than a
// second return value, so misses compose with the rest of your error handling.
package cache

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by [Store] implementations when a key is absent or
// has expired. Test for it with [errors.Is]:
//
//	v, err := cache.Get[User](ctx, c, "user:1")
//	if errors.Is(err, cache.ErrNotFound) {
//		// not cached
//	}
var ErrNotFound = errors.New("cache: key not found")

// ErrNotStored is returned when a conditional write such as [Cache.Add] did not
// take effect because the key already held a live entry.
var ErrNotStored = errors.New("cache: value not stored")

// Forever is the TTL that stores a value without an expiry.
const Forever time.Duration = 0

// Store is the contract implemented by cache backends.
//
// Implementations must be safe for concurrent use by multiple goroutines. A ttl
// of [Forever] means the entry never expires; a negative ttl means the entry is
// already expired and must not be stored.
//
// Implement this interface to add a backend, then prove it with
// [github.com/zahansafallwa1511/gocache/storetest].Run, the same conformance
// suite the built-in drivers run.
type Store interface {
	// Get returns the value stored at key, or ErrNotFound.
	Get(ctx context.Context, key string) ([]byte, error)

	// Put writes value at key, overwriting any existing entry.
	Put(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Add writes value only if key does not already hold a live entry. It
	// reports ErrNotStored when the key was taken.
	Add(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Increment atomically adds delta to the integer stored at key, creating it
	// at delta if absent, and returns the new value. Implementations must use
	// FormatCounter and ParseCounter for the stored representation.
	Increment(ctx context.Context, key string, delta int64) (int64, error)

	// Forget removes key. Removing a missing key is not an error.
	Forget(ctx context.Context, key string) error

	// Flush removes every entry in the store.
	Flush(ctx context.Context) error
}

// TTLStore is implemented by stores that can report the remaining lifetime of
// an entry. [Cache.TTL] requires it.
type TTLStore interface {
	Store

	// TTL reports the time left before key expires. It returns Forever for
	// entries without an expiry and ErrNotFound if the key is absent.
	TTL(ctx context.Context, key string) (time.Duration, error)
}

// ManyStore is implemented by stores that can read or write batches in one
// round trip. [Cache.Many] and [Cache.SetMany] use it when available and fall
// back to a loop otherwise.
type ManyStore interface {
	Store

	// Many returns the values for keys, in the same order. A missing key yields
	// a nil slice rather than an error.
	Many(ctx context.Context, keys []string) ([][]byte, error)

	// PutMany writes every entry of values with the same ttl.
	PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error
}

// LockStore is implemented by stores that can provide atomic, cross-process
// locks. [Cache.Lock] requires it.
type LockStore interface {
	Store

	// Acquire attempts to take the lock named by key on behalf of owner for at
	// most ttl. It reports whether the lock was taken.
	Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)

	// Release frees the lock named by key only if it is still held by owner. It
	// reports whether the lock was released.
	Release(ctx context.Context, key, owner string) (bool, error)

	// ForceRelease frees the lock regardless of which owner holds it.
	ForceRelease(ctx context.Context, key string) error
}

// Closer is implemented by stores holding resources — background goroutines,
// connections — that must be released. [Cache.Close] calls it when present.
type Closer interface {
	Close() error
}
