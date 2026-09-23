// Package null provides a store that caches nothing.
//
// Every read misses and every write is discarded, which turns caching off
// without changing a single call site:
//
//	c := cache.New(null.New())
//
// Use it to measure what your cache is actually saving you, to reproduce a bug
// the cache is hiding, or in tests that must always exercise the origin.
package null

import (
	"context"
	"time"

	"github.com/zahansafallwa1511/gocache"
)

// Store discards everything written to it and reports every read as a miss.
type Store struct{}

// New returns a store that caches nothing.
func New() Store { return Store{} }

// Get implements [cache.Store] by always reporting a miss.
func (Store) Get(context.Context, string) ([]byte, error) { return nil, cache.ErrNotFound }

// Put implements [cache.Store] by discarding the value.
func (Store) Put(context.Context, string, []byte, time.Duration) error { return nil }

// Add implements [cache.Store]. It reports success — nothing is stored, so
// nothing conflicts — which means Add cannot be used as a guard against this
// store.
func (Store) Add(context.Context, string, []byte, time.Duration) error { return nil }

// Increment reports the delta as though the counter had started at zero, but
// stores nothing.
func (Store) Increment(_ context.Context, _ string, delta int64) (int64, error) {
	return delta, nil
}

// Forget implements [cache.Store].
func (Store) Forget(context.Context, string) error { return nil }

// Flush implements [cache.Store].
func (Store) Flush(context.Context) error { return nil }

// TTL always reports a miss.
func (Store) TTL(context.Context, string) (time.Duration, error) { return 0, cache.ErrNotFound }

// Acquire always succeeds: with no shared state there is nothing to contend
// for. Code under test that depends on a lock being exclusive will not see that
// behaviour here — use the memory store for those tests.
func (Store) Acquire(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}

// Release implements [cache.LockStore].
func (Store) Release(context.Context, string, string) (bool, error) { return true, nil }

// ForceRelease implements [cache.LockStore].
func (Store) ForceRelease(context.Context, string) error { return nil }

var (
	_ cache.TTLStore  = Store{}
	_ cache.LockStore = Store{}
)
