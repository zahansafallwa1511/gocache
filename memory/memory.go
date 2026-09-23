// Package memory provides an in-process cache store.
//
// Entries are held in sharded maps, expired lazily on read and swept by a
// background janitor. Nothing crosses a process boundary: each instance of your
// application has its own copy, which makes this the fastest driver and the
// wrong one for anything that must be consistent across instances.
//
//	store := memory.New()
//	c := cache.New(store)
//	defer c.Close()
//
// The store is unbounded. Entries live until they expire, so give them a TTL
// unless the keyspace is naturally small.
package memory

import (
	"context"
	"errors"
	"hash/maphash"
	"sync"
	"time"

	"github.com/zahansafallwa1511/gocache"
)

// shardCount is the number of independently locked maps entries are spread
// across. It bounds lock contention: 32 shards keep unrelated keys from
// serialising on one mutex without wasting much memory on an idle cache.
const shardCount = 32

// entry is one cached value and its deadline.
type entry struct {
	value     []byte
	expiresAt time.Time
}

// expired reports whether e is past its deadline at now.
func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// shard is one lock-striped slice of the keyspace.
type shard struct {
	mu    sync.RWMutex
	items map[string]entry
}

// Store is an in-memory [cache.Store]. It is safe for concurrent use. Create one
// with [New], and call Close to stop its janitor when you are done.
//
// The store is unbounded: nothing is evicted before it expires, so give entries
// a TTL unless you know the keyspace is small. There is no size limit or LRU.
type Store struct {
	shards [shardCount]*shard
	seed   maphash.Seed

	stop     chan struct{}
	stopOnce sync.Once
}

// Option configures a [Store].
type Option func(*config)

// config holds the resolved options.
type config struct {
	interval time.Duration
}

// WithCleanupInterval sets how often expired entries are swept. Entries are
// always expired lazily on read, so this only bounds the memory held by keys
// nobody asks for. Zero disables the janitor. The default is one minute.
func WithCleanupInterval(d time.Duration) Option {
	return func(c *config) { c.interval = d }
}

// New returns an empty in-memory store.
//
//	store := memory.New()
//	c := cache.New(store)
//	defer c.Close()
func New(opts ...Option) *Store {
	cfg := config{interval: time.Minute}
	for _, opt := range opts {
		opt(&cfg)
	}

	s := &Store{seed: maphash.MakeSeed(), stop: make(chan struct{})}
	for i := range s.shards {
		s.shards[i] = &shard{items: make(map[string]entry)}
	}
	if cfg.interval > 0 {
		go s.janitor(cfg.interval)
	}
	return s
}

// shard selects the lock stripe owning key.
func (s *Store) shard(key string) *shard {
	return s.shards[maphash.String(s.seed, key)%shardCount]
}

// janitor periodically drops expired entries, reclaiming memory held by keys
// that are never read again. It exits when the store is closed.
func (s *Store) janitor(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-t.C:
			for _, sh := range s.shards {
				sh.mu.Lock()
				for k, e := range sh.items {
					if e.expired(now) {
						delete(sh.items, k)
					}
				}
				sh.mu.Unlock()
			}
		}
	}
}

// expiry converts a ttl to a deadline, with the zero time meaning no expiry.
func expiry(ttl time.Duration) time.Time {
	if ttl == cache.Forever {
		return time.Time{}
	}
	return time.Now().Add(ttl)
}

// Get implements [cache.Store]. The returned slice is a copy, so callers cannot
// mutate what the next reader sees.
func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	sh := s.shard(key)
	sh.mu.RLock()
	e, ok := sh.items[key]
	sh.mu.RUnlock()

	if !ok || e.expired(time.Now()) {
		return nil, cache.ErrNotFound
	}

	return append([]byte(nil), e.value...), nil
}

// Put implements [cache.Store]. The value is copied.
func (s *Store) Put(_ context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return s.Forget(context.Background(), key)
	}
	sh := s.shard(key)
	sh.mu.Lock()
	sh.items[key] = entry{value: append([]byte(nil), value...), expiresAt: expiry(ttl)}
	sh.mu.Unlock()
	return nil
}

// Add implements [cache.Store].
func (s *Store) Add(_ context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return cache.ErrNotStored
	}
	sh := s.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if e, ok := sh.items[key]; ok && !e.expired(time.Now()) {
		return cache.ErrNotStored
	}
	sh.items[key] = entry{value: append([]byte(nil), value...), expiresAt: expiry(ttl)}
	return nil
}

// Increment implements [cache.Store]. The read-modify-write happens under the
// shard's lock, so concurrent increments cannot lose an update. An existing
// entry keeps its deadline.
func (s *Store) Increment(_ context.Context, key string, delta int64) (int64, error) {
	sh := s.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	var (
		current int64
		expires time.Time
	)
	if e, ok := sh.items[key]; ok && !e.expired(time.Now()) {
		n, err := cache.ParseCounter(e.value)
		if err != nil {
			return 0, err
		}
		current, expires = n, e.expiresAt
	}

	current += delta
	sh.items[key] = entry{value: cache.FormatCounter(current), expiresAt: expires}
	return current, nil
}

// Forget implements [cache.Store].
func (s *Store) Forget(_ context.Context, key string) error {
	sh := s.shard(key)
	sh.mu.Lock()
	delete(sh.items, key)
	sh.mu.Unlock()
	return nil
}

// Flush implements [cache.Store] by replacing every shard's map.
func (s *Store) Flush(context.Context) error {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.items = make(map[string]entry)
		sh.mu.Unlock()
	}
	return nil
}

// TTL implements [cache.TTLStore].
func (s *Store) TTL(_ context.Context, key string) (time.Duration, error) {
	sh := s.shard(key)
	sh.mu.RLock()
	e, ok := sh.items[key]
	sh.mu.RUnlock()

	now := time.Now()
	if !ok || e.expired(now) {
		return 0, cache.ErrNotFound
	}
	if e.expiresAt.IsZero() {
		return cache.Forever, nil
	}
	return e.expiresAt.Sub(now), nil
}

// Many implements [cache.ManyStore].
func (s *Store) Many(ctx context.Context, keys []string) ([][]byte, error) {
	values := make([][]byte, len(keys))
	for i, k := range keys {
		v, err := s.Get(ctx, k)
		if err == nil {
			values[i] = v
		}
	}
	return values, nil
}

// PutMany implements [cache.ManyStore].
func (s *Store) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	for k, v := range values {
		if err := s.Put(ctx, k, v, ttl); err != nil {
			return err
		}
	}
	return nil
}

// Acquire implements [cache.LockStore].
func (s *Store) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	err := s.Add(ctx, key, []byte(owner), ttl)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, cache.ErrNotStored):
		return false, nil
	default:
		return false, err
	}
}

// Release implements [cache.LockStore], freeing the lock only if owner still
// holds it.
func (s *Store) Release(_ context.Context, key, owner string) (bool, error) {
	sh := s.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.items[key]
	if !ok || e.expired(time.Now()) || string(e.value) != owner {
		return false, nil
	}
	delete(sh.items, key)
	return true, nil
}

// ForceRelease implements [cache.LockStore].
func (s *Store) ForceRelease(ctx context.Context, key string) error {
	return s.Forget(ctx, key)
}

// Close stops the janitor. The store stays readable and writable afterwards but
// no longer sweeps expired entries, so closing and continuing to use it leaks
// memory. Close is safe to call more than once.
func (s *Store) Close() error {
	s.stopOnce.Do(func() { close(s.stop) })
	return nil
}

var (
	_ cache.TTLStore  = (*Store)(nil)
	_ cache.ManyStore = (*Store)(nil)
	_ cache.LockStore = (*Store)(nil)
	_ cache.Closer    = (*Store)(nil)
)
