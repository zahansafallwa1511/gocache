package memory

import (
	"context"
	"errors"
	"hash/maphash"
	"sync"
	"time"

	"github.com/zahansafallwa1511/gocache"
)

const shardCount = 32

type entry struct {
	value     []byte
	expiresAt time.Time
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

type shard struct {
	mu    sync.RWMutex
	items map[string]entry
}

type Store struct {
	shards [shardCount]*shard
	seed   maphash.Seed

	stop     chan struct{}
	stopOnce sync.Once
}

type Option func(*config)

type config struct {
	interval time.Duration
}

func WithCleanupInterval(d time.Duration) Option {
	return func(c *config) { c.interval = d }
}

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

func (s *Store) shard(key string) *shard {
	return s.shards[maphash.String(s.seed, key)%shardCount]
}

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

func expiry(ttl time.Duration) time.Time {
	if ttl == cache.Forever {
		return time.Time{}
	}
	return time.Now().Add(ttl)
}

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

func (s *Store) Forget(_ context.Context, key string) error {
	sh := s.shard(key)
	sh.mu.Lock()
	delete(sh.items, key)
	sh.mu.Unlock()
	return nil
}

func (s *Store) Flush(context.Context) error {
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.items = make(map[string]entry)
		sh.mu.Unlock()
	}
	return nil
}

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

func (s *Store) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	for k, v := range values {
		if err := s.Put(ctx, k, v, ttl); err != nil {
			return err
		}
	}
	return nil
}

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

func (s *Store) ForceRelease(ctx context.Context, key string) error {
	return s.Forget(ctx, key)
}

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
