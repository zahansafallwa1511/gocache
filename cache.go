package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

type Cache struct {
	store      Store
	codec      Codec
	prefix     string
	defaultTTL time.Duration
	listeners  []Listener
	tags       []string
	group      *group
	refreshing *refreshSet

	onRefreshError func(ctx context.Context, key string, err error)
}

type refreshSet struct {
	mu   sync.Mutex
	wg   sync.WaitGroup
	keys map[string]struct{}
}

type Option func(*Cache)

func WithPrefix(prefix string) Option {
	return func(c *Cache) { c.prefix = prefix }
}

func WithCodec(codec Codec) Option {
	return func(c *Cache) { c.codec = codec }
}

func WithDefaultTTL(ttl time.Duration) Option {
	return func(c *Cache) { c.defaultTTL = ttl }
}

func WithListener(l Listener) Option {
	return func(c *Cache) { c.listeners = append(c.listeners, l) }
}

func New(store Store, opts ...Option) *Cache {
	c := &Cache{store: store, codec: JSONCodec{}, group: new(group), refreshing: new(refreshSet)}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Cache) Store() Store { return c.store }

func (c *Cache) ttl(ttl time.Duration) time.Duration {
	if ttl == Forever {
		return c.defaultTTL
	}
	return ttl
}

func (c *Cache) emit(ctx context.Context, fn func(Listener, context.Context, string), key string) {
	for _, l := range c.listeners {
		fn(l, ctx, key)
	}
}

func (c *Cache) Get(ctx context.Context, key string, dest any) error {
	full, err := c.key(ctx, key)
	if err != nil {
		return err
	}
	data, err := c.store.Get(ctx, full)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			c.emit(ctx, Listener.OnMiss, full)
		}
		return err
	}
	c.emit(ctx, Listener.OnHit, full)
	return c.codec.Unmarshal(data, dest)
}

func (c *Cache) Has(ctx context.Context, key string) (bool, error) {
	full, err := c.key(ctx, key)
	if err != nil {
		return false, err
	}
	switch _, err := c.store.Get(ctx, full); {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

func (c *Cache) Missing(ctx context.Context, key string) (bool, error) {
	has, err := c.Has(ctx, key)
	return !has, err
}

func (c *Cache) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return c.put(ctx, key, value, c.ttl(ttl))
}

func (c *Cache) Forever(ctx context.Context, key string, value any) error {
	return c.put(ctx, key, value, Forever)
}

func (c *Cache) put(ctx context.Context, key string, value any, ttl time.Duration) error {
	full, err := c.key(ctx, key)
	if err != nil {
		return err
	}
	data, err := c.codec.Marshal(value)
	if err != nil {
		return err
	}
	if err := c.store.Put(ctx, full, data, ttl); err != nil {
		return err
	}
	c.emit(ctx, Listener.OnWrite, full)
	return nil
}

func (c *Cache) Add(ctx context.Context, key string, value any, ttl time.Duration) error {
	full, err := c.key(ctx, key)
	if err != nil {
		return err
	}
	data, err := c.codec.Marshal(value)
	if err != nil {
		return err
	}
	if err := c.store.Add(ctx, full, data, c.ttl(ttl)); err != nil {
		return err
	}
	c.emit(ctx, Listener.OnWrite, full)
	return nil
}

func (c *Cache) Forget(ctx context.Context, key string) error {
	full, err := c.key(ctx, key)
	if err != nil {
		return err
	}
	if err := c.store.Forget(ctx, full); err != nil {
		return err
	}
	c.emit(ctx, Listener.OnForget, full)
	return nil
}

func (c *Cache) Flush(ctx context.Context) error { return c.store.Flush(ctx) }

func (c *Cache) Pull(ctx context.Context, key string, dest any) error {
	if err := c.Get(ctx, key, dest); err != nil {
		return err
	}
	return c.Forget(ctx, key)
}

func (c *Cache) Many(ctx context.Context, keys []string, dests []any) (map[string]bool, error) {
	if len(keys) != len(dests) {
		return nil, errors.New("cache: Many needs one destination per key")
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		f, err := c.key(ctx, k)
		if err != nil {
			return nil, err
		}
		full[i] = f
	}

	values, err := c.many(ctx, full)
	if err != nil {
		return nil, err
	}

	found := make(map[string]bool, len(keys))
	for i, data := range values {
		if data == nil {
			c.emit(ctx, Listener.OnMiss, full[i])
			continue
		}
		if err := c.codec.Unmarshal(data, dests[i]); err != nil {
			return found, err
		}
		c.emit(ctx, Listener.OnHit, full[i])
		found[keys[i]] = true
	}
	return found, nil
}

func (c *Cache) many(ctx context.Context, full []string) ([][]byte, error) {
	if s, ok := c.store.(ManyStore); ok {
		return s.Many(ctx, full)
	}
	values := make([][]byte, len(full))
	for i, k := range full {
		data, err := c.store.Get(ctx, k)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		values[i] = data
	}
	return values, nil
}

func (c *Cache) SetMany(ctx context.Context, values map[string]any, ttl time.Duration) error {
	encoded := make(map[string][]byte, len(values))
	full := make([]string, 0, len(values))
	for k, v := range values {
		f, err := c.key(ctx, k)
		if err != nil {
			return err
		}
		data, err := c.codec.Marshal(v)
		if err != nil {
			return err
		}
		encoded[f] = data
		full = append(full, f)
	}

	if s, ok := c.store.(ManyStore); ok {
		if err := s.PutMany(ctx, encoded, c.ttl(ttl)); err != nil {
			return err
		}
	} else {
		for k, data := range encoded {
			if err := c.store.Put(ctx, k, data, c.ttl(ttl)); err != nil {
				return err
			}
		}
	}
	for _, k := range full {
		c.emit(ctx, Listener.OnWrite, k)
	}
	return nil
}

func (c *Cache) ForgetMany(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		if err := c.Forget(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	full, err := c.key(ctx, key)
	if err != nil {
		return 0, err
	}
	return c.store.Increment(ctx, full, delta)
}

func (c *Cache) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return c.Increment(ctx, key, -delta)
}

func (c *Cache) TTL(ctx context.Context, key string) (time.Duration, error) {
	s, ok := c.store.(TTLStore)
	if !ok {
		return 0, errors.New("cache: store does not report TTLs")
	}
	full, err := c.key(ctx, key)
	if err != nil {
		return 0, err
	}
	return s.TTL(ctx, full)
}

func (c *Cache) Close() error {
	if s, ok := c.store.(Closer); ok {
		return s.Close()
	}
	return nil
}

func (c *Cache) Tags(tags ...string) *Cache {
	clone := *c
	clone.tags = append(append([]string(nil), c.tags...), tags...)
	sort.Strings(clone.tags)
	return &clone
}

func (c *Cache) FlushTags(ctx context.Context) error {
	if len(c.tags) == 0 {
		return errors.New("cache: FlushTags called on an untagged cache")
	}
	for _, tag := range c.tags {
		if err := c.store.Put(ctx, c.tagVersionKey(tag), []byte(randomToken()), Forever); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) tagVersionKey(tag string) string { return c.prefix + "tag:" + tag + ":version" }

func (c *Cache) key(ctx context.Context, key string) (string, error) {
	if len(c.tags) == 0 {
		return c.prefix + key, nil
	}
	namespace, err := c.tagNamespace(ctx)
	if err != nil {
		return "", err
	}
	return c.prefix + namespace + ":" + key, nil
}

func (c *Cache) tagNamespace(ctx context.Context) (string, error) {
	var b strings.Builder
	for _, tag := range c.tags {
		version, err := c.tagVersion(ctx, tag)
		if err != nil {
			return "", err
		}
		b.WriteString(version)
		b.WriteByte('|')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "tags:" + hex.EncodeToString(sum[:8]), nil
}

func (c *Cache) tagVersion(ctx context.Context, tag string) (string, error) {
	vkey := c.tagVersionKey(tag)
	data, err := c.store.Get(ctx, vkey)
	if err == nil {
		return string(data), nil
	}
	if !errors.Is(err, ErrNotFound) {
		return "", err
	}

	version := randomToken()
	switch err := c.store.Add(ctx, vkey, []byte(version), Forever); {
	case err == nil:
		return version, nil
	case errors.Is(err, ErrNotStored):
		data, err := c.store.Get(ctx, vkey)
		if err != nil {
			return "", err
		}
		return string(data), nil
	default:
		return "", err
	}
}

func WithRefreshErrorHandler(fn func(ctx context.Context, key string, err error)) Option {
	return func(c *Cache) { c.onRefreshError = fn }
}

func (c *Cache) reportRefreshError(ctx context.Context, key string, err error) {
	if c.onRefreshError != nil {
		c.onRefreshError(ctx, key, err)
	}
}

func (c *Cache) refresh(key string, fn func()) {
	c.refreshing.mu.Lock()
	if c.refreshing.keys == nil {
		c.refreshing.keys = make(map[string]struct{})
	}
	if _, busy := c.refreshing.keys[key]; busy {
		c.refreshing.mu.Unlock()
		return
	}
	c.refreshing.keys[key] = struct{}{}
	c.refreshing.mu.Unlock()

	c.refreshing.wg.Add(1)
	go func() {
		defer func() {
			c.refreshing.mu.Lock()
			delete(c.refreshing.keys, key)
			c.refreshing.mu.Unlock()
			c.refreshing.wg.Done()
		}()
		fn()
	}()
}

func (c *Cache) WaitForRefreshes() { c.refreshing.wg.Wait() }
