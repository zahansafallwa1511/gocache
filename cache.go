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

// Cache wraps a [Store] with higher-level operations: value encoding, key
// prefixing, tags, locks, events and stampede protection. The zero value is not
// usable; construct one with [New].
//
// A Cache is safe for concurrent use if its Store is, and every built-in store
// is. Copies made by [Cache.Tags] share the underlying store.
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

// refreshSet tracks the background refreshes [Flexible] has in flight, so that
// a key is rebuilt at most once at a time and shutdown can wait for them.
type refreshSet struct {
	mu   sync.Mutex
	wg   sync.WaitGroup
	keys map[string]struct{}
}

// Option configures a [Cache] at construction time.
type Option func(*Cache)

// WithPrefix namespaces every key this Cache reads or writes. It is the usual
// way to share one backend between several applications, or to keep a test's
// keys away from a development cache.
func WithPrefix(prefix string) Option {
	return func(c *Cache) { c.prefix = prefix }
}

// WithCodec sets the encoder used for values. The default is [JSONCodec].
func WithCodec(codec Codec) Option {
	return func(c *Cache) { c.codec = codec }
}

// WithDefaultTTL sets the lifetime used by writes that are passed a ttl of
// [Forever]. It does not affect [Cache.Forever] or [RememberForever], which
// always store without an expiry.
func WithDefaultTTL(ttl time.Duration) Option {
	return func(c *Cache) { c.defaultTTL = ttl }
}

// WithListener registers an observer of cache activity, typically to record
// hit rates. Listeners are called in registration order. May be passed more
// than once.
func WithListener(l Listener) Option {
	return func(c *Cache) { c.listeners = append(c.listeners, l) }
}

// New returns a Cache backed by store.
//
//	c := cache.New(memory.New(),
//		cache.WithPrefix("myapp:"),
//		cache.WithDefaultTTL(10*time.Minute),
//	)
//	defer c.Close()
func New(store Store, opts ...Option) *Cache {
	c := &Cache{store: store, codec: JSONCodec{}, group: new(group), refreshing: new(refreshSet)}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Store returns the underlying store, for operations specific to one driver.
func (c *Cache) Store() Store { return c.store }

// ttl applies the configured default to a caller's Forever.
func (c *Cache) ttl(ttl time.Duration) time.Duration {
	if ttl == Forever {
		return c.defaultTTL
	}
	return ttl
}

// emit notifies every listener. Callbacks run inline, so a slow listener slows
// the cache operation that triggered it.
func (c *Cache) emit(ctx context.Context, fn func(Listener, context.Context, string), key string) {
	for _, l := range c.listeners {
		fn(l, ctx, key)
	}
}

// Get decodes the value stored at key into dest, which must be a non-nil
// pointer. It returns [ErrNotFound] if the key is absent or expired.
//
// Prefer the generic [Get] function, which allocates the destination for you.
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

// Has reports whether key holds a live entry. It does not decode the value, but
// on most drivers it still transfers it, so prefer [Cache.Get] if you intend to
// read the value anyway.
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

// Missing is the negation of [Cache.Has].
func (c *Cache) Missing(ctx context.Context, key string) (bool, error) {
	has, err := c.Has(ctx, key)
	return !has, err
}

// Set encodes value and stores it at key for ttl. A ttl of [Forever] falls back
// to the Cache's default TTL, which is itself Forever unless [WithDefaultTTL]
// was used. A negative ttl removes the key.
func (c *Cache) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return c.put(ctx, key, value, c.ttl(ttl))
}

// Forever stores value with no expiry, ignoring any default TTL. The entry
// lives until it is forgotten, flushed, or evicted by the backend.
func (c *Cache) Forever(ctx context.Context, key string, value any) error {
	return c.put(ctx, key, value, Forever)
}

// put is the shared write path, with the ttl already resolved.
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

// Add stores value only if key is not already taken, and returns [ErrNotStored]
// when it is. Because the check and the write are atomic in every driver, Add
// is a usable one-shot guard:
//
//	err := c.Add(ctx, "welcome-email:"+userID, true, 24*time.Hour)
//	if errors.Is(err, cache.ErrNotStored) {
//		return nil // already sent today
//	}
//
// For anything longer-lived than a single flag, prefer [Cache.Lock].
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

// Forget removes key. Removing a key that is not there is not an error.
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

// Flush removes every entry from the underlying store — including keys written
// by other Caches sharing it, whatever their prefix or tags. To clear one
// application's entries, give it a prefix and forget them, or use tags and
// [Cache.FlushTags].
func (c *Cache) Flush(ctx context.Context) error { return c.store.Flush(ctx) }

// Pull decodes the value at key into dest and removes it, for one-shot values
// such as a flash message.
func (c *Cache) Pull(ctx context.Context, key string, dest any) error {
	if err := c.Get(ctx, key, dest); err != nil {
		return err
	}
	return c.Forget(ctx, key)
}

// Many decodes several keys in one call. dests must hold one non-nil pointer
// per key, positionally matched. The returned set names the keys that were
// found; destinations for missing keys are left untouched.
//
//	var a, b User
//	found, err := c.Many(ctx, []string{"user:1", "user:2"}, []any{&a, &b})
//	if found["user:1"] { ... }
//
// Stores implementing [ManyStore] serve this in one round trip.
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

// many fetches raw values, in one round trip where the store allows it.
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

// SetMany writes every entry of values with the same ttl, in one round trip
// where the store implements [ManyStore]. It is not atomic: on error some
// entries may already be written.
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

// ForgetMany removes several keys, stopping at the first error.
func (c *Cache) ForgetMany(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		if err := c.Forget(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

// Increment adds delta to the counter at key and returns the new value. A
// missing key starts at zero. The operation is atomic in every driver, so it is
// safe for rate limiting and hit counting across processes.
//
// Counters are stored so that [Get] with an integer type still reads them.
func (c *Cache) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	full, err := c.key(ctx, key)
	if err != nil {
		return 0, err
	}
	return c.store.Increment(ctx, full, delta)
}

// Decrement subtracts delta from the counter at key and returns the new value.
// Counters may go negative.
func (c *Cache) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return c.Increment(ctx, key, -delta)
}

// TTL reports the time left before key expires, [Forever] for entries without
// an expiry, and [ErrNotFound] if the key is absent. It returns an error if the
// underlying store does not implement [TTLStore].
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

// Close releases the underlying store if it implements [Closer], stopping any
// background goroutine it runs. The Cache must not be used afterwards.
func (c *Cache) Close() error {
	if s, ok := c.store.(Closer); ok {
		return s.Close()
	}
	return nil
}

// Tags returns a view of this Cache whose entries belong to every named tag.
// Reads and writes through the returned Cache are invisible to the untagged
// Cache and to differently tagged ones, and [Cache.FlushTags] invalidates them
// together:
//
//	posts := c.Tags("posts", "user:1")
//	posts.Set(ctx, "feed", feed, time.Hour)
//	posts.FlushTags(ctx) // drops every entry tagged posts or user:1
//
// Invalidation versions each tag rather than enumerating keys, so FlushTags
// costs one write per tag however many entries it drops, and works on backends
// that cannot list keys. The cost is that orphaned entries are not reclaimed
// eagerly: they occupy space until their own TTL expires. Give tagged entries a
// TTL rather than storing them forever.
//
// Tags cost one extra read per operation, cached by neither side, so a tagged
// Cache is slower than a prefixed one. Reach for tags when you need grouped
// invalidation, not merely namespacing.
func (c *Cache) Tags(tags ...string) *Cache {
	clone := *c
	clone.tags = append(append([]string(nil), c.tags...), tags...)
	sort.Strings(clone.tags)
	return &clone
}

// FlushTags invalidates every entry written through this tagged Cache by
// rotating the version of each of its tags. It reports an error on a Cache with
// no tags; use [Cache.Flush] for that.
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

// tagVersionKey names the entry holding one tag's current version.
func (c *Cache) tagVersionKey(tag string) string { return c.prefix + "tag:" + tag + ":version" }

// key resolves an application key to the key handed to the store, applying the
// prefix and, for a tagged Cache, the current version of every tag.
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

// tagNamespace digests the current versions of this Cache's tags. Rotating any
// one version changes the digest, orphaning everything written under the old
// one. Tags are sorted by Tags, so the digest does not depend on the order they
// were named in.
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

// tagVersion reads a tag's version, creating it if this is the first use. Two
// processes racing to create the same tag agree on a version: the one that
// loses Add reads back the winner's.
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

// WithRefreshErrorHandler installs a callback for errors raised by the
// background refreshes [Flexible] performs, which have no caller to return them
// to. Without it such errors are dropped and the stale value is served until it
// expires.
func WithRefreshErrorHandler(fn func(ctx context.Context, key string, err error)) Option {
	return func(c *Cache) { c.onRefreshError = fn }
}

// reportRefreshError hands a background failure to the configured handler.
func (c *Cache) reportRefreshError(ctx context.Context, key string, err error) {
	if c.onRefreshError != nil {
		c.onRefreshError(ctx, key, err)
	}
}

// refresh runs fn on its own goroutine unless a refresh for key is already in
// flight, so a burst of stale reads triggers one rebuild.
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

// WaitForRefreshes blocks until every background refresh started by [Flexible]
// has finished. Call it during graceful shutdown, and in tests that need to
// observe a refreshed value.
func (c *Cache) WaitForRefreshes() { c.refreshing.wg.Wait() }
