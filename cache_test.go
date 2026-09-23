package cache_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zahansafallwa1511/gocache"
	"github.com/zahansafallwa1511/gocache/memory"
	"github.com/zahansafallwa1511/gocache/null"
)

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func newCache(t *testing.T, opts ...cache.Option) *cache.Cache {
	t.Helper()
	store := memory.New()
	t.Cleanup(func() { store.Close() })
	return cache.New(store, opts...)
}

func TestSetGetRoundTrip(t *testing.T) {
	c := newCache(t)
	want := user{ID: 1, Name: "Ada"}

	if err := c.Set(t.Context(), "user:1", want, time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := cache.Get[user](t.Context(), c, "user:1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != want {
		t.Fatalf("Get = %+v, want %+v", got, want)
	}
}

func TestGetMissingReportsErrNotFound(t *testing.T) {
	c := newCache(t)
	if _, err := cache.Get[user](t.Context(), c, "absent"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
}

func TestGetOrReturnsFallback(t *testing.T) {
	c := newCache(t)
	got, err := cache.GetOr(t.Context(), c, "absent", user{Name: "anonymous"})
	if err != nil {
		t.Fatalf("GetOr: %v", err)
	}
	if got.Name != "anonymous" {
		t.Fatalf("GetOr = %+v, want the fallback", got)
	}
}

func TestPrefixIsolatesCaches(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { store.Close() })

	a := cache.New(store, cache.WithPrefix("a:"))
	b := cache.New(store, cache.WithPrefix("b:"))

	if err := a.Set(t.Context(), "k", "from a", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := cache.Get[string](t.Context(), b, "k"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("prefix b saw prefix a's key: %v", err)
	}
}

func TestDefaultTTLApplies(t *testing.T) {
	c := newCache(t, cache.WithDefaultTTL(20*time.Millisecond))
	if err := c.Set(t.Context(), "k", "v", cache.Forever); err != nil {
		t.Fatalf("Set: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	if _, err := cache.Get[string](t.Context(), c, "k"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("default TTL was not applied: %v", err)
	}
}

func TestForeverIgnoresDefaultTTL(t *testing.T) {
	c := newCache(t, cache.WithDefaultTTL(20*time.Millisecond))
	if err := c.Forever(t.Context(), "k", "v"); err != nil {
		t.Fatalf("Forever: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	if _, err := cache.Get[string](t.Context(), c, "k"); err != nil {
		t.Fatalf("Forever entry expired: %v", err)
	}
}

func TestPullRemovesTheKey(t *testing.T) {
	c := newCache(t)
	if err := c.Set(t.Context(), "flash", "message", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := cache.Pull[string](t.Context(), c, "flash")
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if got != "message" {
		t.Fatalf("Pull = %q, want %q", got, "message")
	}
	if has, _ := c.Has(t.Context(), "flash"); has {
		t.Fatal("Pull left the key behind")
	}
}

func TestHasAndMissing(t *testing.T) {
	c := newCache(t)
	if err := c.Set(t.Context(), "k", 1, time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if has, err := c.Has(t.Context(), "k"); err != nil || !has {
		t.Fatalf("Has = %v, %v; want true, nil", has, err)
	}
	if missing, err := c.Missing(t.Context(), "k"); err != nil || missing {
		t.Fatalf("Missing = %v, %v; want false, nil", missing, err)
	}
	if missing, err := c.Missing(t.Context(), "other"); err != nil || !missing {
		t.Fatalf("Missing on an absent key = %v, %v; want true, nil", missing, err)
	}
}

func TestAddIsOneShot(t *testing.T) {
	c := newCache(t)
	if err := c.Add(t.Context(), "k", "first", time.Minute); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := c.Add(t.Context(), "k", "second", time.Minute); !errors.Is(err, cache.ErrNotStored) {
		t.Fatalf("second Add = %v, want ErrNotStored", err)
	}
}

func TestIncrementAndDecrement(t *testing.T) {
	c := newCache(t)
	if n, err := c.Increment(t.Context(), "hits", 3); err != nil || n != 3 {
		t.Fatalf("Increment = %d, %v; want 3, nil", n, err)
	}
	if n, err := c.Decrement(t.Context(), "hits", 1); err != nil || n != 2 {
		t.Fatalf("Decrement = %d, %v; want 2, nil", n, err)
	}

	got, err := cache.Get[int64](t.Context(), c, "hits")
	if err != nil || got != 2 {
		t.Fatalf("Get of a counter = %d, %v; want 2, nil", got, err)
	}
}

func TestManyAndSetMany(t *testing.T) {
	c := newCache(t)
	err := c.SetMany(t.Context(), map[string]any{
		"a": user{ID: 1, Name: "Ada"},
		"b": user{ID: 2, Name: "Bob"},
	}, time.Minute)
	if err != nil {
		t.Fatalf("SetMany: %v", err)
	}

	var a, b, missing user
	found, err := c.Many(t.Context(), []string{"a", "b", "nope"}, []any{&a, &b, &missing})
	if err != nil {
		t.Fatalf("Many: %v", err)
	}
	if len(found) != 2 || !found["a"] || !found["b"] {
		t.Fatalf("Many found %v, want a and b only", found)
	}
	if a.Name != "Ada" || b.Name != "Bob" {
		t.Fatalf("Many decoded %+v / %+v", a, b)
	}
	if missing != (user{}) {
		t.Fatalf("Many wrote to the destination of a missing key: %+v", missing)
	}
}

func TestTTLReportsRemainingLifetime(t *testing.T) {
	c := newCache(t)
	if err := c.Set(t.Context(), "k", "v", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ttl, err := c.TTL(t.Context(), "k")
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("TTL = %v, want (0, 1m]", ttl)
	}
}

func TestRememberCallsTheLoaderOnce(t *testing.T) {
	c := newCache(t)
	var calls atomic.Int64

	load := func(ctx context.Context) (user, error) {
		calls.Add(1)
		return user{ID: 1, Name: "Ada"}, nil
	}

	for range 3 {
		got, err := cache.Remember(t.Context(), c, "user:1", time.Minute, load)
		if err != nil {
			t.Fatalf("Remember: %v", err)
		}
		if got.Name != "Ada" {
			t.Fatalf("Remember = %+v", got)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader ran %d times, want 1", n)
	}
}

func TestRememberDoesNotCacheErrors(t *testing.T) {
	c := newCache(t)
	sentinel := errors.New("database is down")

	_, err := cache.Remember(t.Context(), c, "k", time.Minute, func(context.Context) (user, error) {
		return user{}, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Remember = %v, want the loader's error", err)
	}
	if has, _ := c.Has(t.Context(), "k"); has {
		t.Fatal("Remember cached a failed load")
	}
}

func TestRememberCollapsesConcurrentMisses(t *testing.T) {
	c := newCache(t)
	var calls atomic.Int64

	load := func(ctx context.Context) (int, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return 42, nil
	}

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := cache.Remember(context.Background(), c, "hot", time.Minute, load); err != nil || got != 42 {
				t.Errorf("Remember = %d, %v", got, err)
			}
		}()
	}
	wg.Wait()

	if n := calls.Load(); n != 1 {
		t.Fatalf("loader ran %d times under a stampede, want 1", n)
	}
}

func TestRememberForeverIgnoresDefaultTTL(t *testing.T) {
	c := newCache(t, cache.WithDefaultTTL(20*time.Millisecond))
	load := func(context.Context) (string, error) { return "v", nil }

	if _, err := cache.RememberForever(t.Context(), c, "k", load); err != nil {
		t.Fatalf("RememberForever: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := cache.Get[string](t.Context(), c, "k"); err != nil {
		t.Fatalf("RememberForever entry expired: %v", err)
	}
}

func TestFlexibleServesStaleAndRefreshes(t *testing.T) {
	c := newCache(t)
	var calls atomic.Int64

	load := func(context.Context) (int, error) {
		return int(calls.Add(1)), nil
	}

	if got, err := cache.Flexible(t.Context(), c, "k", 30*time.Millisecond, time.Minute, load); err != nil || got != 1 {
		t.Fatalf("Flexible = %d, %v; want 1, nil", got, err)
	}

	if got, _ := cache.Flexible(t.Context(), c, "k", 30*time.Millisecond, time.Minute, load); got != 1 {
		t.Fatalf("fresh Flexible = %d, want 1", got)
	}

	time.Sleep(50 * time.Millisecond)

	if got, _ := cache.Flexible(t.Context(), c, "k", 30*time.Millisecond, time.Minute, load); got != 1 {
		t.Fatalf("stale Flexible = %d, want the stale value 1", got)
	}
	c.WaitForRefreshes()

	if got, _ := cache.Flexible(t.Context(), c, "k", 30*time.Millisecond, time.Minute, load); got != 2 {
		t.Fatalf("Flexible after refresh = %d, want 2", got)
	}
}

func TestFlexibleRejectsInvertedWindow(t *testing.T) {
	c := newCache(t)
	load := func(context.Context) (int, error) { return 1, nil }

	if _, err := cache.Flexible(t.Context(), c, "k", time.Minute, time.Second, load); err == nil {
		t.Fatal("Flexible accepted fresh > stale")
	}
}

func TestTagsIsolateAndFlush(t *testing.T) {
	c := newCache(t)
	posts := c.Tags("posts")
	users := c.Tags("users")

	if err := posts.Set(t.Context(), "feed", "post feed", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := users.Set(t.Context(), "feed", "user feed", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if got, _ := cache.Get[string](t.Context(), posts, "feed"); got != "post feed" {
		t.Fatalf("tagged Get = %q", got)
	}
	if got, _ := cache.Get[string](t.Context(), users, "feed"); got != "user feed" {
		t.Fatalf("tagged Get = %q", got)
	}

	if has, _ := c.Has(t.Context(), "feed"); has {
		t.Fatal("a tagged entry leaked into the untagged cache")
	}

	if err := posts.FlushTags(t.Context()); err != nil {
		t.Fatalf("FlushTags: %v", err)
	}
	if has, _ := posts.Has(t.Context(), "feed"); has {
		t.Fatal("FlushTags left the tagged entry readable")
	}
	if got, _ := cache.Get[string](t.Context(), users, "feed"); got != "user feed" {
		t.Fatal("FlushTags invalidated an unrelated tag")
	}
}

func TestFlushTagsRequiresTags(t *testing.T) {
	c := newCache(t)
	if err := c.FlushTags(t.Context()); err == nil {
		t.Fatal("FlushTags on an untagged cache succeeded, want an error")
	}
}

func TestLockIsExclusive(t *testing.T) {
	c := newCache(t)

	first, err := c.Lock("import", time.Minute)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	second, err := c.Lock("import", time.Minute)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	if ok, err := first.Acquire(t.Context()); err != nil || !ok {
		t.Fatalf("Acquire = %v, %v; want true, nil", ok, err)
	}
	if ok, err := second.Acquire(t.Context()); err != nil || ok {
		t.Fatalf("Acquire of a held lock = %v, %v; want false, nil", ok, err)
	}
	if err := first.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if ok, err := second.Acquire(t.Context()); err != nil || !ok {
		t.Fatalf("Acquire after Release = %v, %v; want true, nil", ok, err)
	}
}

func TestLockGetRunsAndReleases(t *testing.T) {
	c := newCache(t)
	lock, err := c.Lock("import", time.Minute)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	ran := false
	ok, err := lock.Get(t.Context(), func(context.Context) error {
		ran = true
		return nil
	})
	if err != nil || !ok || !ran {
		t.Fatalf("Get = %v, %v; ran = %v", ok, err, ran)
	}

	if ok, _ := lock.Acquire(t.Context()); !ok {
		t.Fatal("Get did not release the lock")
	}
}

func TestLockBlockTimesOut(t *testing.T) {
	c := newCache(t)
	held, _ := c.Lock("import", time.Minute)
	if ok, _ := held.Acquire(t.Context()); !ok {
		t.Fatal("could not take the lock")
	}

	waiter, _ := c.Lock("import", time.Minute, cache.WithRetryInterval(5*time.Millisecond))
	err := waiter.Block(t.Context(), 30*time.Millisecond)
	if !errors.Is(err, cache.ErrLockTimeout) {
		t.Fatalf("Block = %v, want ErrLockTimeout", err)
	}
}

func TestLockBlockAcquiresOnceFreed(t *testing.T) {
	c := newCache(t)
	held, _ := c.Lock("import", time.Minute)
	if ok, _ := held.Acquire(t.Context()); !ok {
		t.Fatal("could not take the lock")
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		held.Release(context.Background())
	}()

	waiter, _ := c.Lock("import", time.Minute, cache.WithRetryInterval(5*time.Millisecond))
	if err := waiter.Block(t.Context(), time.Second); err != nil {
		t.Fatalf("Block: %v", err)
	}
}

func TestLockNeedsALockStore(t *testing.T) {
	c := cache.New(unlockableStore{memory.New()})
	if _, err := c.Lock("k", time.Minute); !errors.Is(err, cache.ErrNoLockStore) {
		t.Fatalf("Lock = %v, want ErrNoLockStore", err)
	}
}

type unlockableStore struct{ cache.Store }

func TestListenerSeesHitsAndMisses(t *testing.T) {
	var hits, misses, writes, forgets atomic.Int64
	c := newCache(t, cache.WithListener(cache.ListenerFuncs{
		Hit:    func(context.Context, string) { hits.Add(1) },
		Miss:   func(context.Context, string) { misses.Add(1) },
		Write:  func(context.Context, string) { writes.Add(1) },
		Forget: func(context.Context, string) { forgets.Add(1) },
	}))

	var dest string
	c.Get(t.Context(), "k", &dest)
	c.Set(t.Context(), "k", "v", time.Minute)
	c.Get(t.Context(), "k", &dest)
	c.Forget(t.Context(), "k")

	if hits.Load() != 1 || misses.Load() != 1 || writes.Load() != 1 || forgets.Load() != 1 {
		t.Fatalf("events = %d hits, %d misses, %d writes, %d forgets; want 1 each",
			hits.Load(), misses.Load(), writes.Load(), forgets.Load())
	}
}

func TestNullStoreCachesNothing(t *testing.T) {
	c := cache.New(null.New())
	if err := c.Set(t.Context(), "k", "v", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := cache.Get[string](t.Context(), c, "k"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get from the null store = %v, want ErrNotFound", err)
	}

	var calls int
	for range 2 {
		got, err := cache.Remember(t.Context(), c, "k", time.Minute, func(context.Context) (int, error) {
			calls++
			return 7, nil
		})
		if err != nil || got != 7 {
			t.Fatalf("Remember = %d, %v", got, err)
		}
	}
	if calls != 2 {
		t.Fatalf("loader ran %d times against the null store, want 2", calls)
	}
}

func TestManagerResolvesStoresByName(t *testing.T) {
	m := cache.NewManager()
	m.Register("memory", newCache(t))
	m.Register("null", cache.New(null.New()))

	if got := m.Default(); got != m.MustStore("memory") {
		t.Fatal("the first registered store should be the default")
	}
	if err := m.SetDefault("null"); err != nil {
		t.Fatalf("SetDefault: %v", err)
	}
	if got := m.Default(); got != m.MustStore("null") {
		t.Fatal("SetDefault did not take effect")
	}
	if _, err := m.Store("redis"); err == nil {
		t.Fatal("Store of an unregistered name succeeded, want an error")
	}
	if err := m.SetDefault("redis"); err == nil {
		t.Fatal("SetDefault of an unregistered name succeeded, want an error")
	}
	if names := m.Names(); len(names) != 2 || names[0] != "memory" || names[1] != "null" {
		t.Fatalf("Names = %v, want [memory null]", names)
	}
}
