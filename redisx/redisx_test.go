package redisx_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zahansafallwa1511/gocache"
	"github.com/zahansafallwa1511/gocache/redisx"
	"github.com/zahansafallwa1511/gocache/storetest"
)

func client(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to run the Redis integration tests")
	}

	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect to %s: %v", addr, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestStore(t *testing.T) {
	c := client(t)
	storetest.Run(t, func(t *testing.T) cache.Store {
		if err := c.FlushDB(t.Context()).Err(); err != nil {
			t.Fatalf("FlushDB: %v", err)
		}
		return redisx.New(c)
	})
}

func TestCacheAgainstRealRedis(t *testing.T) {
	rdb := client(t)
	if err := rdb.FlushDB(t.Context()).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}
	c := cache.New(redisx.New(rdb), cache.WithPrefix("test:"))

	type user struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	calls := 0
	load := func(context.Context) (user, error) {
		calls++
		return user{ID: 1, Name: "Ada"}, nil
	}

	for range 3 {
		got, err := cache.Remember(t.Context(), c, "user:1", time.Hour, load)
		if err != nil {
			t.Fatalf("Remember: %v", err)
		}
		if got.Name != "Ada" {
			t.Fatalf("Remember = %+v", got)
		}
	}
	if calls != 1 {
		t.Fatalf("loader ran %d times, want 1", calls)
	}

	tagged := c.Tags("users")
	if err := tagged.Set(t.Context(), "feed", []string{"a"}, time.Hour); err != nil {
		t.Fatalf("tagged Set: %v", err)
	}
	if err := tagged.FlushTags(t.Context()); err != nil {
		t.Fatalf("FlushTags: %v", err)
	}
	if has, _ := tagged.Has(t.Context(), "feed"); has {
		t.Fatal("FlushTags left the entry readable")
	}

	lock, err := c.Lock("import", time.Minute)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	other, _ := c.Lock("import", time.Minute)

	if ok, err := lock.Acquire(t.Context()); err != nil || !ok {
		t.Fatalf("Acquire = %v, %v", ok, err)
	}
	if ok, _ := other.Acquire(t.Context()); ok {
		t.Fatal("two holders acquired the same lock")
	}
	if err := other.Release(t.Context()); err != nil {
		t.Fatalf("Release by a non-owner: %v", err)
	}
	if ok, _ := lock.Acquire(t.Context()); ok {
		t.Fatal("a non-owner released the lock")
	}
	if err := lock.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if ok, _ := other.Acquire(t.Context()); !ok {
		t.Fatal("lock was not released")
	}
}
