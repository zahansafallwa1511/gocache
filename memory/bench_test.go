package memory_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/zahansafallwa1511/gocache"
	"github.com/zahansafallwa1511/gocache/memory"
)

type payload struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Tags []string
}

func benchCache(b *testing.B) *cache.Cache {
	b.Helper()
	store := memory.New(memory.WithCleanupInterval(0))
	b.Cleanup(func() { store.Close() })
	return cache.New(store)
}

func BenchmarkStoreGet(b *testing.B) {
	store := memory.New(memory.WithCleanupInterval(0))
	defer store.Close()
	ctx := context.Background()
	store.Put(ctx, "key", []byte(`{"id":1}`), time.Hour)

	b.ReportAllocs()
	for b.Loop() {
		if _, err := store.Get(ctx, "key"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheGet(b *testing.B) {
	c := benchCache(b)
	ctx := context.Background()
	c.Set(ctx, "key", payload{ID: 1, Name: "Ada", Tags: []string{"a", "b"}}, time.Hour)

	b.ReportAllocs()
	for b.Loop() {
		if _, err := cache.Get[payload](ctx, c, "key"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheSet(b *testing.B) {
	c := benchCache(b)
	ctx := context.Background()
	value := payload{ID: 1, Name: "Ada", Tags: []string{"a", "b"}}

	b.ReportAllocs()
	for b.Loop() {
		if err := c.Set(ctx, "key", value, time.Hour); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRememberHit(b *testing.B) {
	c := benchCache(b)
	ctx := context.Background()
	load := func(context.Context) (payload, error) {
		return payload{ID: 1, Name: "Ada"}, nil
	}
	if _, err := cache.Remember(ctx, c, "key", time.Hour, load); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := cache.Remember(ctx, c, "key", time.Hour, load); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTaggedGet(b *testing.B) {
	c := benchCache(b).Tags("posts")
	ctx := context.Background()
	c.Set(ctx, "key", payload{ID: 1}, time.Hour)

	b.ReportAllocs()
	for b.Loop() {
		if _, err := cache.Get[payload](ctx, c, "key"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParallelGet(b *testing.B) {
	c := benchCache(b)
	ctx := context.Background()
	for i := range 64 {
		c.Set(ctx, fmt.Sprintf("key:%d", i), payload{ID: i}, time.Hour)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			if _, err := cache.Get[payload](ctx, c, fmt.Sprintf("key:%d", i%64)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
