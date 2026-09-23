// Package storetest provides a conformance suite for [cache.Store]
// implementations.
//
// A driver — including one you write outside this module — proves it honours the
// contract by calling [Run] with a factory:
//
//	func TestStore(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) cache.Store {
//			return mystore.New()
//		})
//	}
//
// The suite covers the required behaviour of [cache.Store] and, when the store
// implements them, the optional [cache.TTLStore], [cache.ManyStore] and
// [cache.LockStore] interfaces. Cases for interfaces a store does not implement
// are skipped, so the same call works for a minimal driver and a full one.
//
// Some cases sleep for tens of milliseconds to observe expiry, so the suite
// takes a second or so per driver.
package storetest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zahansafallwa1511/gocache"
)

// Factory builds a store for one subtest. It must return an empty store: the
// suite assumes no key it did not write exists, so a factory for a shared
// backend should flush it, or point each subtest at its own namespace.
type Factory func(t *testing.T) cache.Store

// Run exercises the [cache.Store] contract, plus the optional [cache.TTLStore],
// [cache.ManyStore] and [cache.LockStore] interfaces when the store implements
// them. Each case runs as a subtest, so a failure names the behaviour that broke.
func Run(t *testing.T, newStore Factory) {
	t.Helper()

	t.Run("GetMissing", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Get(t.Context(), "nope"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("Get on a missing key = %v, want ErrNotFound", err)
		}
	})

	t.Run("PutGet", func(t *testing.T) {
		s := newStore(t)
		mustPut(t, s, "k", []byte("v"), time.Minute)

		got, err := s.Get(t.Context(), "k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(got) != "v" {
			t.Fatalf("Get = %q, want %q", got, "v")
		}
	})

	t.Run("PutOverwrites", func(t *testing.T) {
		s := newStore(t)
		mustPut(t, s, "k", []byte("one"), time.Minute)
		mustPut(t, s, "k", []byte("two"), time.Minute)

		if got, _ := s.Get(t.Context(), "k"); string(got) != "two" {
			t.Fatalf("Get = %q, want %q", got, "two")
		}
	})

	t.Run("Expiry", func(t *testing.T) {
		s := newStore(t)
		mustPut(t, s, "k", []byte("v"), 20*time.Millisecond)

		time.Sleep(60 * time.Millisecond)
		if _, err := s.Get(t.Context(), "k"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("Get after expiry = %v, want ErrNotFound", err)
		}
	})

	t.Run("Forever", func(t *testing.T) {
		s := newStore(t)
		mustPut(t, s, "k", []byte("v"), cache.Forever)

		time.Sleep(40 * time.Millisecond)
		if _, err := s.Get(t.Context(), "k"); err != nil {
			t.Fatalf("Get on a forever key: %v", err)
		}
	})

	t.Run("Add", func(t *testing.T) {
		s := newStore(t)
		if err := s.Add(t.Context(), "k", []byte("first"), time.Minute); err != nil {
			t.Fatalf("Add on a free key: %v", err)
		}
		if err := s.Add(t.Context(), "k", []byte("second"), time.Minute); !errors.Is(err, cache.ErrNotStored) {
			t.Fatalf("Add on a taken key = %v, want ErrNotStored", err)
		}
		if got, _ := s.Get(t.Context(), "k"); string(got) != "first" {
			t.Fatalf("Add overwrote the existing value: got %q", got)
		}
	})

	t.Run("AddAfterExpiry", func(t *testing.T) {
		s := newStore(t)
		mustAdd(t, s, "k", []byte("first"), 20*time.Millisecond)

		time.Sleep(60 * time.Millisecond)
		if err := s.Add(t.Context(), "k", []byte("second"), time.Minute); err != nil {
			t.Fatalf("Add after expiry: %v", err)
		}
	})

	t.Run("Increment", func(t *testing.T) {
		s := newStore(t)
		if n, err := s.Increment(t.Context(), "hits", 1); err != nil || n != 1 {
			t.Fatalf("Increment on a missing key = %d, %v; want 1, nil", n, err)
		}
		if n, err := s.Increment(t.Context(), "hits", 4); err != nil || n != 5 {
			t.Fatalf("Increment = %d, %v; want 5, nil", n, err)
		}
		if n, err := s.Increment(t.Context(), "hits", -2); err != nil || n != 3 {
			t.Fatalf("negative Increment = %d, %v; want 3, nil", n, err)
		}
	})

	t.Run("IncrementReadsBackAsAValue", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Increment(t.Context(), "hits", 7); err != nil {
			t.Fatalf("Increment: %v", err)
		}
		got, err := s.Get(t.Context(), "hits")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(got) != "7" {
			t.Fatalf("counter stored as %q, want %q — drivers must use cache.FormatCounter", got, "7")
		}
	})

	t.Run("ConcurrentIncrement", func(t *testing.T) {
		// A counter created under contention is where a read-modify-write
		// implementation loses updates: every writer sees "absent" at once.
		s := newStore(t)

		const workers = 50
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := s.Increment(context.Background(), "hits", 1); err != nil {
					t.Errorf("Increment: %v", err)
				}
			}()
		}
		wg.Wait()

		value, err := s.Get(t.Context(), "hits")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		n, err := cache.ParseCounter(value)
		if err != nil {
			t.Fatalf("ParseCounter: %v", err)
		}
		if n != workers {
			t.Fatalf("counter = %d after %d concurrent increments, want %d", n, workers, workers)
		}
	})

	t.Run("ConcurrentAddElectsOneWinner", func(t *testing.T) {
		// Add is the primitive locks are built on, so exactly one caller must
		// win a contested key.
		s := newStore(t)

		const workers = 50
		var (
			wg      sync.WaitGroup
			winners atomic.Int64
		)
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				switch err := s.Add(context.Background(), "once", []byte("v"), time.Minute); {
				case err == nil:
					winners.Add(1)
				case errors.Is(err, cache.ErrNotStored):
				default:
					t.Errorf("Add: %v", err)
				}
			}()
		}
		wg.Wait()

		if n := winners.Load(); n != 1 {
			t.Fatalf("%d callers believed they stored the key, want exactly 1", n)
		}
	})

	t.Run("Forget", func(t *testing.T) {
		s := newStore(t)
		mustPut(t, s, "k", []byte("v"), time.Minute)

		if err := s.Forget(t.Context(), "k"); err != nil {
			t.Fatalf("Forget: %v", err)
		}
		if _, err := s.Get(t.Context(), "k"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("Get after Forget = %v, want ErrNotFound", err)
		}
		if err := s.Forget(t.Context(), "k"); err != nil {
			t.Fatalf("Forget on a missing key: %v", err)
		}
	})

	t.Run("Flush", func(t *testing.T) {
		s := newStore(t)
		mustPut(t, s, "a", []byte("1"), time.Minute)
		mustPut(t, s, "b", []byte("2"), time.Minute)

		if err := s.Flush(t.Context()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		for _, k := range []string{"a", "b"} {
			if _, err := s.Get(t.Context(), k); !errors.Is(err, cache.ErrNotFound) {
				t.Fatalf("Get(%q) after Flush = %v, want ErrNotFound", k, err)
			}
		}
	})

	t.Run("TTL", func(t *testing.T) {
		s, ok := newStore(t).(cache.TTLStore)
		if !ok {
			t.Skip("store does not implement cache.TTLStore")
		}
		mustPut(t, s, "k", []byte("v"), time.Minute)
		mustPut(t, s, "forever", []byte("v"), cache.Forever)

		switch ttl, err := s.TTL(t.Context(), "k"); {
		case err != nil:
			t.Fatalf("TTL: %v", err)
		case ttl <= 0 || ttl > time.Minute:
			t.Fatalf("TTL = %v, want (0, 1m]", ttl)
		}
		if ttl, err := s.TTL(t.Context(), "forever"); err != nil || ttl != cache.Forever {
			t.Fatalf("TTL of a forever key = %v, %v; want Forever, nil", ttl, err)
		}
		if _, err := s.TTL(t.Context(), "missing"); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("TTL of a missing key = %v, want ErrNotFound", err)
		}
	})

	t.Run("Many", func(t *testing.T) {
		s, ok := newStore(t).(cache.ManyStore)
		if !ok {
			t.Skip("store does not implement cache.ManyStore")
		}
		if err := s.PutMany(t.Context(), map[string][]byte{"a": []byte("1"), "b": []byte("2")}, time.Minute); err != nil {
			t.Fatalf("PutMany: %v", err)
		}

		values, err := s.Many(t.Context(), []string{"a", "missing", "b"})
		if err != nil {
			t.Fatalf("Many: %v", err)
		}
		if len(values) != 3 {
			t.Fatalf("Many returned %d values, want 3", len(values))
		}
		if string(values[0]) != "1" || values[1] != nil || string(values[2]) != "2" {
			t.Fatalf("Many = %q, want [1 <nil> 2] in key order", values)
		}
	})

	t.Run("Lock", func(t *testing.T) {
		s, ok := newStore(t).(cache.LockStore)
		if !ok {
			t.Skip("store does not implement cache.LockStore")
		}
		ctx := t.Context()

		if ok, err := s.Acquire(ctx, "job", "alice", time.Minute); err != nil || !ok {
			t.Fatalf("Acquire on a free lock = %v, %v; want true, nil", ok, err)
		}
		if ok, err := s.Acquire(ctx, "job", "bob", time.Minute); err != nil || ok {
			t.Fatalf("Acquire on a held lock = %v, %v; want false, nil", ok, err)
		}
		if ok, err := s.Release(ctx, "job", "bob"); err != nil || ok {
			t.Fatalf("Release by the wrong owner = %v, %v; want false, nil", ok, err)
		}
		if ok, err := s.Release(ctx, "job", "alice"); err != nil || !ok {
			t.Fatalf("Release by the owner = %v, %v; want true, nil", ok, err)
		}
		if ok, err := s.Acquire(ctx, "job", "bob", time.Minute); err != nil || !ok {
			t.Fatalf("Acquire after Release = %v, %v; want true, nil", ok, err)
		}
		if err := s.ForceRelease(ctx, "job"); err != nil {
			t.Fatalf("ForceRelease: %v", err)
		}
		if ok, _ := s.Acquire(ctx, "job", "carol", time.Minute); !ok {
			t.Fatal("Acquire after ForceRelease = false, want true")
		}
	})
}

// mustPut stores a value, failing the test if the store rejects it.
func mustPut(t *testing.T, s cache.Store, key string, value []byte, ttl time.Duration) {
	t.Helper()
	if err := s.Put(context.Background(), key, value, ttl); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}

// mustAdd stores a value conditionally, failing the test if the store rejects it.
func mustAdd(t *testing.T, s cache.Store, key string, value []byte, ttl time.Duration) {
	t.Helper()
	if err := s.Add(context.Background(), key, value, ttl); err != nil {
		t.Fatalf("Add(%q): %v", key, err)
	}
}
