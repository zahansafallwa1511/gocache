package null

import (
	"context"
	"time"

	"github.com/zahansafallwa1511/gocache"
)

type Store struct{}

func New() Store { return Store{} }

func (Store) Get(context.Context, string) ([]byte, error) { return nil, cache.ErrNotFound }

func (Store) Put(context.Context, string, []byte, time.Duration) error { return nil }

func (Store) Add(context.Context, string, []byte, time.Duration) error { return nil }

func (Store) Increment(_ context.Context, _ string, delta int64) (int64, error) {
	return delta, nil
}

func (Store) Forget(context.Context, string) error { return nil }

func (Store) Flush(context.Context) error { return nil }

func (Store) TTL(context.Context, string) (time.Duration, error) { return 0, cache.ErrNotFound }

func (Store) Acquire(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}

func (Store) Release(context.Context, string, string) (bool, error) { return true, nil }

func (Store) ForceRelease(context.Context, string) error { return nil }

var (
	_ cache.TTLStore  = Store{}
	_ cache.LockStore = Store{}
)
