package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

var ErrLockTimeout = errors.New("cache: timed out waiting for lock")

var ErrNoLockStore = errors.New("cache: store does not support locks")

type Lock struct {
	store LockStore
	key   string
	owner string
	ttl   time.Duration
	retry time.Duration
}

type LockOption func(*Lock)

func WithOwner(owner string) LockOption {
	return func(l *Lock) { l.owner = owner }
}

func WithRetryInterval(d time.Duration) LockOption {
	return func(l *Lock) { l.retry = d }
}

func (c *Cache) Lock(key string, ttl time.Duration, opts ...LockOption) (*Lock, error) {
	s, ok := c.store.(LockStore)
	if !ok {
		return nil, ErrNoLockStore
	}
	l := &Lock{store: s, key: c.prefix + "lock:" + key, owner: randomToken(), ttl: ttl, retry: 100 * time.Millisecond}
	for _, opt := range opts {
		opt(l)
	}
	return l, nil
}

func (l *Lock) Owner() string { return l.owner }

func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	return l.store.Acquire(ctx, l.key, l.owner, l.ttl)
}

func (l *Lock) Release(ctx context.Context) error {
	_, err := l.store.Release(ctx, l.key, l.owner)
	return err
}

func (l *Lock) ForceRelease(ctx context.Context) error {
	return l.store.ForceRelease(ctx, l.key)
}

func (l *Lock) Block(ctx context.Context, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	timer := time.NewTimer(0)
	defer timer.Stop()
	if !timer.Stop() {
		<-timer.C
	}

	for {
		ok, err := l.Acquire(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ErrLockTimeout
		}
		timer.Reset(min(l.retry, remaining))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *Lock) Get(ctx context.Context, fn func(context.Context) error) (bool, error) {
	ok, err := l.Acquire(ctx)
	if err != nil || !ok {
		return false, err
	}
	defer func() { _ = l.Release(context.WithoutCancel(ctx)) }()
	return true, fn(ctx)
}

func randomToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("cache: unable to read randomness: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
