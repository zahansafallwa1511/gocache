package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

// ErrLockTimeout is returned by [Lock.Block] when the lock could not be acquired
// within the given wait.
var ErrLockTimeout = errors.New("cache: timed out waiting for lock")

// ErrNoLockStore is returned by [Cache.Lock] when the underlying store cannot
// provide atomic locks.
var ErrNoLockStore = errors.New("cache: store does not support locks")

// Lock is an atomic lock backed by the cache store, held across processes.
//
// A Lock always carries a ttl, so a holder that crashes cannot block the key
// forever. That makes the ttl a real deadline rather than a formality: if the
// work outlives it, another process may acquire the lock while the first still
// believes it holds one. Size the ttl above the worst-case duration of the work,
// and treat the lock as advisory.
type Lock struct {
	store LockStore
	key   string
	owner string
	ttl   time.Duration
	retry time.Duration
}

// LockOption configures a [Lock].
type LockOption func(*Lock)

// WithOwner sets the lock's owner token, which identifies the holder. Two Locks
// sharing an owner can release each other's hold, which is how a job can hand a
// lock to its own retry. The default is a fresh random token.
func WithOwner(owner string) LockOption {
	return func(l *Lock) { l.owner = owner }
}

// WithRetryInterval sets how often [Lock.Block] re-attempts acquisition. The
// default is 100ms.
func WithRetryInterval(d time.Duration) LockOption {
	return func(l *Lock) { l.retry = d }
}

// Lock returns a lock named key, held for at most ttl once acquired. The
// underlying store must implement [LockStore] — memory, file, redis, sql and
// null all do — and [ErrNoLockStore] is returned when it does not.
//
//	lock, err := c.Lock("rebuild-index", 5*time.Minute)
//	if err != nil {
//		return err
//	}
//	ran, err := lock.Get(ctx, rebuildIndex)
//
// Calling Lock does not acquire anything; it describes the lock. Lock keys live
// in their own namespace and are unaffected by tags.
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

// Owner returns the token identifying the holder of this lock.
func (l *Lock) Owner() string { return l.owner }

// Acquire attempts to take the lock once, without waiting, and reports whether
// it is now held. Release it when the work is done.
func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	return l.store.Acquire(ctx, l.key, l.owner, l.ttl)
}

// Release frees the lock if this owner still holds it. Releasing a lock that
// expired and was retaken by someone else does nothing, which is what stops a
// slow holder from freeing its successor's lock.
func (l *Lock) Release(ctx context.Context) error {
	_, err := l.store.Release(ctx, l.key, l.owner)
	return err
}

// ForceRelease frees the lock whoever holds it. Reserve it for administrative
// recovery from a holder that will never come back.
func (l *Lock) ForceRelease(ctx context.Context) error {
	return l.store.ForceRelease(ctx, l.key)
}

// Block waits up to wait for the lock, retrying at the configured interval. It
// returns [ErrLockTimeout] if the deadline passes first, and abandons the wait if
// ctx is cancelled.
//
//	if err := lock.Block(ctx, 10*time.Second); err != nil {
//		return err
//	}
//	defer lock.Release(ctx)
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

// Get acquires the lock, runs fn, and releases it afterwards, reporting whether
// fn ran. If the lock is held elsewhere it returns false without calling fn.
//
// The lock is released even if fn fails, using a context detached from ctx so
// that a cancelled request still gives the lock back.
func (l *Lock) Get(ctx context.Context, fn func(context.Context) error) (bool, error) {
	ok, err := l.Acquire(ctx)
	if err != nil || !ok {
		return false, err
	}
	defer func() { _ = l.Release(context.WithoutCancel(ctx)) }()
	return true, fn(ctx)
}

// randomToken returns a 128-bit hex token, used for lock owners and tag
// versions. It panics only if the system entropy source fails, which a program
// cannot meaningfully continue past.
func randomToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("cache: unable to read randomness: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
