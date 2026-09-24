// Package file provides a filesystem-backed cache store.
//
// Each entry is one file under a root directory, named by a hash of its key and
// fanned out over two levels of subdirectories. Writes are atomic — the bytes go
// to a temporary file which is then renamed over the target — so the store is
// safe to share between processes on one machine, and survives restarts.
//
//	store, err := file.New("/var/cache/myapp")
//	if err != nil {
//		return err
//	}
//	c := cache.New(store)
//
// There is no janitor: expired files are removed when read, so keys that are
// written and never read again occupy disk until Flush. Sweep them with a
// periodic job if that matters.
package file

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zahansafallwa1511/gocache"
)

// headerSize is the fixed prefix of every entry file: the expiry as Unix
// milliseconds, big-endian, or zero for an entry that never expires.
const headerSize = 8

// Store is a filesystem [cache.Store]. It is safe for concurrent use within a
// process and between processes on the same machine.
type Store struct {
	root string
	perm fs.FileMode

	mu sync.Mutex
}

// Option configures a [Store].
type Option func(*Store)

// WithFileMode sets the permissions of the files written. The default is 0600,
// readable only by the user running the process. Widen it only if another user
// must read the cache, remembering that cached values are often sensitive.
func WithFileMode(perm fs.FileMode) Option {
	return func(s *Store) { s.perm = perm }
}

// New returns a store rooted at dir, creating the directory if needed.
//
//	store, err := file.New("/var/cache/myapp")
func New(dir string, opts ...Option) (*Store, error) {
	s := &Store{root: dir, perm: 0o600}
	for _, opt := range opts {
		opt(s)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("cache/file: create root: %w", err)
	}
	return s, nil
}

// path maps a key to its file: a hash, fanned out over two levels of
// subdirectories so that a large cache does not produce one enormous directory.
func (s *Store) path(key string) string {
	sum := gocacheHash(key)
	return filepath.Join(s.root, sum[0:2], sum[2:4], sum)
}

// read returns the payload and deadline stored at key, expiring it if due.
func (s *Store) read(key string) ([]byte, time.Time, error) {
	data, err := os.ReadFile(s.path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, time.Time{}, cache.ErrNotFound
	}
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("cache/file: read: %w", err)
	}
	if len(data) < headerSize {
		return nil, time.Time{}, cache.ErrNotFound
	}

	var expiresAt time.Time
	if ms := int64(binary.BigEndian.Uint64(data[:headerSize])); ms != 0 {
		expiresAt = time.UnixMilli(ms)
		if time.Now().After(expiresAt) {
			_ = os.Remove(s.path(key))
			return nil, time.Time{}, cache.ErrNotFound
		}
	}
	return data[headerSize:], expiresAt, nil
}

// writeTemp writes a complete entry to a fresh temporary file beside its final
// path and returns that file's name. The caller decides how to publish it:
// rename to overwrite, or link to claim the name only if it is free.
func (s *Store) writeTemp(path string, value []byte, expiresAt time.Time) (string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("cache/file: create bucket: %w", err)
	}

	buf := make([]byte, headerSize+len(value))
	if !expiresAt.IsZero() {
		binary.BigEndian.PutUint64(buf[:headerSize], uint64(expiresAt.UnixMilli()))
	}
	copy(buf[headerSize:], value)

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return "", fmt.Errorf("cache/file: create temp: %w", err)
	}
	if _, err := tmp.Write(buf); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("cache/file: write: %w", err)
	}
	if err := tmp.Chmod(s.perm); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("cache/file: chmod: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return "", fmt.Errorf("cache/file: close: %w", err)
	}
	return tmp.Name(), nil
}

// write stores value atomically: the bytes go to a temporary file in the same
// directory, which is then renamed over the target. A reader therefore sees
// either the old entry or the new one, never a half-written file.
func (s *Store) write(key string, value []byte, expiresAt time.Time) error {
	path := s.path(key)
	tmp, err := s.writeTemp(path, value, expiresAt)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp) }()

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("cache/file: rename: %w", err)
	}
	return nil
}

// expiry converts a ttl to a deadline, with the zero time meaning no expiry.
func expiry(ttl time.Duration) time.Time {
	if ttl == cache.Forever {
		return time.Time{}
	}
	return time.Now().Add(ttl)
}

// Get implements [cache.Store].
func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	value, _, err := s.read(key)
	return value, err
}

// Put implements [cache.Store].
func (s *Store) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return s.Forget(ctx, key)
	}
	return s.write(key, value, expiry(ttl))
}

// addAttempts bounds the retry loop in Add. Each retry means another process
// replaced an expired entry a moment before we did.
const addAttempts = 3

// Add implements [cache.Store].
//
// The entry is written to a temporary file and published with link, which fails
// if the name is already taken. That is what makes the check and the write one
// atomic step across processes — creating the file first and writing afterwards
// would leave a window in which a second caller also believes it won.
func (s *Store) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return cache.ErrNotStored
	}

	path := s.path(key)
	tmp, err := s.writeTemp(path, value, expiry(ttl))
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp) }()

	for range addAttempts {
		switch err := os.Link(tmp, path); {
		case err == nil:
			return nil
		case !errors.Is(err, fs.ErrExist):
			return fmt.Errorf("cache/file: link: %w", err)
		}

		// The name is taken. If what is there is live, the key belongs to
		// somebody else; if it has expired, read has already removed it and the
		// next attempt can claim the name.
		if _, _, err := s.read(key); err == nil {
			return cache.ErrNotStored
		} else if !errors.Is(err, cache.ErrNotFound) {
			return err
		}
	}
	return cache.ErrNotStored
}

// Increment implements [cache.Store]. The read-modify-write is guarded by a
// mutex within this process; across processes two simultaneous increments can
// lose an update, so use the redis or sql driver if counters must be exact in a
// multi-process deployment.
func (s *Store) Increment(_ context.Context, key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		current   int64
		expiresAt time.Time
	)
	switch value, exp, err := s.read(key); {
	case err == nil:
		n, err := cache.ParseCounter(value)
		if err != nil {
			return 0, err
		}
		current, expiresAt = n, exp
	case !errors.Is(err, cache.ErrNotFound):
		return 0, err
	}

	current += delta
	return current, s.write(key, cache.FormatCounter(current), expiresAt)
}

// Forget implements [cache.Store].
func (s *Store) Forget(_ context.Context, key string) error {
	err := os.Remove(s.path(key))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("cache/file: remove: %w", err)
	}
	return nil
}

// Flush implements [cache.Store]. It empties the root directory but keeps the
// directory itself.
func (s *Store) Flush(context.Context) error {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return fmt.Errorf("cache/file: read root: %w", err)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(s.root, e.Name())); err != nil {
			return fmt.Errorf("cache/file: flush: %w", err)
		}
	}
	return nil
}

// TTL implements [cache.TTLStore].
func (s *Store) TTL(_ context.Context, key string) (time.Duration, error) {
	_, expiresAt, err := s.read(key)
	if err != nil {
		return 0, err
	}
	if expiresAt.IsZero() {
		return cache.Forever, nil
	}
	return time.Until(expiresAt), nil
}

// Acquire implements [cache.LockStore].
func (s *Store) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	switch err := s.Add(ctx, key, []byte(owner), ttl); {
	case err == nil:
		return true, nil
	case errors.Is(err, cache.ErrNotStored):
		return false, nil
	default:
		return false, err
	}
}

// Release implements [cache.LockStore].
func (s *Store) Release(ctx context.Context, key, owner string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, _, err := s.read(key)
	if errors.Is(err, cache.ErrNotFound) || (err == nil && string(value) != owner) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, s.Forget(ctx, key)
}

// ForceRelease implements [cache.LockStore].
func (s *Store) ForceRelease(ctx context.Context, key string) error {
	return s.Forget(ctx, key)
}

var (
	_ cache.TTLStore  = (*Store)(nil)
	_ cache.LockStore = (*Store)(nil)
)
