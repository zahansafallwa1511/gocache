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

const headerSize = 8

type Store struct {
	root string
	perm fs.FileMode

	mu sync.Mutex
}

type Option func(*Store)

func WithFileMode(perm fs.FileMode) Option {
	return func(s *Store) { s.perm = perm }
}

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

func (s *Store) path(key string) string {
	sum := gocacheHash(key)
	return filepath.Join(s.root, sum[0:2], sum[2:4], sum)
}

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

func (s *Store) write(key string, value []byte, expiresAt time.Time) error {
	path := s.path(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("cache/file: create bucket: %w", err)
	}

	buf := make([]byte, headerSize+len(value))
	if !expiresAt.IsZero() {
		binary.BigEndian.PutUint64(buf[:headerSize], uint64(expiresAt.UnixMilli()))
	}
	copy(buf[headerSize:], value)

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("cache/file: create temp: %w", err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		return fmt.Errorf("cache/file: write: %w", err)
	}
	if err := tmp.Chmod(s.perm); err != nil {
		tmp.Close()
		return fmt.Errorf("cache/file: chmod: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cache/file: close: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("cache/file: rename: %w", err)
	}
	return nil
}

func expiry(ttl time.Duration) time.Time {
	if ttl == cache.Forever {
		return time.Time{}
	}
	return time.Now().Add(ttl)
}

func (s *Store) Get(_ context.Context, key string) ([]byte, error) {
	value, _, err := s.read(key)
	return value, err
}

func (s *Store) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return s.Forget(ctx, key)
	}
	return s.write(key, value, expiry(ttl))
}

func (s *Store) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return cache.ErrNotStored
	}

	path := s.path(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("cache/file: create bucket: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, s.perm)
	if errors.Is(err, fs.ErrExist) {

		if _, _, err := s.read(key); err == nil {
			return cache.ErrNotStored
		}
		return s.write(key, value, expiry(ttl))
	}
	if err != nil {
		return fmt.Errorf("cache/file: create: %w", err)
	}
	f.Close()
	_ = os.Remove(path)
	return s.write(key, value, expiry(ttl))
}

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

func (s *Store) Forget(_ context.Context, key string) error {
	err := os.Remove(s.path(key))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("cache/file: remove: %w", err)
	}
	return nil
}

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

func (s *Store) ForceRelease(ctx context.Context, key string) error {
	return s.Forget(ctx, key)
}

var (
	_ cache.TTLStore  = (*Store)(nil)
	_ cache.LockStore = (*Store)(nil)
)
