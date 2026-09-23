package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/zahansafallwa1511/gocache"
)

type Conn interface {
	Do(ctx context.Context, args ...any) (any, error)
}

type Store struct {
	conn Conn
}

func New(conn Conn) *Store { return &Store{conn: conn} }

func nilReply(reply any, err error) bool {
	if err != nil {
		return err.Error() == "redis: nil"
	}
	return reply == nil
}

func toBytes(reply any) ([]byte, error) {
	switch v := reply.(type) {
	case nil:
		return nil, cache.ErrNotFound
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cache/redis: unexpected reply type %T", reply)
	}
}

func toInt64(reply any) (int64, error) {
	switch v := reply.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("cache/redis: unexpected integer reply type %T", reply)
	}
}

func ttlArgs(args []any, ttl time.Duration) []any {
	if ttl == cache.Forever {
		return args
	}
	return append(args, "PX", strconv.FormatInt(max(ttl.Milliseconds(), 1), 10))
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	reply, err := s.conn.Do(ctx, "GET", key)
	if nilReply(reply, err) {
		return nil, cache.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("cache/redis: GET: %w", err)
	}
	return toBytes(reply)
}

func (s *Store) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return s.Forget(ctx, key)
	}
	if _, err := s.conn.Do(ctx, ttlArgs([]any{"SET", key, value}, ttl)...); err != nil {
		return fmt.Errorf("cache/redis: SET: %w", err)
	}
	return nil
}

func (s *Store) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return cache.ErrNotStored
	}
	reply, err := s.conn.Do(ctx, ttlArgs([]any{"SET", key, value, "NX"}, ttl)...)
	if nilReply(reply, err) {
		return cache.ErrNotStored
	}
	if err != nil {
		return fmt.Errorf("cache/redis: SET NX: %w", err)
	}
	return nil
}

func (s *Store) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	reply, err := s.conn.Do(ctx, "INCRBY", key, delta)
	if err != nil {
		return 0, fmt.Errorf("cache/redis: INCRBY: %w", err)
	}
	return toInt64(reply)
}

func (s *Store) Forget(ctx context.Context, key string) error {
	if _, err := s.conn.Do(ctx, "DEL", key); err != nil {
		return fmt.Errorf("cache/redis: DEL: %w", err)
	}
	return nil
}

func (s *Store) Flush(ctx context.Context) error {
	if _, err := s.conn.Do(ctx, "FLUSHDB"); err != nil {
		return fmt.Errorf("cache/redis: FLUSHDB: %w", err)
	}
	return nil
}

func (s *Store) TTL(ctx context.Context, key string) (time.Duration, error) {
	reply, err := s.conn.Do(ctx, "PTTL", key)
	if nilReply(reply, err) {
		return 0, cache.ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("cache/redis: PTTL: %w", err)
	}
	ms, err := toInt64(reply)
	if err != nil {
		return 0, err
	}
	switch ms {
	case -2:
		return 0, cache.ErrNotFound
	case -1:
		return cache.Forever, nil
	default:
		return time.Duration(ms) * time.Millisecond, nil
	}
}

func (s *Store) Many(ctx context.Context, keys []string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(keys)+1)
	args = append(args, "MGET")
	for _, k := range keys {
		args = append(args, k)
	}

	reply, err := s.conn.Do(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("cache/redis: MGET: %w", err)
	}
	items, ok := reply.([]any)
	if !ok {
		return nil, fmt.Errorf("cache/redis: unexpected MGET reply type %T", reply)
	}

	values := make([][]byte, len(keys))
	for i, item := range items {
		if i >= len(values) || item == nil {
			continue
		}
		v, err := toBytes(item)
		if err != nil {
			return nil, err
		}
		values[i] = v
	}
	return values, nil
}

func (s *Store) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	for k, v := range values {
		if err := s.Put(ctx, k, v, ttl); err != nil {
			return err
		}
	}
	return nil
}

const releaseScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`

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
	reply, err := s.conn.Do(ctx, "EVAL", releaseScript, 1, key, owner)
	if err != nil {
		return false, fmt.Errorf("cache/redis: EVAL: %w", err)
	}
	n, err := toInt64(reply)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *Store) ForceRelease(ctx context.Context, key string) error {
	return s.Forget(ctx, key)
}

var (
	_ cache.TTLStore  = (*Store)(nil)
	_ cache.ManyStore = (*Store)(nil)
	_ cache.LockStore = (*Store)(nil)
)
