// Package redis provides a Redis-backed cache store.
//
// The package deliberately depends on no Redis client. It talks to Redis through
// the one-method [Conn] interface, which any client satisfies in a few lines, so
// importing it neither picks your client nor pins its version.
//
// If you use github.com/redis/go-redis/v9, the adapter is already written:
// import github.com/zahansafallwa1511/gocache/redisx and call redisx.New. For
// any other client, implement Conn yourself:
//
//	type conn struct{ c *someclient.Client }
//
//	func (c conn) Do(ctx context.Context, args ...any) (any, error) {
//		return c.c.Do(ctx, args...)
//	}
//
//	store := redis.New(conn{client})
//
// This is the driver to choose when several processes must share one cache.
package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zahansafallwa1511/gocache"
)

// Conn is the slice of a Redis client this store needs: the ability to run one
// command and get its raw reply. A nil reply, or a reply carrying the sentinel
// error "redis: nil", means the key is absent.
type Conn interface {
	Do(ctx context.Context, args ...any) (any, error)
}

// Store is a Redis-backed [cache.Store].
type Store struct {
	conn Conn

	// noMultiKey is set once a multi-key command has been refused because its
	// keys live on different shards, after which batches are read key by key.
	noMultiKey atomic.Bool
}

// Option configures a [Store].
type Option func(*Store)

// WithClusterMode tells the store that its keys may live on different shards,
// so multi-key commands such as MGET cannot be used. Set it for a Redis Cluster,
// including AWS ElastiCache or MemoryDB with cluster mode enabled.
//
// It is a hint, not a requirement: without it the store discovers the same thing
// from the first CROSSSLOT error and adapts. Setting it up front avoids that one
// failed round trip.
func WithClusterMode() Option {
	return func(s *Store) { s.noMultiKey.Store(true) }
}

// New returns a store that issues commands over conn.
func New(conn Conn, opts ...Option) *Store {
	s := &Store{conn: conn}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// crossSlot reports whether err is Redis Cluster refusing a command whose keys
// are not all on one shard.
func crossSlot(err error) bool {
	return err != nil && strings.Contains(err.Error(), "CROSSSLOT")
}

// nilReply reports whether a reply means "no such key". Clients signal this
// either with a nil value or with a sentinel error, and the sentinel is matched
// by message so that no client-specific import is needed.
func nilReply(reply any, err error) bool {
	if err != nil {
		return err.Error() == "redis: nil"
	}
	return reply == nil
}

// toBytes normalises the string-or-bytes replies clients return.
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

// toInt64 normalises an integer reply, which clients may hand back in any of
// several types.
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

// ttlArgs appends the expiry arguments for ttl to a SET command. A sub-
// millisecond ttl is rounded up to 1ms, since PX rejects zero.
func ttlArgs(args []any, ttl time.Duration) []any {
	if ttl == cache.Forever {
		return args
	}
	return append(args, "PX", strconv.FormatInt(max(ttl.Milliseconds(), 1), 10))
}

// Get implements [cache.Store] with GET.
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

// Put implements [cache.Store] with SET.
func (s *Store) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return s.Forget(ctx, key)
	}
	if _, err := s.conn.Do(ctx, ttlArgs([]any{"SET", key, value}, ttl)...); err != nil {
		return fmt.Errorf("cache/redis: SET: %w", err)
	}
	return nil
}

// Add implements [cache.Store] with SET NX, which is atomic server-side.
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

// Increment implements [cache.Store] with INCRBY, which is atomic server-side.
func (s *Store) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	reply, err := s.conn.Do(ctx, "INCRBY", key, delta)
	if err != nil {
		return 0, fmt.Errorf("cache/redis: INCRBY: %w", err)
	}
	return toInt64(reply)
}

// Forget implements [cache.Store] with DEL.
func (s *Store) Forget(ctx context.Context, key string) error {
	if _, err := s.conn.Do(ctx, "DEL", key); err != nil {
		return fmt.Errorf("cache/redis: DEL: %w", err)
	}
	return nil
}

// Flush implements [cache.Store] with FLUSHDB. It empties the whole selected
// database, including keys this cache never wrote, so give the cache its own
// database or its own Redis instance.
//
// On a Redis Cluster a single FLUSHDB reaches one node, leaving the other shards
// untouched. Do not rely on it there: invalidate with tags, or flush each master
// through your client.
func (s *Store) Flush(ctx context.Context) error {
	if _, err := s.conn.Do(ctx, "FLUSHDB"); err != nil {
		return fmt.Errorf("cache/redis: FLUSHDB: %w", err)
	}
	return nil
}

// TTL implements [cache.TTLStore] with PTTL.
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

// Many implements [cache.ManyStore] with MGET.
//
// On a Redis Cluster, MGET only works when every key hashes to one shard, which
// cache keys generally do not. The first CROSSSLOT refusal switches this store
// to reading batches key by key for the rest of its life; [WithClusterMode]
// skips straight to that. Correctness is unaffected either way — only the number
// of round trips.
func (s *Store) Many(ctx context.Context, keys []string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if s.noMultiKey.Load() {
		return s.manyByKey(ctx, keys)
	}
	args := make([]any, 0, len(keys)+1)
	args = append(args, "MGET")
	for _, k := range keys {
		args = append(args, k)
	}

	reply, err := s.conn.Do(ctx, args...)
	if crossSlot(err) {
		s.noMultiKey.Store(true)
		return s.manyByKey(ctx, keys)
	}
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

// manyByKey reads a batch one key at a time, for deployments where the keys may
// not share a shard.
func (s *Store) manyByKey(ctx context.Context, keys []string) ([][]byte, error) {
	values := make([][]byte, len(keys))
	for i, k := range keys {
		value, err := s.Get(ctx, k)
		if errors.Is(err, cache.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

// PutMany implements [cache.ManyStore]. Redis has no MSET that carries a TTL,
// so entries are written one command at a time; with a pipelining client that
// still costs a single round trip.
func (s *Store) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	for k, v := range values {
		if err := s.Put(ctx, k, v, ttl); err != nil {
			return err
		}
	}
	return nil
}

// releaseScript deletes the lock key only if it still carries our owner token.
// Done as a script so the check and the delete are one atomic step, which is
// what stops a holder whose lock already expired from releasing its successor's.
const releaseScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`

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

// Release implements [cache.LockStore] atomically, via a Lua script.
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

// ForceRelease implements [cache.LockStore].
func (s *Store) ForceRelease(ctx context.Context, key string) error {
	return s.Forget(ctx, key)
}

var (
	_ cache.TTLStore  = (*Store)(nil)
	_ cache.ManyStore = (*Store)(nil)
	_ cache.LockStore = (*Store)(nil)
)
