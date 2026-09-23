package redis_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zahansafallwa1511/gocache"
	redisstore "github.com/zahansafallwa1511/gocache/redis"
)

// crossSlotConn refuses MGET the way Redis Cluster does when a command's keys do
// not all live on one shard, and counts how often it is asked.
type crossSlotConn struct {
	*fakeConn
	mgets atomic.Int64
	gets  atomic.Int64
}

func (c *crossSlotConn) Do(ctx context.Context, args ...any) (any, error) {
	switch strings.ToUpper(args[0].(string)) {
	case "MGET":
		c.mgets.Add(1)
		return nil, errors.New("CROSSSLOT Keys in request don't hash to the same slot")
	case "GET":
		c.gets.Add(1)
	}
	return c.fakeConn.Do(ctx, args...)
}

func TestManyFallsBackWhenKeysSpanShards(t *testing.T) {
	conn := &crossSlotConn{fakeConn: newFakeConn()}
	store := redisstore.New(conn)
	ctx := t.Context()

	if err := store.Put(ctx, "a", []byte("1"), time.Minute); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Put(ctx, "b", []byte("2"), time.Minute); err != nil {
		t.Fatalf("Put: %v", err)
	}

	values, err := store.Many(ctx, []string{"a", "missing", "b"})
	if err != nil {
		t.Fatalf("Many across shards: %v", err)
	}
	if string(values[0]) != "1" || values[1] != nil || string(values[2]) != "2" {
		t.Fatalf("Many = %q, want [1 <nil> 2]", values)
	}

	if _, err := store.Many(ctx, []string{"a", "b"}); err != nil {
		t.Fatalf("second Many: %v", err)
	}
	if n := conn.mgets.Load(); n != 1 {
		t.Fatalf("MGET attempted %d times, want 1 — the store should stop trying after CROSSSLOT", n)
	}
}

func TestClusterModeSkipsMultiKeyCommands(t *testing.T) {
	conn := &crossSlotConn{fakeConn: newFakeConn()}
	store := redisstore.New(conn, redisstore.WithClusterMode())
	ctx := t.Context()

	if err := store.Put(ctx, "a", []byte("1"), time.Minute); err != nil {
		t.Fatalf("Put: %v", err)
	}

	values, err := store.Many(ctx, []string{"a"})
	if err != nil {
		t.Fatalf("Many: %v", err)
	}
	if string(values[0]) != "1" {
		t.Fatalf("Many = %q, want [1]", values)
	}
	if n := conn.mgets.Load(); n != 0 {
		t.Fatalf("MGET attempted %d times in cluster mode, want 0", n)
	}
}

func TestCacheManyWorksOnACluster(t *testing.T) {
	conn := &crossSlotConn{fakeConn: newFakeConn()}
	c := cache.New(redisstore.New(conn))
	ctx := t.Context()

	if err := c.SetMany(ctx, map[string]any{"a": 1, "b": 2}, time.Minute); err != nil {
		t.Fatalf("SetMany: %v", err)
	}

	var a, b int
	found, err := c.Many(ctx, []string{"a", "b"}, []any{&a, &b})
	if err != nil {
		t.Fatalf("Many: %v", err)
	}
	if len(found) != 2 || a != 1 || b != 2 {
		t.Fatalf("Many = %d, %d, found %v", a, b, found)
	}
}
