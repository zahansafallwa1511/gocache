// Package redisx adapts a go-redis client to the gocache Redis driver.
//
// The driver itself depends on no client, which keeps the core module free of
// dependencies but leaves every go-redis user writing the same short adapter.
// This package is that adapter:
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	c := cache.New(redisx.New(client))
//
// It is a separate module, so only programs that import it take on go-redis.
package redisx

import (
	"context"

	"github.com/redis/go-redis/v9"
	redisstore "github.com/zahansafallwa1511/gocache/redis"
)

// conn adapts a go-redis client to the one-method interface the driver needs:
// go-redis returns a *redis.Cmd holder, the driver wants (value, error), and
// Result unpacks the one into the other.
type conn struct{ client redis.UniversalClient }

// Do implements the Conn interface of the redis driver.
func (c conn) Do(ctx context.Context, args ...any) (any, error) {
	return c.client.Do(ctx, args...).Result()
}

// New returns a cache store backed by an existing go-redis client.
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	c := cache.New(redisx.New(client))
//
// It accepts any [redis.UniversalClient] — a plain client, a cluster client, a
// ring, or a failover client — so it works against a single server, a managed
// service such as AWS ElastiCache or MemoryDB, or a self-run cluster.
//
// Pass [redisstore.WithClusterMode] when the client talks to a cluster, so that
// batch reads skip the multi-key commands a cluster refuses:
//
//	c := cache.New(redisx.New(clusterClient, redisstore.WithClusterMode()))
func New(client redis.UniversalClient, opts ...redisstore.Option) *redisstore.Store {
	return redisstore.New(conn{client}, opts...)
}
