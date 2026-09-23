package redisx

import (
	"context"

	"github.com/redis/go-redis/v9"
	redisstore "github.com/zahansafallwa1511/gocache/redis"
)

type conn struct{ client redis.UniversalClient }

func (c conn) Do(ctx context.Context, args ...any) (any, error) {
	return c.client.Do(ctx, args...).Result()
}

func New(client redis.UniversalClient) *redisstore.Store {
	return redisstore.New(conn{client})
}
