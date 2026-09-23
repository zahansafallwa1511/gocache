package redis_test

import (
	"testing"

	"github.com/zahansafallwa1511/gocache"
	redisstore "github.com/zahansafallwa1511/gocache/redis"
	"github.com/zahansafallwa1511/gocache/storetest"
)

func TestStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) cache.Store {
		return redisstore.New(newFakeConn())
	})
}
