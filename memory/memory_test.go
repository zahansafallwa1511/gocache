package memory_test

import (
	"testing"

	"github.com/zahansafallwa1511/gocache"
	"github.com/zahansafallwa1511/gocache/memory"
	"github.com/zahansafallwa1511/gocache/storetest"
)

func TestStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) cache.Store {
		s := memory.New()
		t.Cleanup(func() { s.Close() })
		return s
	})
}
