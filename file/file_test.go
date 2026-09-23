package file_test

import (
	"testing"

	"github.com/zahansafallwa1511/gocache"
	"github.com/zahansafallwa1511/gocache/file"
	"github.com/zahansafallwa1511/gocache/storetest"
)

func TestStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) cache.Store {
		s, err := file.New(t.TempDir())
		if err != nil {
			t.Fatalf("file.New: %v", err)
		}
		return s
	})
}
