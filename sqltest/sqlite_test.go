package sqltest_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/zahansafallwa1511/gocache"
	sqlstore "github.com/zahansafallwa1511/gocache/sql"
	"github.com/zahansafallwa1511/gocache/storetest"
	_ "modernc.org/sqlite"
)

func sqliteDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "cache.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	// One writer at a time: SQLite serialises writes, and the conformance suite
	// runs transactions that would otherwise collide as SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestSQLite(t *testing.T) {
	storetest.Run(t, func(t *testing.T) cache.Store {
		store := sqlstore.New(sqliteDB(t), sqlstore.SQLite)
		if err := store.CreateTable(t.Context()); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		return store
	})
}

func TestSQLiteDeleteExpired(t *testing.T) {
	store := sqlstore.New(sqliteDB(t), sqlstore.SQLite)
	ctx := t.Context()
	if err := store.CreateTable(ctx); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	if err := store.Put(ctx, "gone", []byte("v"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Put(ctx, "kept", []byte("v"), cache.Forever); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Put(ctx, "expired", []byte("v"), 1); err != nil {
		t.Fatalf("Put: %v", err)
	}

	n, err := store.DeleteExpired(ctx)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if n != 1 {
		t.Fatalf("DeleteExpired removed %d rows, want 1", n)
	}
	if _, err := store.Get(ctx, "kept"); err != nil {
		t.Fatalf("DeleteExpired removed a live row: %v", err)
	}
}

func TestSQLiteCreateTableIsIdempotent(t *testing.T) {
	store := sqlstore.New(sqliteDB(t), sqlstore.SQLite)
	for range 2 {
		if err := store.CreateTable(t.Context()); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
	}
}
