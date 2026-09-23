package sqltest_test

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/zahansafallwa1511/gocache"
	sqlstore "github.com/zahansafallwa1511/gocache/sql"
	"github.com/zahansafallwa1511/gocache/storetest"
)

// serverDB opens a connection named by an environment variable, skipping the
// test when it is unset so the suite stays runnable without any server.
func serverDB(t *testing.T, env, driver string) *sql.DB {
	t.Helper()

	dsn := os.Getenv(env)
	if dsn == "" {
		t.Skipf("set %s to run the %s tests", env, driver)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s: %v", driver, err)
	}
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("connect to %s: %v", driver, err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func runServerSuite(t *testing.T, db *sql.DB, dialect sqlstore.Dialect) {
	t.Helper()

	storetest.Run(t, func(t *testing.T) cache.Store {
		// Each subtest gets its own table, so cases cannot see each other's keys.
		table := fmt.Sprintf("cache_%d", time.Now().UnixNano())
		store := sqlstore.New(db, dialect, sqlstore.WithTable(table))
		if err := store.CreateTable(t.Context()); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) })
		return store
	})
}

func TestPostgres(t *testing.T) {
	runServerSuite(t, serverDB(t, "POSTGRES_DSN", "postgres"), sqlstore.Postgres)
}

func TestMySQL(t *testing.T) {
	runServerSuite(t, serverDB(t, "MYSQL_DSN", "mysql"), sqlstore.MySQL)
}

// TestPostgresConcurrentIncrement is the case a single-process test cannot
// prove: many connections incrementing one counter must not lose an update.
func TestPostgresConcurrentIncrement(t *testing.T) {
	db := serverDB(t, "POSTGRES_DSN", "postgres")
	table := fmt.Sprintf("cache_inc_%d", time.Now().UnixNano())
	store := sqlstore.New(db, sqlstore.Postgres, sqlstore.WithTable(table))
	if err := store.CreateTable(t.Context()); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) })

	const workers = 25
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.Increment(t.Context(), "hits", 1); err != nil {
				t.Errorf("Increment: %v", err)
			}
		}()
	}
	wg.Wait()

	value, err := store.Get(t.Context(), "hits")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	n, err := cache.ParseCounter(value)
	if err != nil {
		t.Fatalf("ParseCounter: %v", err)
	}
	if n != workers {
		t.Fatalf("counter = %d after %d concurrent increments, want %d", n, workers, workers)
	}
}

// TestPostgresLockAcrossConnections proves the lock is exclusive between
// separate connections, not merely within one process's memory.
func TestPostgresLockAcrossConnections(t *testing.T) {
	db := serverDB(t, "POSTGRES_DSN", "postgres")
	table := fmt.Sprintf("cache_lock_%d", time.Now().UnixNano())
	store := sqlstore.New(db, sqlstore.Postgres, sqlstore.WithTable(table))
	if err := store.CreateTable(t.Context()); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) })

	c := cache.New(store)
	first, err := c.Lock("import", time.Minute)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	second, _ := c.Lock("import", time.Minute)

	if ok, err := first.Acquire(t.Context()); err != nil || !ok {
		t.Fatalf("Acquire = %v, %v; want true, nil", ok, err)
	}
	if ok, _ := second.Acquire(t.Context()); ok {
		t.Fatal("two holders acquired the same lock")
	}
	if err := second.Release(t.Context()); err != nil {
		t.Fatalf("Release by a non-owner: %v", err)
	}
	if ok, _ := first.Acquire(t.Context()); ok {
		t.Fatal("a non-owner released the lock")
	}
}
