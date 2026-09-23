// Package sql provides a database-backed cache store.
//
// It uses only database/sql, so any driver works. Three dialects ship with it —
// [Postgres], [MySQL] and [SQLite] — because they differ in placeholder syntax
// and in how an upsert is spelled.
//
//	store := sql.New(db, sql.Postgres)
//	if err := store.CreateTable(ctx); err != nil {
//		return err
//	}
//	c := cache.New(store)
//
// Call [Store.CreateTable] once, or run the equivalent migration yourself, and
// schedule [Store.DeleteExpired] to reclaim expired rows — this driver has no
// janitor of its own.
//
// A database cache is slower than memory or Redis and competes with your
// application for the same connection pool. It earns its place when you want
// caching without operating another service, or when entries must survive a
// restart and be shared across instances.
package sql

import (
	"context"
	databasesql "database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/zahansafallwa1511/gocache"
)

// Dialect names the SQL flavour of the target database. Dialects differ in
// placeholder syntax and in how an upsert is spelled.
type Dialect int

const (
	Postgres Dialect = iota

	MySQL

	SQLite
)

// DB is the subset of [database/sql.DB] the store uses. *sql.DB satisfies it,
// as does any wrapper with the same shape.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (databasesql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *databasesql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*databasesql.Rows, error)
	BeginTx(ctx context.Context, opts *databasesql.TxOptions) (*databasesql.Tx, error)
}

// Store is a database-backed [cache.Store].
type Store struct {
	db      DB
	dialect Dialect
	table   string
}

// Option configures a [Store].
type Option func(*Store)

// WithTable sets the table name. The default is "cache". The name is
// interpolated into statements rather than bound as a parameter, because SQL
// does not allow a placeholder there — so pass a constant, never user input.
func WithTable(name string) Option {
	return func(s *Store) { s.table = name }
}

// New returns a store backed by db.
//
//	store := sql.New(db, sql.Postgres)
//	if err := store.CreateTable(ctx); err != nil {
//		return err
//	}
func New(db DB, dialect Dialect, opts ...Option) *Store {
	s := &Store{db: db, dialect: dialect, table: "cache"}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// rebind rewrites the ? placeholders of query into the dialect's own form.
// Statements are written with ? throughout and translated here.
func (s *Store) rebind(query string) string {
	if s.dialect != Postgres {
		return query
	}
	var b strings.Builder
	n := 0
	for _, r := range query {
		if r == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// CreateTable creates the cache table and its index if they do not exist. It is
// safe to call on every boot. Run it once at startup, or manage the schema with
// your own migrations instead.
func (s *Store) CreateTable(ctx context.Context) error {
	var stmt string
	switch s.dialect {
	case MySQL:
		// MySQL has no CREATE INDEX IF NOT EXISTS, so the index is declared
		// inline where re-running the statement is harmless.
		stmt = `CREATE TABLE IF NOT EXISTS %s (
			cache_key VARCHAR(255) NOT NULL PRIMARY KEY,
			value LONGBLOB NOT NULL,
			expires_at BIGINT NOT NULL,
			KEY cache_expires_at_idx (expires_at)
		)`
	case Postgres:
		stmt = `CREATE TABLE IF NOT EXISTS %s (
			cache_key TEXT NOT NULL PRIMARY KEY,
			value BYTEA NOT NULL,
			expires_at BIGINT NOT NULL
		)`
	default:
		stmt = `CREATE TABLE IF NOT EXISTS %s (
			cache_key TEXT NOT NULL PRIMARY KEY,
			value BLOB NOT NULL,
			expires_at INTEGER NOT NULL
		)`
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(stmt, s.table)); err != nil {
		return fmt.Errorf("cache/sql: create table: %w", err)
	}
	if s.dialect == MySQL {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %s_expires_at_idx ON %s (expires_at)`, s.table, s.table)); err != nil {
		return fmt.Errorf("cache/sql: create index: %w", err)
	}
	return nil
}

// expiresAt encodes a ttl as Unix milliseconds, with zero meaning no expiry.
func expiresAt(ttl time.Duration) int64 {
	if ttl == cache.Forever {
		return 0
	}
	return time.Now().Add(ttl).UnixMilli()
}

// live is the SQL predicate matching rows that have not expired.
const live = `(expires_at = 0 OR expires_at > ?)`

// Get implements [cache.Store].
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	query := s.rebind(fmt.Sprintf(`SELECT value FROM %s WHERE cache_key = ? AND `+live, s.table))

	var value []byte
	err := s.db.QueryRowContext(ctx, query, key, time.Now().UnixMilli()).Scan(&value)
	if errors.Is(err, databasesql.ErrNoRows) {
		return nil, cache.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("cache/sql: get: %w", err)
	}
	return value, nil
}

// upsert writes a row, replacing any existing one, in the dialect's spelling.
func (s *Store) upsert(ctx context.Context, db DB, key string, value []byte, expires int64) error {
	var clause string
	switch s.dialect {
	case MySQL:
		clause = `ON DUPLICATE KEY UPDATE value = VALUES(value), expires_at = VALUES(expires_at)`
	default:
		clause = `ON CONFLICT (cache_key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at`
	}
	query := s.rebind(fmt.Sprintf(
		`INSERT INTO %s (cache_key, value, expires_at) VALUES (?, ?, ?) %s`, s.table, clause))

	if _, err := db.ExecContext(ctx, query, key, value, expires); err != nil {
		return fmt.Errorf("cache/sql: put: %w", err)
	}
	return nil
}

// Put implements [cache.Store].
func (s *Store) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return s.Forget(ctx, key)
	}
	return s.upsert(ctx, s.db, key, value, expiresAt(ttl))
}

// Add implements [cache.Store]. It first tries a plain insert and, if the row
// already exists, overwrites it only when the existing entry has expired — so
// two racing callers can never both believe they stored the value.
func (s *Store) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return cache.ErrNotStored
	}
	expires := expiresAt(ttl)

	insert := s.rebind(fmt.Sprintf(
		`INSERT INTO %s (cache_key, value, expires_at) VALUES (?, ?, ?)`, s.table))
	if _, err := s.db.ExecContext(ctx, insert, key, value, expires); err == nil {
		return nil
	}

	update := s.rebind(fmt.Sprintf(
		`UPDATE %s SET value = ?, expires_at = ? WHERE cache_key = ? AND expires_at <> 0 AND expires_at <= ?`,
		s.table))
	res, err := s.db.ExecContext(ctx, update, value, expires, key, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("cache/sql: add: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cache/sql: add: %w", err)
	}
	if n == 0 {
		return cache.ErrNotStored
	}
	return nil
}

// Increment implements [cache.Store] inside a transaction, taking a row lock
// where the dialect supports one, so concurrent writers cannot lose an update.
func (s *Store) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("cache/sql: begin: %w", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`SELECT value, expires_at FROM %s WHERE cache_key = ? AND `+live, s.table)
	if s.dialect != SQLite {
		query += ` FOR UPDATE`
	}

	var (
		raw     []byte
		expires int64
		current int64
	)
	switch err := tx.QueryRowContext(ctx, s.rebind(query), key, time.Now().UnixMilli()).Scan(&raw, &expires); {
	case err == nil:
		if current, err = cache.ParseCounter(raw); err != nil {
			return 0, err
		}
	case !errors.Is(err, databasesql.ErrNoRows):
		return 0, fmt.Errorf("cache/sql: increment: %w", err)
	}

	current += delta
	if err := s.upsert(ctx, txDB{tx}, key, cache.FormatCounter(current), expires); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("cache/sql: commit: %w", err)
	}
	return current, nil
}

// Forget implements [cache.Store].
func (s *Store) Forget(ctx context.Context, key string) error {
	query := s.rebind(fmt.Sprintf(`DELETE FROM %s WHERE cache_key = ?`, s.table))
	if _, err := s.db.ExecContext(ctx, query, key); err != nil {
		return fmt.Errorf("cache/sql: forget: %w", err)
	}
	return nil
}

// Flush implements [cache.Store] by deleting every row in the table.
func (s *Store) Flush(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, s.table)); err != nil {
		return fmt.Errorf("cache/sql: flush: %w", err)
	}
	return nil
}

// DeleteExpired removes expired rows and reports how many it deleted.
//
// Unlike the other drivers, a database cache has no janitor: expired rows are
// invisible to reads but stay on disk until this runs. Call it from a scheduled
// job, or the table grows without bound.
func (s *Store) DeleteExpired(ctx context.Context) (int64, error) {
	query := s.rebind(fmt.Sprintf(
		`DELETE FROM %s WHERE expires_at <> 0 AND expires_at <= ?`, s.table))
	res, err := s.db.ExecContext(ctx, query, time.Now().UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("cache/sql: delete expired: %w", err)
	}
	return res.RowsAffected()
}

// TTL implements [cache.TTLStore].
func (s *Store) TTL(ctx context.Context, key string) (time.Duration, error) {
	query := s.rebind(fmt.Sprintf(`SELECT expires_at FROM %s WHERE cache_key = ? AND `+live, s.table))

	var expires int64
	err := s.db.QueryRowContext(ctx, query, key, time.Now().UnixMilli()).Scan(&expires)
	if errors.Is(err, databasesql.ErrNoRows) {
		return 0, cache.ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("cache/sql: ttl: %w", err)
	}
	if expires == 0 {
		return cache.Forever, nil
	}
	return time.Until(time.UnixMilli(expires)), nil
}

// Many implements [cache.ManyStore] with a single IN query.
func (s *Store) Many(ctx context.Context, keys []string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	args := make([]any, 0, len(keys)+1)
	placeholders := make([]string, len(keys))
	for i, k := range keys {
		placeholders[i] = "?"
		args = append(args, k)
	}
	args = append(args, time.Now().UnixMilli())

	query := s.rebind(fmt.Sprintf(`SELECT cache_key, value FROM %s WHERE cache_key IN (%s) AND `+live,
		s.table, strings.Join(placeholders, ", ")))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("cache/sql: many: %w", err)
	}
	defer rows.Close()

	found := make(map[string][]byte, len(keys))
	for rows.Next() {
		var (
			key   string
			value []byte
		)
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("cache/sql: many: %w", err)
		}
		found[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cache/sql: many: %w", err)
	}

	values := make([][]byte, len(keys))
	for i, k := range keys {
		values[i] = found[k]
	}
	return values, nil
}

// PutMany implements [cache.ManyStore] in one transaction.
func (s *Store) PutMany(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("cache/sql: begin: %w", err)
	}
	defer tx.Rollback()

	expires := expiresAt(ttl)
	for k, v := range values {
		if err := s.upsert(ctx, txDB{tx}, k, v, expires); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cache/sql: commit: %w", err)
	}
	return nil
}

// Acquire implements [cache.LockStore] on top of the atomic Add.
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

// Release implements [cache.LockStore] with an owner-checked delete.
func (s *Store) Release(ctx context.Context, key, owner string) (bool, error) {
	query := s.rebind(fmt.Sprintf(`DELETE FROM %s WHERE cache_key = ? AND value = ?`, s.table))
	res, err := s.db.ExecContext(ctx, query, key, []byte(owner))
	if err != nil {
		return false, fmt.Errorf("cache/sql: release: %w", err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// ForceRelease implements [cache.LockStore].
func (s *Store) ForceRelease(ctx context.Context, key string) error {
	return s.Forget(ctx, key)
}

// txDB adapts a transaction to the DB interface, so statement builders are
// shared between the transactional and non-transactional paths.
type txDB struct{ tx *databasesql.Tx }

func (t txDB) ExecContext(ctx context.Context, q string, args ...any) (databasesql.Result, error) {
	return t.tx.ExecContext(ctx, q, args...)
}

func (t txDB) QueryRowContext(ctx context.Context, q string, args ...any) *databasesql.Row {
	return t.tx.QueryRowContext(ctx, q, args...)
}

func (t txDB) QueryContext(ctx context.Context, q string, args ...any) (*databasesql.Rows, error) {
	return t.tx.QueryContext(ctx, q, args...)
}

func (t txDB) BeginTx(context.Context, *databasesql.TxOptions) (*databasesql.Tx, error) {
	return nil, errors.New("cache/sql: nested transactions are not supported")
}

var (
	_ cache.TTLStore  = (*Store)(nil)
	_ cache.ManyStore = (*Store)(nil)
	_ cache.LockStore = (*Store)(nil)
)
