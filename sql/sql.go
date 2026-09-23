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

type Dialect int

const (
	Postgres Dialect = iota

	MySQL

	SQLite
)

type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (databasesql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *databasesql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*databasesql.Rows, error)
	BeginTx(ctx context.Context, opts *databasesql.TxOptions) (*databasesql.Tx, error)
}

type Store struct {
	db      DB
	dialect Dialect
	table   string
}

type Option func(*Store)

func WithTable(name string) Option {
	return func(s *Store) { s.table = name }
}

func New(db DB, dialect Dialect, opts ...Option) *Store {
	s := &Store{db: db, dialect: dialect, table: "cache"}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

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

func (s *Store) CreateTable(ctx context.Context) error {
	var stmt string
	switch s.dialect {
	case MySQL:
		stmt = `CREATE TABLE IF NOT EXISTS %s (
			cache_key VARCHAR(255) NOT NULL PRIMARY KEY,
			value LONGBLOB NOT NULL,
			expires_at BIGINT NOT NULL
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
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %s_expires_at_idx ON %s (expires_at)`, s.table, s.table)); err != nil {
		return fmt.Errorf("cache/sql: create index: %w", err)
	}
	return nil
}

func expiresAt(ttl time.Duration) int64 {
	if ttl == cache.Forever {
		return 0
	}
	return time.Now().Add(ttl).UnixMilli()
}

const live = `(expires_at = 0 OR expires_at > ?)`

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

func (s *Store) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return s.Forget(ctx, key)
	}
	return s.upsert(ctx, s.db, key, value, expiresAt(ttl))
}

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

func (s *Store) Forget(ctx context.Context, key string) error {
	query := s.rebind(fmt.Sprintf(`DELETE FROM %s WHERE cache_key = ?`, s.table))
	if _, err := s.db.ExecContext(ctx, query, key); err != nil {
		return fmt.Errorf("cache/sql: forget: %w", err)
	}
	return nil
}

func (s *Store) Flush(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, s.table)); err != nil {
		return fmt.Errorf("cache/sql: flush: %w", err)
	}
	return nil
}

func (s *Store) DeleteExpired(ctx context.Context) (int64, error) {
	query := s.rebind(fmt.Sprintf(
		`DELETE FROM %s WHERE expires_at <> 0 AND expires_at <= ?`, s.table))
	res, err := s.db.ExecContext(ctx, query, time.Now().UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("cache/sql: delete expired: %w", err)
	}
	return res.RowsAffected()
}

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

func (s *Store) Release(ctx context.Context, key, owner string) (bool, error) {
	query := s.rebind(fmt.Sprintf(`DELETE FROM %s WHERE cache_key = ? AND value = ?`, s.table))
	res, err := s.db.ExecContext(ctx, query, key, []byte(owner))
	if err != nil {
		return false, fmt.Errorf("cache/sql: release: %w", err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *Store) ForceRelease(ctx context.Context, key string) error {
	return s.Forget(ctx, key)
}

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
