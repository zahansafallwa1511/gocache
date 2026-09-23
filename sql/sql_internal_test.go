package sql

import (
	"strings"
	"testing"
)

func TestRebindRewritesPlaceholdersForPostgresOnly(t *testing.T) {
	query := `SELECT value FROM cache WHERE cache_key = ? AND expires_at > ?`

	if got := New(nil, Postgres).rebind(query); !strings.Contains(got, "$1") || !strings.Contains(got, "$2") {
		t.Fatalf("Postgres rebind = %q, want numbered placeholders", got)
	}
	for _, d := range []Dialect{MySQL, SQLite} {
		if got := New(nil, d).rebind(query); got != query {
			t.Fatalf("dialect %v rewrote the query: %q", d, got)
		}
	}
}

func TestUpsertClauseMatchesDialect(t *testing.T) {

	s := New(nil, SQLite, WithTable("app_cache"))
	if s.table != "app_cache" {
		t.Fatalf("WithTable = %q, want app_cache", s.table)
	}
	if New(nil, SQLite).table != "cache" {
		t.Fatal("default table name should be cache")
	}
}
