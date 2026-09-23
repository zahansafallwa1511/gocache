// Package sqltest runs the cache conformance suite against real databases.
//
// It exists as its own module so that the drivers it needs — SQLite, Postgres
// and MySQL — stay out of the main module's dependency graph. Nothing here is
// meant to be imported; it is a test harness.
//
// SQLite runs everywhere, with no server, because the driver is pure Go. The
// Postgres and MySQL suites are skipped unless POSTGRES_DSN or MYSQL_DSN name a
// reachable server:
//
//	docker run -d --rm -p 5433:5432 -e POSTGRES_PASSWORD=cache postgres:16-alpine
//	POSTGRES_DSN='postgres://postgres:cache@localhost:5433/postgres?sslmode=disable' go test ./...
package sqltest
