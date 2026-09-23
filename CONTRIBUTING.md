# Contributing

Thanks for considering a contribution.

## Getting set up

The repository holds three modules: the dependency-free core, plus `redisx` and
`sqltest`, which need third-party drivers. A workspace ties them together for
local development:

```sh
go work init . ./redisx ./sqltest
go build ./...
```

`go.work` is deliberately not committed — it would break consumers.

## Running the tests

```sh
go test -race ./...          # core, no servers needed
cd sqltest && go test ./...  # SQLite runs with no server
```

Tests against real servers are skipped unless you point them at one:

```sh
docker run -d --rm -p 6399:6379 redis:7-alpine
docker run -d --rm -p 55432:5432 -e POSTGRES_PASSWORD=cache postgres:16-alpine
docker run -d --rm -p 33307:3306 -e MYSQL_ROOT_PASSWORD=cache -e MYSQL_DATABASE=cache mysql:8

cd redisx && REDIS_ADDR=localhost:6399 go test ./...
cd sqltest && \
  POSTGRES_DSN='postgres://postgres:cache@localhost:55432/postgres?sslmode=disable' \
  MYSQL_DSN='root:cache@tcp(localhost:33307)/cache' \
  go test ./...
```

Pick ports that are free on your machine. CI runs all of them on every push.

## The one rule

**The core module must keep an empty `require` block.** It is the reason drivers
live in their own packages and `redisx` is its own module. CI fails the build if
a dependency appears. If you need a third-party library, it belongs in a
satellite module.

## Adding a driver

Implement `cache.Store`, then prove it against the shared conformance suite:

```go
func TestStore(t *testing.T) {
    storetest.Run(t, func(t *testing.T) cache.Store { return mystore.New() })
}
```

Implement `TTLStore`, `ManyStore` or `LockStore` as well and the suite picks up
the extra cases automatically. A driver that needs a third-party client should
follow the `redis` pattern: define the narrow interface you need, and ship the
adapter as a separate module.

## Before opening a pull request

```sh
gofmt -l .        # must print nothing
go vet ./...
staticcheck ./...
go test -race ./...
```

Document every exported identifier — the reference page is the library's user
interface. Explain *why* in comments where the reason is not obvious from the
code; the code already says what it does.
