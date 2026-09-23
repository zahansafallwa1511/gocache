# gocache

[![CI](https://github.com/zahansafallwa1511/gocache/actions/workflows/ci.yml/badge.svg)](https://github.com/zahansafallwa1511/gocache/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zahansafallwa1511/gocache.svg)](https://pkg.go.dev/github.com/zahansafallwa1511/gocache)
[![Go Report Card](https://goreportcard.com/badge/github.com/zahansafallwa1511/gocache)](https://goreportcard.com/report/github.com/zahansafallwa1511/gocache)
[![Go 1.25+](https://img.shields.io/badge/go-1.25%2B-00ADD8)](https://go.dev/dl/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A caching library for Go that keeps the call site short without giving up the
manners of a standard-library package: `context` first, errors returned, no
globals, and **no third-party dependencies**.

```go
user, err := cache.Remember(ctx, c, "user:1", time.Hour, func(ctx context.Context) (User, error) {
    return db.FindUser(ctx, 1)
})
```

## Install

```sh
go get github.com/zahansafallwa1511/gocache
```

## Design

Two layers, so that using the library and extending it are separate jobs:

| | |
|---|---|
| `cache.Store` | The six-method driver contract. Deals in `[]byte`. Implement it and any backend works everywhere. |
| `cache.Cache` | What you call. Encoding, key prefixing, tags, locks, events, stampede protection. |

Drivers live in their own packages, so importing `gocache` never drags in a
Redis or SQL client.

| Package | Shared between processes | Survives restart | Use it when |
|---|---|---|---|
| `gocache/memory` | no | no | You want the fastest possible cache and each instance may hold its own copy. |
| `gocache/file` | same machine only | yes | One machine, no extra service to run. |
| `gocache/redis` | yes | yes | Several instances must agree. The usual production choice. |
| `gocache/redisx` | yes | yes | As above, with the `go-redis` adapter already written. |
| `gocache/sql` | yes | yes | You want a shared cache without operating another service. |
| `gocache/null` | — | — | You want caching off without touching call sites. |

All of them support the full API — tags, locks, batches, counters — so switching
driver is a one-line change.

## Usage

### Basics

```go
store := memory.New()
c := cache.New(store, cache.WithPrefix("app:"), cache.WithDefaultTTL(10*time.Minute))
defer c.Close()

c.Set(ctx, "user:1", user, time.Hour)   // put
c.Forever(ctx, "config", cfg)           // no expiry
c.Add(ctx, "once", true, time.Minute)   // only if absent; ErrNotStored if taken
c.Forget(ctx, "user:1")
c.Flush(ctx)

ok, err := c.Has(ctx, "user:1")
ttl, err := c.TTL(ctx, "user:1")
```

Reads are generic, so nothing is an `any`:

```go
u, err := cache.Get[User](ctx, c, "user:1")        // ErrNotFound if absent
u, err := cache.GetOr(ctx, c, "user:1", anonymous) // fallback instead
u, err := cache.Pull[User](ctx, c, "user:1")       // read and delete
```

A miss is `cache.ErrNotFound`, tested with `errors.Is`. It is an ordinary error,
not a `(value, bool)` pair, so it composes with the rest of your error handling.

### Remember

`Remember` is the reason the library exists: read through the cache, and on a
miss compute, store, and return.

```go
feed, err := cache.Remember(ctx, c, "feed", 5*time.Minute, buildFeed)
feed, err := cache.RememberForever(ctx, c, "feed", buildFeed)
```

Concurrent misses on the same key are collapsed into **one** call to the loader,
so a hot key expiring under load does not stampede your database. A loader that
returns an error caches nothing.

### Flexible (stale-while-revalidate)

```go
// Fresh for a minute; served stale for up to an hour while it refreshes behind
// the caller.
stats, err := cache.Flexible(ctx, c, "stats", time.Minute, time.Hour, computeStats)
```

Past the stale window the value is gone and the loader runs inline. Background
refreshes are deduplicated per key and use a detached context so they outlive
the request that triggered them; observe their failures with
`cache.WithRefreshErrorHandler`.

### Batches

```go
c.SetMany(ctx, map[string]any{"a": 1, "b": 2}, time.Hour)

var a, b int
found, err := c.Many(ctx, []string{"a", "b"}, []any{&a, &b})
```

Stores that implement `cache.ManyStore` (memory, redis, sql) serve these in one
round trip; others fall back to a loop.

### Counters

```go
hits, err := c.Increment(ctx, "hits", 1)
left, err := c.Decrement(ctx, "quota", 5)
```

Counters use a shared wire format (`cache.FormatCounter`), so a key written by
`Increment` still reads back through `cache.Get[int64]`.

### Tags

```go
posts := c.Tags("posts", "user:1")
posts.Set(ctx, "feed", feed, time.Hour)

posts.FlushTags(ctx) // invalidates everything tagged posts or user:1
```

Tagged entries are invisible to the untagged cache. Invalidation works by
versioning each tag rather than enumerating keys, so `FlushTags` costs one write
per tag no matter how many entries it drops — and works on every driver,
including ones that cannot list keys. Orphaned entries fall off on their TTL.

### Locks

```go
lock, err := c.Lock("rebuild-index", 5*time.Minute)

ran, err := lock.Get(ctx, func(ctx context.Context) error {
    return rebuildIndex(ctx)
})                                  // skips if held elsewhere

if err := lock.Block(ctx, 10*time.Second); err != nil { // or wait for it
    return err // cache.ErrLockTimeout if it never freed
}
defer lock.Release(ctx)
```

Locks are owner-checked: a process that lost its lock to the TTL cannot release
the holder that came after it. Redis does the check in Lua, SQL in a single
`DELETE … WHERE value = ?`.

### Events

```go
c := cache.New(store, cache.WithListener(cache.ListenerFuncs{
    Hit:  func(ctx context.Context, key string) { metrics.Hits.Inc() },
    Miss: func(ctx context.Context, key string) { metrics.Misses.Inc() },
}))
```

### Several stores

```go
m := cache.NewManager()
m.Register("redis", cache.New(redisStore))
m.Register("memory", cache.New(memory.New()))
m.SetDefault("redis")

m.MustStore("memory").Set(ctx, "k", v, time.Minute)
m.Default().Get(ctx, "k", &v)
```

Named stores without a global. Keep the `*Manager` — or better, the one `*Cache`
a component actually needs — on your application struct and pass it in. An
implicit global makes tests share state and hides a dependency that is easier to
see in a constructor.

## Redis

The quickest path, if you already use `go-redis`:

```sh
go get github.com/zahansafallwa1511/gocache/redisx
```

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
c := cache.New(redisx.New(client))
```

`redisx` lives in its own module, so importing `gocache` itself never pulls in
`go-redis`. It accepts any `redis.UniversalClient` — a plain client, a cluster,
a ring, or a failover client.

### Managed Redis: ElastiCache, MemoryDB, Valkey

Anything that speaks the Redis protocol works, because the driver only issues
ordinary commands. AWS ElastiCache (Redis or Valkey), MemoryDB, Azure Cache for
Redis, GCP Memorystore and Upstash are all just a client configuration:

```go
client := redis.NewClient(&redis.Options{
    Addr:      "my-cache.abc123.ng.0001.use1.cache.amazonaws.com:6379",
    Username:  "default",              // ElastiCache RBAC
    Password:  os.Getenv("REDIS_AUTH"),
    TLSConfig: &tls.Config{},          // required when encryption in transit is on
})
c := cache.New(redisx.New(client))
```

**With cluster mode enabled**, use a cluster client and say so:

```go
client := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs:     []string{"clustercfg.my-cache.abc123.use1.cache.amazonaws.com:6379"},
    TLSConfig: &tls.Config{},
})
c := cache.New(redisx.New(client, redisstore.WithClusterMode()))
```

A cluster refuses multi-key commands whose keys live on different shards, so
`Many` reads key by key there instead of using `MGET`. The driver detects this
from the first `CROSSSLOT` error even without the option — `WithClusterMode`
only saves that one failed round trip. Two things to know on a cluster:

- `Flush` reaches a single node. Invalidate with tags instead, or flush each
  master through your client.
- Locks and counters are single-key operations, so they behave normally.

ElastiCache's **Memcached** engine is not supported — this library has no
Memcached driver.

### Any other client

The `redis` driver itself depends on no client at all. It needs one method:

```go
type Conn interface {
    Do(ctx context.Context, args ...any) (any, error)
}
```

Implement it over rueidis, redigo, or anything else, and the driver works
unchanged. `redisx` above is exactly this adapter for `go-redis`, nothing more.

`Flush` issues `FLUSHDB`, so give the cache its own database.

## Database

```go
store := sqlstore.New(db, sqlstore.Postgres) // or MySQL, SQLite
if err := store.CreateTable(ctx); err != nil {
    return err
}
```

Unlike the other drivers this one has no janitor: call `store.DeleteExpired(ctx)`
from a scheduled job.

## Writing a driver

Implement `cache.Store`, then prove it with the shared conformance suite — the
same one the built-in drivers run:

```go
func TestStore(t *testing.T) {
    storetest.Run(t, func(t *testing.T) cache.Store { return mystore.New() })
}
```

Implement `TTLStore`, `ManyStore` or `LockStore` as well and the suite picks up
the extra cases automatically; `cache.Cache` detects them at runtime and falls
back gracefully when they are absent.

## Performance

Against the memory driver on an M-series laptop, one core:

```
BenchmarkStoreGet      93.9 ns/op      8 B/op    1 allocs/op   raw store read
BenchmarkCacheGet     968.3 ns/op    384 B/op   10 allocs/op   + JSON decode
BenchmarkCacheSet     340.4 ns/op    154 B/op    3 allocs/op
BenchmarkRememberHit  805.4 ns/op    320 B/op    7 allocs/op
BenchmarkTaggedGet   1229.0 ns/op    584 B/op   14 allocs/op   + one tag lookup
```

The store itself costs about 90ns; the rest is JSON. If a hot path needs more,
supply a faster `Codec` — that is what the interface is for. Tags add a read per
operation, so prefer a prefix when you do not need grouped invalidation.

Run them with `go test -bench . ./memory/`.

## Testing

```sh
go test -race ./...
```

Everything runs with no servers. Integration tests against real backends are
skipped unless you point them at one:

```sh
docker run -d --rm -p 6399:6379 redis:7-alpine
cd redisx && REDIS_ADDR=localhost:6399 go test ./...

cd sqltest && go test ./...   # SQLite needs no server
```

CI runs the full matrix — Linux, macOS and Windows, plus the conformance suite
against real Redis, Postgres, MySQL and SQLite — on every push. See
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT.
