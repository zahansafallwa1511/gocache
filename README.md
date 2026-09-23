# gocache

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

| Package | Notes |
|---|---|
| `gocache/memory` | Sharded maps, lazy expiry, background janitor. |
| `gocache/file` | Atomic writes via rename; safe across processes. |
| `gocache/redis` | Client-agnostic — see below. |
| `gocache/redisx` | Ready-made `go-redis` adapter. Separate module, so only its users take the dependency. |
| `gocache/sql` | Postgres, MySQL, SQLite via `database/sql`. |
| `gocache/null` | Caches nothing. Turn caching off without touching call sites. |

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

## Testing

```sh
go test ./...
```

The Redis integration tests need a server and are skipped without one:

```sh
docker run -d --rm -p 6399:6379 redis:7-alpine
cd redisx && REDIS_ADDR=localhost:6399 go test ./...
```

## License

MIT.
