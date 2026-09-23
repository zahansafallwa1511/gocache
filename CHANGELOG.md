# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses
[semantic versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-24

### Added

- Full documentation on every exported identifier, so the reference on
  pkg.go.dev is usable.
- `redis.WithClusterMode`, and automatic recovery from `CROSSSLOT` errors, so
  batch reads work against a Redis Cluster — AWS ElastiCache and MemoryDB in
  cluster mode included.
- Integration tests running the conformance suite against real servers: Redis
  via `redisx`, and SQLite, Postgres and MySQL via the `sqltest` module.
- Benchmarks for the memory driver and the `Cache` layer.
- Continuous integration across Linux, macOS and Windows, including a check that
  the core module never gains a dependency.

### Fixed

- The `sql` driver failed to create its table on MySQL, which rejects
  `CREATE INDEX IF NOT EXISTS`. The index is now declared inline in the
  `CREATE TABLE` for that dialect. This was caught by the new MySQL tests.

## [0.1.0] - 2026-09-23

### Added

- Initial release: `Store` driver contract and the `Cache` API over it.
- `Remember`, `RememberForever` and `Flexible` (stale-while-revalidate), with
  per-process deduplication of concurrent misses.
- Tags with version-based invalidation, atomic cross-process locks, batch reads
  and writes, counters, events and a multi-store manager.
- Drivers: `memory`, `file`, `redis`, `sql` and `null`.
- `storetest`, a conformance suite third-party drivers can run.

[Unreleased]: https://github.com/zahansafallwa1511/gocache/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/zahansafallwa1511/gocache/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/zahansafallwa1511/gocache/releases/tag/v0.1.0
