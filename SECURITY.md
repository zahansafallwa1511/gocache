# Security Policy

## Supported versions

The latest minor release receives security fixes.

## Reporting a vulnerability

Please report security issues privately through
[GitHub's security advisory form](https://github.com/zahansafallwa1511/gocache/security/advisories/new)
rather than opening a public issue.

## Notes on safe use

- **Cached values are as sensitive as their source.** The `file` driver writes
  with mode 0600 by default; widen it only deliberately. The `redis` and `sql`
  drivers store whatever your codec produces, in plaintext — supply an
  encrypting `Codec` if the backend is shared or untrusted.
- **Keys are not secrets, and they are not escaped.** Prefixes and tag names are
  concatenated into the stored key. Building a key from unvalidated user input
  lets a caller address another user's entries; scope such keys yourself.
- **`WithTable` in the `sql` driver is interpolated into statements**, because
  SQL does not allow a placeholder for an identifier. Pass a constant, never
  user input.
- **`Flush` empties the entire backend**, not just this cache's prefix. On a
  shared Redis database it removes other applications' keys too.
