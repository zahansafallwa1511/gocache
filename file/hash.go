package file

import (
	"crypto/sha256"
	"encoding/hex"
)

// gocacheHash maps a cache key to the hex digest used as its filename. Keys are
// hashed rather than escaped so that any key is storable, including one holding
// path separators, characters the filesystem rejects, or more bytes than a
// filename allows.
func gocacheHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
