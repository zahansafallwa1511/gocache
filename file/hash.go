package file

import (
	"crypto/sha256"
	"encoding/hex"
)

func gocacheHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
