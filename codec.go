package cache

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Codec converts values to and from the byte slices a [Store] holds. Supply an
// alternative with [WithCodec] to use msgpack, gob, protobuf or encryption.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec encodes values as JSON. It is the default codec: portable between
// languages, debuggable by eye, and adequate for most caches.
type JSONCodec struct{}

// Marshal implements [Codec].
func (JSONCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal implements [Codec].
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// FormatCounter renders a counter in the representation every driver's
// Increment must store. Because it matches the JSON encoding of an integer, a
// key written by Increment still reads back through [Get] as an int64, and a
// key written as an integer through Set can be incremented.
func FormatCounter(n int64) []byte { return strconv.AppendInt(nil, n, 10) }

// ParseCounter reads a counter written by [FormatCounter].
func ParseCounter(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cache: value is not an integer: %w", err)
	}
	return n, nil
}
