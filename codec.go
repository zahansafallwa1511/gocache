package cache

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func FormatCounter(n int64) []byte { return strconv.AppendInt(nil, n, 10) }

func ParseCounter(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cache: value is not an integer: %w", err)
	}
	return n, nil
}
