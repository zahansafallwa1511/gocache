package redis_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

type fakeConn struct {
	mu    sync.Mutex
	items map[string]fakeEntry
}

type fakeEntry struct {
	value     []byte
	expiresAt time.Time
}

func newFakeConn() *fakeConn { return &fakeConn{items: make(map[string]fakeEntry)} }

var errNil = errors.New("redis: nil")

func (c *fakeConn) live(key string) (fakeEntry, bool) {
	e, ok := c.items[key]
	if !ok {
		return fakeEntry{}, false
	}
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		delete(c.items, key)
		return fakeEntry{}, false
	}
	return e, true
}

func (c *fakeConn) Do(_ context.Context, args ...any) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := strings.ToUpper(args[0].(string))
	switch cmd {
	case "GET":
		e, ok := c.live(str(args[1]))
		if !ok {
			return nil, errNil
		}
		return e.value, nil

	case "SET":
		key, value := str(args[1]), toBytes(args[2])
		rest := args[3:]

		var (
			nx      bool
			expires time.Time
		)
		for i := 0; i < len(rest); i++ {
			switch strings.ToUpper(str(rest[i])) {
			case "NX":
				nx = true
			case "PX":
				i++
				ms, _ := strconv.ParseInt(str(rest[i]), 10, 64)
				expires = time.Now().Add(time.Duration(ms) * time.Millisecond)
			}
		}
		if _, ok := c.live(key); nx && ok {
			return nil, errNil
		}
		c.items[key] = fakeEntry{value: value, expiresAt: expires}
		return "OK", nil

	case "INCRBY":
		key := str(args[1])
		delta, _ := strconv.ParseInt(str(args[2]), 10, 64)

		var current int64
		var expires time.Time
		if e, ok := c.live(key); ok {
			current, _ = strconv.ParseInt(string(e.value), 10, 64)
			expires = e.expiresAt
		}
		current += delta
		c.items[key] = fakeEntry{value: []byte(strconv.FormatInt(current, 10)), expiresAt: expires}
		return current, nil

	case "DEL":
		delete(c.items, str(args[1]))
		return int64(1), nil

	case "FLUSHDB":
		clear(c.items)
		return "OK", nil

	case "PTTL":
		e, ok := c.live(str(args[1]))
		if !ok {
			return int64(-2), nil
		}
		if e.expiresAt.IsZero() {
			return int64(-1), nil
		}
		return time.Until(e.expiresAt).Milliseconds(), nil

	case "MGET":
		replies := make([]any, 0, len(args)-1)
		for _, a := range args[1:] {
			if e, ok := c.live(str(a)); ok {
				replies = append(replies, e.value)
				continue
			}
			replies = append(replies, nil)
		}
		return replies, nil

	case "EVAL":

		key, owner := str(args[3]), str(args[4])
		if e, ok := c.live(key); ok && string(e.value) == owner {
			delete(c.items, key)
			return int64(1), nil
		}
		return int64(0), nil
	}
	return nil, errors.New("fakeConn: unsupported command " + cmd)
}

func str(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	}
	return ""
}

func toBytes(v any) []byte {
	if b, ok := v.([]byte); ok {
		return append([]byte(nil), b...)
	}
	return []byte(str(v))
}
