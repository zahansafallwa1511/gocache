package cache

import "sync"

// group deduplicates concurrent calls for the same key, so that a cache miss
// hammered by N goroutines results in one call to the expensive loader rather
// than N. It is a trimmed-down equivalent of golang.org/x/sync/singleflight,
// reimplemented here to keep this package dependency-free.
type group struct {
	mu    sync.Mutex
	calls map[string]*call
}

// call is one in-flight computation and its eventual result.
type call struct {
	wg  sync.WaitGroup
	val any
	err error
}

// Do runs fn for key unless a call for key is already in flight, in which case
// it waits for that call and returns its result. The result and error are shared
// by every waiter.
func (g *group) Do(key string, fn func() (any, error)) (any, error) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	if g.calls == nil {
		g.calls = make(map[string]*call)
	}
	c := new(call)
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
		c.wg.Done()
	}()

	c.val, c.err = fn()
	return c.val, c.err
}
