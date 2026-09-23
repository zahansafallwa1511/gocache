package cache

import (
	"fmt"
	"maps"
	"slices"
	"sync"
)

// Manager holds the caches an application has configured and hands them out by
// name, for applications that use more than one backend — say a local memory
// cache for hot values and Redis for shared ones.
//
// A Manager is safe for concurrent use. Keep it on your application struct and
// pass it where it is needed rather than making it a package-level global.
type Manager struct {
	mu       sync.RWMutex
	caches   map[string]*Cache
	fallback string
}

// NewManager returns an empty Manager.
func NewManager() *Manager {
	return &Manager{caches: make(map[string]*Cache)}
}

// Register adds a cache under name. The first cache registered becomes the
// default unless [Manager.SetDefault] says otherwise. Registering an existing
// name replaces it.
func (m *Manager) Register(name string, c *Cache) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.caches[name] = c
	if m.fallback == "" {
		m.fallback = name
	}
}

// SetDefault names the cache [Manager.Default] returns. It reports an error if
// no such cache is registered.
func (m *Manager) SetDefault(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.caches[name]; !ok {
		return fmt.Errorf("cache: no store named %q", name)
	}
	m.fallback = name
	return nil
}

// Store returns the cache registered under name, or an error naming the stores
// that are registered.
func (m *Manager) Store(name string) (*Cache, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.caches[name]
	if !ok {
		return nil, fmt.Errorf("cache: no store named %q (have %v)", name, slices.Sorted(maps.Keys(m.caches)))
	}
	return c, nil
}

// MustStore is [Manager.Store] for configuration read at startup, where a
// missing store is a programming error rather than a runtime condition. It
// panics if name is not registered.
func (m *Manager) MustStore(name string) *Cache {
	c, err := m.Store(name)
	if err != nil {
		panic(err)
	}
	return c
}

// Default returns the default cache, or nil if none is registered.
func (m *Manager) Default() *Cache {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.caches[m.fallback]
}

// Names lists the registered store names, sorted.
func (m *Manager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Sorted(maps.Keys(m.caches))
}

// Close closes every registered cache and returns the first error, attempting
// all of them regardless.
func (m *Manager) Close() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var firstErr error
	for _, c := range m.caches {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
