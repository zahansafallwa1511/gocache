package cache

import (
	"fmt"
	"maps"
	"slices"
	"sync"
)

type Manager struct {
	mu       sync.RWMutex
	caches   map[string]*Cache
	fallback string
}

func NewManager() *Manager {
	return &Manager{caches: make(map[string]*Cache)}
}

func (m *Manager) Register(name string, c *Cache) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.caches[name] = c
	if m.fallback == "" {
		m.fallback = name
	}
}

func (m *Manager) SetDefault(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.caches[name]; !ok {
		return fmt.Errorf("cache: no store named %q", name)
	}
	m.fallback = name
	return nil
}

func (m *Manager) Store(name string) (*Cache, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.caches[name]
	if !ok {
		return nil, fmt.Errorf("cache: no store named %q (have %v)", name, slices.Sorted(maps.Keys(m.caches)))
	}
	return c, nil
}

func (m *Manager) MustStore(name string) *Cache {
	c, err := m.Store(name)
	if err != nil {
		panic(err)
	}
	return c
}

func (m *Manager) Default() *Cache {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.caches[m.fallback]
}

func (m *Manager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Sorted(maps.Keys(m.caches))
}

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
