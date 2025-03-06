package cache

import "sync"

type Cache struct {
	data map[string]string
	mu   sync.Mutex
}

func NewCache() *Cache {
	return &Cache{
		data: make(map[string]string),
	}
}

func (c *Cache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
}

func (c *Cache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, exists := c.data[key]
	return value, exists
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
}
