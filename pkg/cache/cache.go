package cache

import (
	"sync"
	"time"
)

type CacheItem struct { // 结构体：存储缓存项。
	value     string    // 字段：缓存的值。
	expiresAt time.Time // 字段：过期时间，用 time.Time 类型。
}

type Cache struct {
	data map[string]CacheItem
	mu   sync.Mutex
}

func NewCache() *Cache {
	c := &Cache{
		data: make(map[string]CacheItem),
	}
	go c.startCleanup(10 * time.Second) // 新增：启动清理 Goroutine，每10秒检查一次
	return c
}

func (c *Cache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = CacheItem{
		value:     value,
		expiresAt: time.Now().Add(5 * time.Second), // 方法：当前时间加 5 秒。
	}
}

func (c *Cache) Get(key string) (string, bool) { // 方法：取缓存值。
	c.mu.Lock()
	defer c.mu.Unlock()
	item, exists := c.data[key]                      // 从 map 取值。
	if !exists || time.Now().After(item.expiresAt) { // 方法：检查是否过期。
		delete(c.data, key) // 函数：删除过期项。
		return "", false
	}
	return item.value, true
}

func (c *Cache) Delete(key string) { // 方法：删除缓存。
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key) // 函数：从 map 删除。
}

func (c *Cache) startCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for key, item := range c.data {
			if now.After(item.expiresAt) {
				delete(c.data, key) // 删除过期项
			}
		}
		c.mu.Unlock()
	}
}
