package cache

import (
	"testing"
	"time"
)

// 测试 Set 和 Get
func TestCacheSetGet(t *testing.T) {
	c := NewCache(2)
	c.Set("key1", "value1")
	val, exists := c.Get("key1")
	if !exists {
		t.Errorf("key1 should exist, but does not")
	}
	if val != "value1" {
		t.Errorf("value for key1 should be value1, but got %s", val)
	}
}

//测试TTL
func TestCacheTTL(t *testing.T) {
	c := NewCache(2)
	c.Set("key1", "value1")
    time.Sleep(6 * time.Second) // 等待 6 秒，超过 5 秒 TTL
    _, exists := c.Get("key1")
    if exists {
        t.Errorf("key1 应该过期，但仍然存在")
    }
}

// 测试 LRU 淘汰
func TestCacheLRU(t *testing.T) {
    c := NewCache(2) // 容量为 2
    c.Set("key1", "value1")
    c.Set("key2", "value2")
    c.Set("key3", "value3") // 超出容量，key1 应被淘汰
    _, exists := c.Get("key1")
    if exists {
        t.Errorf("key1 应该被淘汰，但仍然存在")
    }
    val, exists := c.Get("key2")
    if !exists || val != "value2" {
        t.Errorf("key2 应该存在且值为 value2，得到 %s", val)
    }
}