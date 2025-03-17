package cache

import (
	"container/list"
	"sync"
	"time"
	"github.com/quqxiaoli/distributed-cache/internal/util" // 引入 util 包
)

// CacheItem 表示缓存中的一个条目，包含键、值和过期时间。
type CacheItem struct {
	Key       string
	Value     string
	ExpiresAt time.Time
}

// Cache 表示一个缓存对象，使用 LRU（Least Recently Used）算法来管理缓存项。
type Cache struct {
	// data 是一个映射，用于存储缓存项的键和对应的双向链表元素，方便快速查找。
	data map[string]*list.Element
	// list 是一个双向链表，用于维护缓存项的访问顺序，最近访问的元素位于链表头部。
	list     *list.List
	capacity int
	mu       sync.Mutex
	logger   *util.Logger // 新增 logger 字段
}

// NewCache 创建一个新的缓存实例，接收一个整数参数 capacity 表示缓存的最大容量。
func NewCache(capacity int, logger *util.Logger) *Cache {
	// 初始化一个 Cache 结构体指针
	c := &Cache{
		// 初始化 data 映射，用于存储缓存项的键和对应的双向链表元素
		data: make(map[string]*list.Element),
		// 初始化双向链表，用于维护缓存项的访问顺序
		list:     list.New(),
		capacity: capacity,
		logger:   logger,
	}
	// 启动一个协程，定期清理过期的缓存项，清理间隔为 10 秒
	go c.startCleanup(10 * time.Second)
	return c
}

// startCleanup 定期清理过期的缓存项，接收一个 time.Duration 类型的参数 interval 表示清理间隔
func (c *Cache) startCleanup(interval time.Duration) {
	// 创建一个时间间隔为 interval 的定时器
	ticker := time.NewTicker(interval)
	// 确保在函数返回时停止定时器
	defer ticker.Stop()
	for range ticker.C {
		// 加锁，保证并发安全
		c.mu.Lock()
		// 获取当前时间
		now := time.Now()
		// 计算每次清理时最多检查的缓存项数量，为链表长度的十分之一，至少为 1
		maxCheck := c.list.Len() / 10
		if maxCheck == 0 {
			maxCheck = 1
		}
		// 记录已经检查的缓存项数量
		checked := 0
		// 从链表尾部开始遍历，检查是否有过期的缓存项
		for elem := c.list.Back(); elem != nil && checked < maxCheck; elem = elem.Prev() {
			// 获取当前链表元素对应的缓存项
			item := elem.Value.(*CacheItem)
			// 如果缓存项已过期
			if now.After(item.ExpiresAt) {
				// 从链表中移除该元素
				c.list.Remove(elem)
				// 从 data 映射中删除该缓存项
				delete(c.data, item.Key)
			}
			// 增加已检查的缓存项数量
			checked++
		}
		// 解锁
		c.mu.Unlock()
	}
}

// Set 方法用于设置缓存项，接收键和值作为参数
func (c *Cache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查缓存中是否已存在该键对应的缓存项
	if elem, exists := c.data[key]; exists {
		// 如果存在，将该缓存项移动到链表头部，表示最近访问
		c.list.MoveToFront(elem)
		item := elem.Value.(*CacheItem)
		item.Value = value
		// 更新缓存项的过期时间为当前时间加上 5 秒
		item.ExpiresAt = time.Now().Add(5 * time.Second)
		return
	}

	// 如果缓存项数量达到或超过缓存容量
	if c.list.Len() >= c.capacity {
		// 获取链表尾部的元素，即最旧的缓存项
		oldest := c.list.Back()
		if oldest != nil {
			// 从链表中移除该元素
			c.list.Remove(oldest)
			oldItem := oldest.Value.(*CacheItem)
			// 从 data 映射中删除该缓存项
			delete(c.data, oldItem.Key)
		}
	}

	// 创建一个新的缓存项
	item := &CacheItem{
		Key:       key,
		Value:     value,
		ExpiresAt: time.Now().Add(5 * time.Second),
	}
	// 将新的缓存项添加到链表头部
	elem := c.list.PushFront(item)
	// 将新的缓存项的键和对应的链表元素添加到 data 映射中
	c.data[key] = elem
}

// Delete 方法用于删除指定键的缓存项
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查缓存中是否存在该键对应的缓存项
	if elem, exists := c.data[key]; exists {
		// 如果存在，从链表中移除该元素
		c.list.Remove(elem)
		// 从 data 映射中删除该缓存项
		delete(c.data, key)
	}
}

// GetAll 方法用于获取所有未过期的缓存项，返回一个键值对的映射
func (c *Cache) GetAll() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 初始化一个用于存储结果的映射
	result := make(map[string]string)
	now := time.Now()
	for key, elem := range c.data {
		// 获取当前链表元素对应的缓存项
		item := elem.Value.(*CacheItem)
		// 如果缓存项未过期
		if now.Before(item.ExpiresAt) {
			// 将该缓存项的键值对添加到结果映射中
			result[key] = item.Value
		}
	}
	return result
}

// Get 方法用于获取指定键的缓存项，返回缓存项的值和是否存在的布尔值
func (c *Cache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 缓存请求计数加 1
	CacheRequests.Add(1)

	// 检查缓存中是否存在该键对应的缓存项
	elem, exists := c.data[key]
	if !exists {
		// 如果不存在，缓存未命中计数加 1
		CacheMisses.Add(1)
		return "", false
	}

	// 获取该缓存项
	item := elem.Value.(*CacheItem)
	// 如果缓存项已过期
	if time.Now().After(item.ExpiresAt) {
		// 从链表中移除该元素
		c.list.Remove(elem)
		// 从 data 映射中删除该缓存项
		delete(c.data, key)
		// 缓存未命中计数加 1
		CacheMisses.Add(1)
		return "", false
	}

	c.logger.Info("Cache Get: key=%s, value=%s, exists=true", key, item.Value)
	// 将该缓存项移动到链表头部，表示最近访问
	c.list.MoveToFront(elem)
	// 缓存命中计数加 1
	CacheHits.Add(1)
	return item.Value, true
}

func (c *Cache) SetWithTTL(key, value string, ttl time.Duration) {
    c.mu.Lock()
    defer c.mu.Unlock()
	c.logger.Info("Cache SetWithTTL: key=%s, value=%s, ttl=%v", key, value, ttl)

    if elem, exists := c.data[key]; exists {
        c.list.MoveToFront(elem)
        item := elem.Value.(*CacheItem)
        item.Value = value
        item.ExpiresAt = time.Now().Add(ttl)
        return
    }

    if c.list.Len() >= c.capacity {
        oldest := c.list.Back()
        if oldest != nil {
            c.list.Remove(oldest)
            oldItem := oldest.Value.(*CacheItem)
            delete(c.data, oldItem.Key)
        }
    }

    item := &CacheItem{
        Key:       key,
        Value:     value,
        ExpiresAt: time.Now().Add(ttl),
    }
    elem := c.list.PushFront(item)
    c.data[key] = elem
}
