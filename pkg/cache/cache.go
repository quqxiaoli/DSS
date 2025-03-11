package cache

import (
	"container/list"
	"sync"
	"time"
)

type CacheItem struct {
	Key       string    // 键
	Value     string    // 值
	ExpiresAt time.Time // 过期时间
}

type Cache struct {
	data     map[string]*list.Element // 改为映射到链表节点
	list     *list.List               // 新增：链表记录使用顺序
	capacity int                      // 新增：容量限制
	mu       sync.Mutex               // 并发保护
}

func NewCache(capacity int) *Cache { // 修改：接受容量参数
	c := &Cache{
		data:     make(map[string]*list.Element),
		list:     list.New(),
		capacity: capacity, // 设置容量
	}
	go c.startCleanup(10 * time.Second)
	return c
}

func (c *Cache) Set(key, value string) { // 暂不加 TTL 参数，后续扩展
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果键已存在，更新值并移到头部
	if elem, exists := c.data[key]; exists {
		c.list.MoveToFront(elem)
		item := elem.Value.(*CacheItem)
		item.Value = value
		item.ExpiresAt = time.Now().Add(5 * time.Second)
		return
	}

	// 检查容量，满了就删除最旧的
	if c.list.Len() >= c.capacity {
		oldest := c.list.Back() // 取链表尾部（最旧）
		if oldest != nil {
			c.list.Remove(oldest)
			oldItem := oldest.Value.(*CacheItem)
			delete(c.data, oldItem.Key)
		}
	}

	// 添加新项
	item := &CacheItem{
		Key:       key,
		Value:     value,
		ExpiresAt: time.Now().Add(5 * time.Second),
	}
	elem := c.list.PushFront(item) // 加到链表头部
	c.data[key] = elem             // 存入 map
}

func (c *Cache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.data[key]
	if !exists {
		return "", false
	}

	item := elem.Value.(*CacheItem)
	if time.Now().After(item.ExpiresAt) {
		c.list.Remove(elem)
		delete(c.data, key)
		return "", false
	}

	c.list.MoveToFront(elem) // 更新使用顺序
	return item.Value, true
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.data[key]; exists {
		c.list.Remove(elem)
		delete(c.data, key)
	}
}

func (c *Cache) startCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		// 从尾部向前检查，过期就删除
		for elem := c.list.Back(); elem != nil; elem = elem.Prev() {
			item := elem.Value.(*CacheItem)
			if now.After(item.ExpiresAt) {
				c.list.Remove(elem)
				delete(c.data, item.Key)
			}
		}
		c.mu.Unlock()
	}
}
