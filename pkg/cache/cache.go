package cache

import (
    "container/list"
    "sync"
    "time"
)

type CacheItem struct {
    Key       string
    Value     string
    ExpiresAt time.Time
}

type Cache struct {
    data     map[string]*list.Element
    list     *list.List
    capacity int
    mu       sync.Mutex
}

func NewCache(capacity int) *Cache {
    c := &Cache{
        data:     make(map[string]*list.Element),
        list:     list.New(),
        capacity: capacity,
    }
    go c.startCleanup(10 * time.Second)
    return c
}

func (c *Cache) Set(key, value string) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if elem, exists := c.data[key]; exists {
        c.list.MoveToFront(elem)
        item := elem.Value.(*CacheItem)
        item.Value = value
        item.ExpiresAt = time.Now().Add(5 * time.Second)
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
        ExpiresAt: time.Now().Add(5 * time.Second),
    }
    elem := c.list.PushFront(item)
    c.data[key] = elem
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
        maxCheck := c.list.Len() / 10
        if maxCheck == 0 {
            maxCheck = 1
        }
        checked := 0
        for elem := c.list.Back(); elem != nil && checked < maxCheck; elem = elem.Prev() {
            item := elem.Value.(*CacheItem)
            if now.After(item.ExpiresAt) {
                c.list.Remove(elem)
                delete(c.data, item.Key)
            }
            checked++
        }
        c.mu.Unlock()
    }
}

func (c *Cache) GetAll() map[string]string {
    c.mu.Lock()
    defer c.mu.Unlock()

    result := make(map[string]string)
    now := time.Now()
    for key, elem := range c.data {
        item := elem.Value.(*CacheItem)
        if now.Before(item.ExpiresAt) {
            result[key] = item.Value
        }
    }
    return result
}

func (c *Cache) Get(key string) (string, bool) {
    c.mu.Lock()
    defer c.mu.Unlock()

    CacheRequests.Add(1)

    elem, exists := c.data[key]
    if !exists {
        CacheMisses.Add(1)
        return "", false
    }

    item := elem.Value.(*CacheItem)
    if time.Now().After(item.ExpiresAt) {
        c.list.Remove(elem)
        delete(c.data, key)
        CacheMisses.Add(1)
        return "", false
    }

    c.list.MoveToFront(elem)
    CacheHits.Add(1)
    return item.Value, true
}