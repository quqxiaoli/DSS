package main

import (
	"fmt"
	"net/http"
)

// 定义 Cache 结构体
type Cache struct {
	data map[string]string
}

// 初始化 Cache
func NewCache() *Cache {
	return &Cache{
		data: make(map[string]string),
	}
}

// 存方法
func (c *Cache) Set(key, value string) {
	c.data[key] = value
}

// 取方法
func (c *Cache) Get(key string) (string, bool) {
	value, exists := c.data[key]
	return value, exists
}

// 删除方法
func (c *Cache) Delete(key string) {
	delete(c.data, key)
}
func main() {
	// 创建 Cache 实例
	cache := NewCache()

	// 存入键值对
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		cache.Set(key, value)
		fmt.Fprintf(w, "Set %s = %s\n", key, value)
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value, exists := cache.Get(key)
		if exists {
			fmt.Fprintf(w, "Value: %s\n", value)
		} else {
			fmt.Fprintf(w, "Key not found\n")
		}
	})

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		cache.Delete(key)
		fmt.Fprintf(w, "Deleted key: %s\n", key)
	})
	fmt.Println("Server running on :8080")
	http.ListenAndServe(":8080", nil)
}
