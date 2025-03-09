package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/quqxiaoli/distributed-cache/pkg/cache"
)

type Response struct {
	Status string `json:"status"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
	Error  string `json:"error,omitempty"`
}

type GetRequest struct {
	key    string
	result chan struct {
		value  string
		exists bool
	}
}

// 全局日志变量。
var (
	infoLogger  = log.New(os.Stdout, "[INFO] ", log.LstdFlags)  // 默认终端输出 INFO 日志。
	errorLogger = log.New(os.Stdout, "[ERROR] ", log.LstdFlags) // 默认终端输出 ERROR 日志。
)

func handleGets(cache *cache.Cache, ch chan GetRequest) {
	for req := range ch {
		value, exists := cache.Get(req.key)
		req.result <- struct {
			value  string
			exists bool
		}{value, exists}
	}
}

const validAPIKey = "my-secret-key"

func main() {
	// 设置日志文件。
	logFile, err := os.OpenFile("cache.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err) // 打开失败则退出。
	}
	defer logFile.Close()          // 主函数结束时关闭文件。
	infoLogger.SetOutput(logFile)  // 重定向 INFO 日志到文件。
	errorLogger.SetOutput(logFile) // 重定向 ERROR 日志到文件。

	log.SetFlags(log.LstdFlags | log.Lshortfile) // 设置日志格式，带时间和文件名。
	log.Println("启动分布式缓存服务器...")

	cache := cache.NewCache()
	getCh := make(chan GetRequest, 10)
	go handleGets(cache, getCh)

	ring := cache.NewHashRing(3)   // 创建哈希环，3 个副本。
	ring.AddNode("localhost:8080") // 添加静态节点。

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.URL.Query().Get("api_key")
		if apiKey != validAPIKey {
			errorLogger.Printf("Invalid API key: %s", apiKey) // 记录错误日志。
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
			return
		}
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		node := ring.GetNode(key) // 获取 key 所属节点。
		infoLogger.Printf("Set request: key=%s, value=%s, node=%s", key, value, node)
		w.Header().Set("Content-Type", "application/json")
		if key == "" || value == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(Response{Error: "Key or value cannot be empty"})
			return
		}
		ch := make(chan error)
		go func() {
			cache.Set(key, value)
			ch <- nil
		}()
		if err := <-ch; err != nil {
			errorLogger.Printf("Failed to set key: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(Response{Error: "Failed to set key"})
			return
		}
		json.NewEncoder(w).Encode(Response{
			Status: "ok",
			Key:    key,
			Value:  value,
		})
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.URL.Query().Get("api_key")
		if apiKey != validAPIKey {
			errorLogger.Printf("Invalid API key: %s", apiKey)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
			return
		}
		key := r.URL.Query().Get("key")
		node := ring.GetNode(key) // 获取 key 所属节点。
		infoLogger.Printf("Set request: key=%s, node=%s", key, node)
		w.Header().Set("Content-Type", "application/json")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(Response{Error: "Key cannot be empty"})
			return
		}
		resultCh := make(chan struct {
			value  string
			exists bool
		})
		getCh <- GetRequest{key, resultCh}
		result := <-resultCh
		if result.exists {
			json.NewEncoder(w).Encode(Response{
				Status: "ok",
				Key:    key,
				Value:  result.value,
			})
		} else {
			json.NewEncoder(w).Encode(Response{
				Status: "not found",
				Key:    key,
			})
		}
	})

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.URL.Query().Get("api_key")
		if apiKey != validAPIKey {
			errorLogger.Printf("Invalid API key: %s", apiKey)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
			return
		}
		key := r.URL.Query().Get("key")
		node := ring.GetNode(key) // 获取 key 所属节点。
		infoLogger.Printf("Set request: key=%s, node=%s", key, node)
		w.Header().Set("Content-Type", "application/json")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(Response{Error: "Key cannot be empty"})
			return
		}
		ch := make(chan error)
		go func() {
			cache.Delete(key)
			ch <- nil
		}()
		if err := <-ch; err != nil {
			errorLogger.Printf("Failed to delete key: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(Response{Error: "Failed to delete key"})
			return
		}
		json.NewEncoder(w).Encode(Response{
			Status: "ok",
			Key:    key,
		})
	})

	fmt.Println("Server running on :8080")
	http.ListenAndServe(":8080", nil)
}
