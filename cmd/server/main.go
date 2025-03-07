package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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

	log.SetFlags(log.LstdFlags | log.Lshortfile)
    log.Println("启动分布式缓存服务器...")
	
	cache := cache.NewCache()
	getCh := make(chan GetRequest, 10)
	go handleGets(cache, getCh)

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.URL.Query().Get("api_key")
		if apiKey != validAPIKey {
			log.Printf("Invalid API key: %s", apiKey)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
			return
		}
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		log.Printf("Set request: key=%s, value=%s", key, value)
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
			log.Printf("Invalid API key: %s", apiKey)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
			return
		}
		key := r.URL.Query().Get("key")
		log.Printf("Get request: key=%s", key)
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
			log.Printf("Invalid API key: %s", apiKey)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
			return
		}
		key := r.URL.Query().Get("key")
		log.Printf("Delete request: key=%s", key)
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
