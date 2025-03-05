package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/quqxiaoli/distributed-cache/pkg/cache"
)

func main() {
	cache := cache.NewCache()

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		w.Header().Set("Content-Type", "application/json")
		if key == "" || value == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Key or value cannot be empty",
			})
			return
		}
		go func() {
			cache.Set(key, value)
		}()
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
			"key":    key,
			"value":  value,
		})
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		w.Header().Set("Content-Type", "application/json")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Key cannot be empty",
			})
			return
		}
		value, exists := cache.Get(key)
		if exists {
			json.NewEncoder(w).Encode(map[string]string{
				"status": "ok",
				"key":    key,
				"value":  value,
			})
		} else {
			json.NewEncoder(w).Encode(map[string]string{
				"status": "not found",
				"key":    key,
			})
		}
	})

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		w.Header().Set("Content-Type", "application/json")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Key cannot be empty",
			})
			return
		}
		cache.Delete(key)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
			"key":    key,
		})
	})

	fmt.Println("Server running on :8080")
	http.ListenAndServe(":8080", nil)
}
