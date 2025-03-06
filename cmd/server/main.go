package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"github.com/quqxiaoli/distributed-cache/pkg/cache"
)

type Response struct {
	Status string `json:"status"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
	Error  string `json:"error,omitempty"`
}

func main() {
	cache := cache.NewCache()

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		w.Header().Set("Content-Type", "application/json")
		if key == "" || value == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(Response{Error: "Key or value cannot be empty"})
			return
		}
		ch := make(chan error)
		go func() {
			cache.Set(key, value)
			ch <- nil // 存成功
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
		key := r.URL.Query().Get("key")
		w.Header().Set("Content-Type", "application/json")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(Response{Error: "Key cannot be empty"})
			return
		}
		ch := make(chan struct {
			value  string
			exists bool
		})
		go func() {
			value, exists := cache.Get(key)
			ch <- struct {
				value  string
				exists bool
			}{value, exists}
		}()
		result := <-ch
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
		key := r.URL.Query().Get("key")
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
