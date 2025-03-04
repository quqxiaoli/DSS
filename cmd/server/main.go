package main

import (
    "fmt"
    "net/http"
    "github.com/quqxiaoli/distributed-cache/pkg/cache"
)

func main() {
    cache := cache.NewCache()

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