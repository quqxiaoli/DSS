package main

import (
    "encoding/json" // 包：处理 JSON 编码和解码。
    "fmt"           // 包：格式化输出。
    "log"           // 包：记录日志。
    "net/http"      // 包：实现 HTTP 服务。
    "os"            // 包：操作文件系统，如日志文件。
    "github.com/quqxiaoli/distributed-cache/pkg/cache" // 自定义包：导入 cache 包。
)

// Response 结构体：定义 HTTP 响应的 JSON 格式。
type Response struct {
    Status string `json:"status"`         // 字段：状态（ok/not found）。
    Key    string `json:"key,omitempty"`   // 字段：键（可选）。
    Value  string `json:"value,omitempty"` // 字段：值（可选）。
    Error  string `json:"error,omitempty"` // 字段：错误信息（可选）。
}

// GetRequest 结构体：封装 /get 请求的参数。
type GetRequest struct {
    key    string                    // 字段：请求的键。
    result chan struct {             // 字段：结果通道。
        value  string             // 嵌套字段：返回值。
        exists bool               // 嵌套字段：是否存在。
    }
}

// 全局日志变量：用于记录 INFO 和 ERROR 级别的日志。
var (
    infoLogger  = log.New(os.Stdout, "[INFO] ", log.LstdFlags)  // 默认输出到终端。
    errorLogger = log.New(os.Stdout, "[ERROR] ", log.LstdFlags) // 默认输出到终端。
)

const validAPIKey = "my-secret-key" // 常量：API 密钥，用于认证。

// handleGets 函数：常驻 Goroutine，处理 /get 请求。
func handleGets(localCache *cache.Cache, ch chan GetRequest) {
    for req := range ch { // 无限循环，从通道读取请求。
        value, exists := localCache.Get(req.key) // 获取键值。
        req.result <- struct {                   // 将结果发送回通道。
            value  string
            exists bool
        }{value, exists}
    }
}

func main() {
    // 打开日志文件，用于持久化记录。
    logFile, err := os.OpenFile("cache.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatalf("Failed to open log file: %v", err) // 打开失败则退出程序。
    }
    defer logFile.Close()           // 在 main 函数结束时关闭文件。
    infoLogger.SetOutput(logFile)  // 设置 INFO 日志输出到文件。
    errorLogger.SetOutput(logFile) // 设置 ERROR 日志输出到文件。

    // 设置日志格式，包含时间和文件名。
    log.SetFlags(log.LstdFlags | log.Lshortfile)
    infoLogger.Println("启动分布式缓存服务器...") // 启动日志。

    localCache := cache.NewCache()     // 创建本地缓存实例，重命名为 localCache。
    getCh := make(chan GetRequest, 10) // 创建缓冲通道，容量 10。
    go handleGets(localCache, getCh)   // 启动常驻 Goroutine 处理 /get。

    ring := cache.NewHashRing(3)   // 创建一致性哈希环，3 个副本，明确用包名 cache。
    ring.AddNode("localhost:8080") // 添加静态节点（目前只有本地）。

    // 处理 /set 请求。
    http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
        apiKey := r.URL.Query().Get("api_key") // 获取 URL 参数中的 API 密钥。
        if apiKey != validAPIKey {             // 校验密钥。
            errorLogger.Printf("Invalid API key: %s", apiKey) // 记录错误日志。
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusUnauthorized) // 返回 401 未授权。
            json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
            return
        }
        key := r.URL.Query().Get("key")   // 获取键。
        value := r.URL.Query().Get("value") // 获取值。
        node := ring.GetNode(key)         // 使用一致性哈希确定节点。
        infoLogger.Printf("Set request: key=%s, value=%s, node=%s", key, value, node) // 记录请求日志。
        w.Header().Set("Content-Type", "application/json")
        if key == "" || value == "" { // 参数校验。
            w.WriteHeader(http.StatusBadRequest) // 返回 400 错误请求。
            json.NewEncoder(w).Encode(Response{Error: "Key or value cannot be empty"})
            return
        }
        ch := make(chan error) // 创建错误通道。
        go func() {            // 在 Goroutine 中设置缓存。
            localCache.Set(key, value) // 使用 localCache 设置。
            ch <- nil                  // 发送无错误信号。
        }()
        if err := <-ch; err != nil { // 检查设置是否出错。
            errorLogger.Printf("Failed to set key: %v", err)
            w.WriteHeader(http.StatusInternalServerError) // 返回 500 服务器错误。
            json.NewEncoder(w).Encode(Response{Error: "Failed to set key"})
            return
        }
        json.NewEncoder(w).Encode(Response{ // 返回成功响应。
            Status: "ok",
            Key:    key,
            Value:  value,
        })
    })

    // 处理 /get 请求。
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
        node := ring.GetNode(key) // 使用一致性哈希确定节点。
        infoLogger.Printf("Get request: key=%s, node=%s", key, node)
        w.Header().Set("Content-Type", "application/json")
        if key == "" {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Key cannot be empty"})
            return
        }
        resultCh := make(chan struct { // 创建结果通道。
            value  string
            exists bool
        })
        getCh <- GetRequest{key, resultCh} // 发送请求到通道。
        result := <-resultCh               // 接收结果。
        if result.exists {                 // 如果键存在。
            json.NewEncoder(w).Encode(Response{
                Status: "ok",
                Key:    key,
                Value:  result.value,
            })
        } else { // 如果键不存在或过期。
            json.NewEncoder(w).Encode(Response{
                Status: "not found",
                Key:    key,
            })
        }
    })

    // 处理 /delete 请求。
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
        node := ring.GetNode(key) // 使用一致性哈希确定节点。
        infoLogger.Printf("Delete request: key=%s, node=%s", key, node)
        w.Header().Set("Content-Type", "application/json")
        if key == "" {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Key cannot be empty"})
            return
        }
        ch := make(chan error)
        go func() {
            localCache.Delete(key) // 使用 localCache 删除。
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

    fmt.Println("Server running on :8080")    // 提示服务启动。
    http.ListenAndServe(":8080", nil)         // 启动 HTTP 服务。
}