package main

import (
    "bytes"
    "compress/gzip"
    "crypto/tls"
    "encoding/json"
    "expvar"
    "flag"
    "fmt"
    "io"
    "log"
    "net/http"
    _ "net/http/pprof"
    "os"
    "strings"
    "sync"
    "time"

    "github.com/quqxiaoli/distributed-cache/pkg/cache" // 假设这是你的cache包路径
)

// gzipHandler 启用GZIP压缩，提升响应效率
func gzipHandler(h http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
            h.ServeHTTP(w, r)
            return
        }
        w.Header().Set("Content-Encoding", "gzip")
        gz := gzip.NewWriter(w)
        defer gz.Close()
        gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
        h.ServeHTTP(gzw, r)
    })
}

type gzipResponseWriter struct {
    io.Writer
    http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
    return w.Writer.Write(b)
}

// 定义性能监控指标
var (
    getLatency = expvar.NewFloat("get_latency")
    setLatency = expvar.NewFloat("set_latency")
)

// Response 定义API响应结构
type Response struct {
    Status string `json:"status"`
    Key    string `json:"key,omitempty"`
    Value  string `json:"value,omitempty"`
    Error  string `json:"error,omitempty"`
}

// GetRequest 用于本地缓存的异步获取请求
type GetRequest struct {
    key    string
    result chan struct {
        value  string
        exists bool
    }
}

var (
    infoLogger  *log.Logger  // 信息日志
    errorLogger *log.Logger  // 错误日志
    hb          *cache.Heartbeat // 心跳检测实例
    lastLogTime time.Time    // 上次日志时间，用于限流
    logMutex    sync.Mutex   // 日志并发控制
)

// logWithRateLimit 限制日志频率，避免过度日志导致性能问题
func logWithRateLimit(logger *log.Logger, format string, v ...interface{}) {
    logMutex.Lock()
    defer logMutex.Unlock()
    now := time.Now()
    if now.Sub(lastLogTime) < 2*time.Second { // 改为2秒间隔，减少日志量
        return
    }
    lastLogTime = now
    logger.Printf(format, v...)
}

// 全局HTTP客户端，优化连接池管理以避免"too many open files"
var globalClient = &http.Client{
    Transport: &http.Transport{
        TLSClientConfig:     &tls.Config{InsecureSkipVerify: true}, // 测试用，生产应移除
        MaxIdleConns:        50,           // 减少到50，平衡性能和资源
        MaxIdleConnsPerHost: 20,           // 每个主机20个空闲连接
        IdleConnTimeout:     15 * time.Second, // 15秒超时，防止连接堆积
        DisableKeepAlives:   false,        // 保持长连接
        TLSHandshakeTimeout: 2 * time.Second, // TLS握手超时
    },
    Timeout: 8 * time.Second, // 整体超时设为8秒，给足够响应时间但不过长
}

const validAPIKey = "my-secret-key" // API密钥

// handleGets 处理本地缓存的异步获取请求
func handleGets(localCache *cache.Cache, ch chan GetRequest) {
    for req := range ch {
        value, exists := localCache.Get(req.key)
        req.result <- struct {
            value  string
            exists bool
        }{value, exists}
    }
}

// forwardRequest 转发请求，增加循环检测和重试机制
func forwardRequest(method, url, apiKey, key, value string) ([]Response, error) {
    maxRetries := 3
    for i := 0; i < maxRetries; i++ {
        var req *http.Request
        var err error

        // 统一使用IP地址，避免localhost和127.0.0.1不一致
        url = strings.Replace(url, "localhost", "127.0.0.1", 1)
        if !strings.HasPrefix(url, "https://") {
            url = "https://" + url
        }
        logWithRateLimit(infoLogger, "Forwarding request to: %s (attempt %d/%d)", url, i+1, maxRetries)

        // 根据方法构造请求
        if method == http.MethodGet {
            req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
        } else if method == http.MethodPost {
            if strings.Contains(url, "/batch_set") {
                req, err = http.NewRequest(method, url+"?api_key="+apiKey, bytes.NewBuffer([]byte(value)))
                req.Header.Set("Content-Type", "application/json")
            } else {
                body := map[string]string{"key": key, "value": value}
                jsonBody, _ := json.Marshal(body)
                req, err = http.NewRequest(method, url+"?api_key="+apiKey, bytes.NewBuffer(jsonBody))
                req.Header.Set("Content-Type", "application/json")
            }
        } else if method == http.MethodDelete {
            req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
        }
        if err != nil {
            logWithRateLimit(errorLogger, "Failed to create request for %s: %v", url, err)
            return nil, err
        }

        // 添加转发标记，防止循环
        req.Header.Set("X-Forwarded", "true")

        start := time.Now()
        resp, err := globalClient.Do(req)
        duration := time.Since(start)
        if err != nil {
            logWithRateLimit(errorLogger, "Forward request to %s failed after %v: %v", url, duration, err)
            if i < maxRetries-1 {
                time.Sleep(time.Duration(1<<i) * 500 * time.Millisecond) // 指数退避：0.5s, 1s, 2s
            }
            continue
        }
        defer resp.Body.Close() // 确保连接关闭，释放文件句柄
        logWithRateLimit(infoLogger, "Forward request to %s succeeded in %v", url, duration)

        body, err := io.ReadAll(resp.Body)
        if err != nil {
            logWithRateLimit(errorLogger, "Failed to read response body from %s: %v", url, err)
            return nil, err
        }

        var responses []Response
        err = json.Unmarshal(body, &responses)
        if err != nil {
            var singleResp Response
            if jsonErr := json.Unmarshal(body, &singleResp); jsonErr == nil {
                responses = []Response{singleResp}
            } else {
                logWithRateLimit(errorLogger, "Failed to unmarshal response from %s: %v", url, err)
                return nil, err
            }
        }
        return responses, nil
    }
    return nil, fmt.Errorf("failed to forward request to %s after %d retries", url, maxRetries)
}

func main() {
    port := flag.String("port", "8080", "server port")
    flag.Parse()

    // 初始化日志文件
    logFile, err := os.OpenFile(fmt.Sprintf("cache_%s.log", *port), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatalf("Failed to open log file: %v", err)
    }
    defer logFile.Close()

    infoLogger = log.New(logFile, fmt.Sprintf("[INFO localhost:%s] ", *port), log.LstdFlags)
    errorLogger = log.New(logFile, fmt.Sprintf("[ERROR localhost:%s] ", *port), log.LstdFlags)
    log.SetFlags(log.LstdFlags | log.Lshortfile)
    logWithRateLimit(infoLogger, "启动分布式缓存服务器...")

    // 初始化本地缓存和异步获取通道
    localCache := cache.NewCache(100)
    getCh := make(chan GetRequest, 10)
    go handleGets(localCache, getCh)

    // 初始化一致性哈希环，确保节点地址统一为IP格式
    ring := cache.NewHashRing(100)
    ring.AddNode("127.0.0.1:8080")
    ring.AddNode("127.0.0.1:8081")
    ring.AddNode("127.0.0.1:8082")

    localAddr := fmt.Sprintf("127.0.0.1:%s", *port) // 统一使用IP地址，避免localhost不一致
    nodes := ring.GetNodes()

    // 初始化心跳检测，优化间隔和错误处理在heartbeat.go中实现
    hb = cache.NewHeartbeat(ring, localAddr, validAPIKey, infoLogger, errorLogger, localCache)
    hb.Start()

    // 健康检查端点
    http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        apiKey := r.URL.Query().Get("api_key")
        if apiKey != validAPIKey {
            logWithRateLimit(errorLogger, "Invalid API key for health check: %s", apiKey)
            w.WriteHeader(http.StatusUnauthorized)
            json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
            return
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(Response{Status: "ok"})
    })

    // 数据同步端点，避免循环转发
    http.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
        apiKey := r.URL.Query().Get("api_key")
        if apiKey != validAPIKey {
            logWithRateLimit(errorLogger, "Invalid API key: %s", apiKey)
            w.WriteHeader(http.StatusUnauthorized)
            json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
            return
        }
        var body map[string]string
        if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Invalid request body"})
            return
        }
        key := body["key"]
        value := body["value"]
        if key == "" || value == "" {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Key or value cannot be empty"})
            return
        }
        localCache.Set(key, value)
        logWithRateLimit(infoLogger, "Sync received: key=%s, value=%s", key, value)
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(Response{Status: "ok", Key: key, Value: value})
    })

    // broadcast 广播数据到其他节点，控制并发避免请求洪水
    broadcast := func(nodes []string, localAddr, apiKey, key, value string) {
        workerPool := make(chan struct{}, 2) // 限制并发为2个
        var wg sync.WaitGroup
        for _, n := range nodes {
            if n != localAddr {
                wg.Add(1)
                workerPool <- struct{}{}
                go func(node string) {
                    defer wg.Done()
                    defer func() { <-workerPool }()
                    _, err := forwardRequest(http.MethodPost, node+"/sync", apiKey, key, value)
                    if err != nil {
                        logWithRateLimit(errorLogger, "Failed to sync to %s: %v", node, err)
                    }
                    time.Sleep(200 * time.Millisecond) // 批次间延迟，减轻压力
                }(n)
            }
        }
        wg.Wait()
    }

    // 设置键值对端点，防止自转发循环
    http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        defer func() {
            duration := time.Since(start).Seconds()
            totalRequests := cache.CacheRequests.Value()
            if totalRequests > 0 {
                setLatency.Set((setLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests))
            }
        }()

        apiKey := r.URL.Query().Get("api_key")
        if apiKey != validAPIKey {
            logWithRateLimit(errorLogger, "Invalid API key: %s", apiKey)
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusUnauthorized)
            json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
            return
        }

        bodyBytes, err := io.ReadAll(r.Body)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Failed to read request body"})
            return
        }
        r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

        var body map[string]string
        if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Invalid request body"})
            return
        }
        key := body["key"]
        value := body["value"]

        node := ring.GetNode(key)
        w.Header().Set("Content-Type", "application/json")
        if key == "" || value == "" {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Key or value cannot be empty"})
            return
        }

        // 检查是否本地处理，防止自转发循环
        logWithRateLimit(infoLogger, "Set request: key=%s, node=%s, localAddr=%s, X-Forwarded=%s", key, node, localAddr, r.Header.Get("X-Forwarded"))
        if node == localAddr || r.Header.Get("X-Forwarded") == "true" {
            logWithRateLimit(infoLogger, "Set request handled locally: key=%s, value=%s", key, value)
            localCache.Set(key, value)
            go broadcast(nodes, localAddr, apiKey, key, value)
            json.NewEncoder(w).Encode(Response{Status: "ok", Key: key, Value: value})
            return
        }

        // 转发请求，标记已转发
        resp, err := forwardRequest(http.MethodPost, "https://"+node+"/set", apiKey, key, value)
        if err != nil {
            logWithRateLimit(errorLogger, "Failed to forward set request to %s: %v", node, err)
            w.WriteHeader(http.StatusInternalServerError)
            json.NewEncoder(w).Encode(Response{Error: "Failed to forward request"})
            return
        }
        localCache.Set(key, value)
        json.NewEncoder(w).Encode(resp[0])
    })

    // 获取键值对端点，类似处理
    http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        defer func() {
            duration := time.Since(start).Seconds()
            totalRequests := cache.CacheRequests.Value()
            if totalRequests > 0 {
                getLatency.Set((getLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests))
            }
        }()

        apiKey := r.URL.Query().Get("api_key")
        if apiKey != validAPIKey {
            logWithRateLimit(errorLogger, "Invalid API key: %s", apiKey)
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusUnauthorized)
            json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
            return
        }
        key := r.URL.Query().Get("key")
        node := ring.GetNode(key)
        logWithRateLimit(infoLogger, "Get request: key=%s, target node=%s", key, node)
        w.Header().Set("Content-Type", "application/json")
        if key == "" {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Key cannot be empty"})
            return
        }

        if node == localAddr || r.Header.Get("X-Forwarded") == "true" {
            resultCh := make(chan struct {
                value  string
                exists bool
            })
            getCh <- GetRequest{key, resultCh}
            result := <-resultCh
            if result.exists {
                json.NewEncoder(w).Encode(Response{Status: "ok", Key: key, Value: result.value})
            } else {
                json.NewEncoder(w).Encode(Response{Status: "not found", Key: key})
            }
        } else {
            resp, err := forwardRequest(http.MethodGet, node+"/get", apiKey, key, "")
            if err != nil {
                logWithRateLimit(errorLogger, "Failed to forward get request to %s: %v", node, err)
                w.WriteHeader(http.StatusInternalServerError)
                json.NewEncoder(w).Encode(Response{Error: "Failed to fetch from target node"})
                return
            }
            json.NewEncoder(w).Encode(resp[0])
        }
    })

    // 删除键值对端点
    http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
        apiKey := r.URL.Query().Get("api_key")
        if apiKey != validAPIKey {
            logWithRateLimit(errorLogger, "Invalid API key: %s", apiKey)
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusUnauthorized)
            json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
            return
        }
        key := r.URL.Query().Get("key")
        node := ring.GetNode(key)
        logWithRateLimit(infoLogger, "Delete request: key=%s, node=%s", key, node)
        w.Header().Set("Content-Type", "application/json")
        if key == "" {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Key cannot be empty"})
            return
        }

        if node == localAddr || r.Header.Get("X-Forwarded") == "true" {
            ch := make(chan error)
            go func() {
                localCache.Delete(key)
                ch <- nil
            }()
            if err := <-ch; err != nil {
                logWithRateLimit(errorLogger, "Failed to delete key: %v", err)
                w.WriteHeader(http.StatusInternalServerError)
                json.NewEncoder(w).Encode(Response{Error: "Failed to delete key"})
                return
            }
            json.NewEncoder(w).Encode(Response{Status: "ok", Key: key})
        } else {
            resp, err := forwardRequest(http.MethodDelete, node+"/delete", apiKey, key, "")
            if err != nil {
                logWithRateLimit(errorLogger, "Failed to forward delete request: %v", err)
                w.WriteHeader(http.StatusInternalServerError)
                json.NewEncoder(w).Encode(Response{Error: "Failed to forward request"})
                return
            }
            json.NewEncoder(w).Encode(resp[0])
        }
    })

    // 批量获取端点
    http.HandleFunc("/batch_get", func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        defer func() {
            duration := time.Since(start).Seconds()
            totalRequests := cache.CacheRequests.Value()
            if totalRequests > 0 {
                getLatency.Set((getLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests))
            }
        }()

        apiKey := r.URL.Query().Get("api_key")
        if apiKey != validAPIKey {
            logWithRateLimit(errorLogger, "Invalid API key: %s", apiKey)
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusUnauthorized)
            json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
            return
        }

        keysParam := r.URL.Query().Get("keys")
        if keysParam == "" {
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Keys cannot be empty"})
            return
        }

        keys := strings.Split(keysParam, ",")
        w.Header().Set("Content-Type", "application/json")

        if ring.GetNode(keys[0]) == localAddr || r.Header.Get("X-Forwarded") == "true" {
            results := make([]Response, len(keys))
            for i, key := range keys {
                resultCh := make(chan struct {
                    value  string
                    exists bool
                })
                getCh <- GetRequest{key, resultCh}
                result := <-resultCh
                if result.exists {
                    results[i] = Response{Status: "ok", Key: key, Value: result.value}
                } else {
                    results[i] = Response{Status: "not found", Key: key}
                }
            }
            logWithRateLimit(infoLogger, "Batch get request: keys=%v, target node=%s", keys, localAddr)
            json.NewEncoder(w).Encode(results)
        } else {
            node := ring.GetNode(keys[0])
            logWithRateLimit(infoLogger, "Batch get request forwarded: keys=%v, target node=%s", keys, node)
            url := fmt.Sprintf("%s/batch_get?api_key=%s&keys=%s", node, apiKey, keysParam)
            resp, err := globalClient.Get(url)
            if err != nil {
                logWithRateLimit(errorLogger, "Failed to forward batch get to %s: %v", node, err)
                w.WriteHeader(http.StatusInternalServerError)
                json.NewEncoder(w).Encode(Response{Error: "Failed to fetch from target node"})
                return
            }
            defer resp.Body.Close() // 确保关闭连接
            body, err := io.ReadAll(resp.Body)
            if err != nil {
                logWithRateLimit(errorLogger, "Failed to read batch get response: %v", err)
                w.WriteHeader(http.StatusInternalServerError)
                json.NewEncoder(w).Encode(Response{Error: "Failed to read response"})
                return
            }
            var results []Response
            if err := json.Unmarshal(body, &results); err != nil {
                logWithRateLimit(errorLogger, "Failed to unmarshal batch get response: %v", err)
                w.WriteHeader(http.StatusInternalServerError)
                json.NewEncoder(w).Encode(Response{Error: "Invalid response format"})
                return
            }
            json.NewEncoder(w).Encode(results)
        }
    })

    // 批量设置端点，分批处理避免请求洪水
    http.HandleFunc("/batch_set", func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        defer func() {
            duration := time.Since(start).Seconds()
            totalRequests := cache.CacheRequests.Value()
            if totalRequests > 0 {
                setLatency.Set((setLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests))
            }
        }()

        apiKey := r.URL.Query().Get("api_key")
        if apiKey != validAPIKey {
            logWithRateLimit(errorLogger, "Invalid API key: %s", apiKey)
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusUnauthorized)
            json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
            return
        }

        if r.Method != http.MethodPost {
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusMethodNotAllowed)
            json.NewEncoder(w).Encode(Response{Error: "Method not allowed"})
            return
        }

        var pairs []struct {
            Key   string `json:"key"`
            Value string `json:"value"`
        }
        if err := json.NewDecoder(r.Body).Decode(&pairs); err != nil {
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Invalid request body"})
            return
        }

        if len(pairs) == 0 {
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Pairs cannot be empty"})
            return
        }

        w.Header().Set("Content-Type", "application/json")

        // 检查是否本地处理，防止循环
        node := ring.GetNode(pairs[0].Key)
        if node == localAddr || r.Header.Get("X-Forwarded") == "true" {
            results := make([]Response, len(pairs))
            for i, pair := range pairs {
                if pair.Key == "" || pair.Value == "" {
                    results[i] = Response{Error: "Key or value cannot be empty"}
                } else {
                    localCache.Set(pair.Key, pair.Value)
                    results[i] = Response{Status: "ok", Key: pair.Key}
                }
            }
            logWithRateLimit(infoLogger, "Batch set local: pairs=%v", pairs)
            json.NewEncoder(w).Encode(results)
            go broadcast(nodes, localAddr, apiKey, pairs[0].Key, pairs[0].Value) // 仅广播第一个键值对作为示例
            return
        }

        // 分批转发，减轻压力
        batchSize := 50
        results := make([]Response, len(pairs))
        resultIdx := 0
        for i := 0; i < len(pairs); i += batchSize {
            end := i + batchSize
            if end > len(pairs) {
                end = len(pairs)
            }
            batch := pairs[i:end]
            jsonBody, err := json.Marshal(batch)
            if err != nil {
                logWithRateLimit(errorLogger, "Failed to marshal batch: %v", err)
                w.WriteHeader(http.StatusInternalServerError)
                json.NewEncoder(w).Encode(Response{Error: "Failed to serialize request"})
                return
            }
            forwardedResults, err := forwardRequest(http.MethodPost, "https://"+node+"/batch_set", apiKey, "", string(jsonBody))
            if err != nil {
                logWithRateLimit(errorLogger, "Failed to forward batch set to %s: %v", node, err)
                w.WriteHeader(http.StatusInternalServerError)
                json.NewEncoder(w).Encode(Response{Error: "Failed to forward request"})
                return
            }
            for _, r := range forwardedResults {
                results[resultIdx] = r
                resultIdx++
            }
            time.Sleep(200 * time.Millisecond) // 批次间延迟
        }
        json.NewEncoder(w).Encode(results)
    })

    // 配置HTTP服务器
    server := &http.Server{
        Addr: ":" + *port,
        TLSConfig: &tls.Config{
            InsecureSkipVerify:       true, // 测试用，生产应移除
            ClientSessionCache:       tls.NewLRUClientSessionCache(5000),
            SessionTicketsDisabled:   false,
            MinVersion:               tls.VersionTLS12,
            PreferServerCipherSuites: true,
            CurvePreferences:         []tls.CurveID{tls.X25519, tls.CurveP256},
        },
        ReadTimeout:  10 * time.Second,
        WriteTimeout: 10 * time.Second,
        IdleTimeout:  20 * time.Second,
    }

    fmt.Printf("Server running on https://localhost:%s\n", *port)
    server.Handler = gzipHandler(http.DefaultServeMux)
    err = server.ListenAndServeTLS("cert.pem", "key.pem")
    if err != nil {
        log.Fatalf("Failed to start TLS server: %v", err)
    }
}