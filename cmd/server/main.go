package main

import (
	"bytes" //处理POST请求的字节缓冲区
	"compress/gzip"
	"crypto/tls"
	"encoding/json"    //处理JSON数据
	"expvar"           //暴露指标
	"flag"             //解析命令行参数
	"fmt"              //格式化输出
	"io"               //读取 HTTP 响应体
	"log"              //日志记录
	"net/http"         //实现HTTP服务器和客户端
	_ "net/http/pprof" // 引入 pprof，注册 /debug/pprof 端点
	"os"               //文件操作
	"strings"          //检查字符串前缀
	"sync"
	"time" //时间处理

	"github.com/quqxiaoli/distributed-cache/pkg/cache"
)

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

// 定义请求延迟指标
var (
	getLatency = expvar.NewFloat("get_latency") // /get 请求平均延迟（秒）
	setLatency = expvar.NewFloat("set_latency") // /set 请求平均延迟（秒）
)

// HTTP相应的JSON结构
type Response struct {
	Status string `json:"status"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
	Error  string `json:"error,omitempty"`
}

// 封装/get请求的参数，用于并发处理
type GetRequest struct {
	key    string
	result chan struct { //结果通道，返回查询结果
		value  string
		exists bool
	}
}

// 全局日志变量，延迟初始化
var (
	infoLogger  *log.Logger      //普通日志
	errorLogger *log.Logger      //错误日志
	hb          *cache.Heartbeat // Heartbeat 实例，用于健康检查
	// 新增：日志限流计数器
	lastLogTime time.Time
	logMutex    sync.Mutex
)

// 新增：限制日志频率，每秒最多记录一次
func logWithRateLimit(logger *log.Logger, format string, v ...interface{}) {
	logMutex.Lock()
	defer logMutex.Unlock()
	now := time.Now()
	if now.Sub(lastLogTime) < 1*time.Second {
		return
	}
	lastLogTime = now
	logger.Printf(format, v...)
}

var globalClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     30 * time.Second,
		DisableKeepAlives:   false,
		TLSHandshakeTimeout: 2 * time.Second,
	},
	Timeout: 10 * time.Second,
}

const validAPIKey = "my-secret-key" //密匙

// 后台go协程处理/get请求，减少锁竞争
func handleGets(localCache *cache.Cache, ch chan GetRequest) {
	for req := range ch {
		value, exists := localCache.Get(req.key) //从本地缓存查询键
		req.result <- struct {                   //将结果发送到通道
			value  string
			exists bool
		}{value, exists}
	}
}

// forwardRequest 转发 HTTP 请求到其他节点
func forwardRequest(method, url, apiKey, key, value string) ([]Response, error) {
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		var req *http.Request // 请求对象：待构造的 HTTP 请求
		var err error         // 错误变量：记录构造或发送请求时的错误

		// 使用 IP 地址替代 localhost，避免 DNS 解析问题
		url = strings.Replace(url, "localhost", "127.0.0.1", 1)
		// 确保 URL 以 https:// 开头
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			url = "https://" + url
		}
		logWithRateLimit(infoLogger, "Forwarding request to: %s", url) // 优化：使用限流日志

		// 根据请求方法构造不同的 HTTP 请求
		if method == http.MethodGet { // GET 请求：查询键值
			req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
		} else if method == http.MethodPost { // POST 请求：设置或同步键值
			if strings.Contains(url, "/batch_set") {
				req, err = http.NewRequest(method, url+"?api_key="+apiKey, bytes.NewBuffer([]byte(value)))
				req.Header.Set("Content-Type", "application/json")
			} else {
				body := map[string]string{"key": key, "value": value}
				jsonBody, _ := json.Marshal(body)
				req, err = http.NewRequest(method, url+"?api_key="+apiKey, bytes.NewBuffer(jsonBody))
				req.Header.Set("Content-Type", "application/json")
				if strings.HasSuffix(url, "/set") {
					req.Header.Set("X-Forwarded", "true")
				}
			}
		} else if method == http.MethodDelete {
			req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
		}
		if err != nil {
			logWithRateLimit(errorLogger, "Failed to create request for %s: %v", url, err)
			return nil, err
		}

		start := time.Now()
		resp, err := globalClient.Do(req)
		duration := time.Since(start)
		if err != nil {
			logWithRateLimit(errorLogger, "Forward request to %s failed after %v: %v", url, duration, err)
			if i < maxRetries-1 {
				time.Sleep(500 * time.Millisecond)
			}
			continue
		}
		logWithRateLimit(infoLogger, "Forward request to %s succeeded in %v", url, duration)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			logWithRateLimit(errorLogger, "Failed to read response body from %s: %v", url, err)
			return nil, err
		}

		var responses []Response
		err = json.Unmarshal(body, &responses)
		if err != nil {
			// 兼容单响应情况
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
	//解析命令行参数
	port := flag.String("port", "8080", "server port") //参数：服务器端口，默认8080
	flag.Parse()                                       //解析命令行输入

	//创建日志文件，按端口命名
	logFile, err := os.OpenFile(fmt.Sprintf("cache_%s.log", *port), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	infoLogger = log.New(logFile, fmt.Sprintf("[INFO localhost:%s] ", *port), log.LstdFlags)
	errorLogger = log.New(logFile, fmt.Sprintf("[ERROR localhost:%s] ", *port), log.LstdFlags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	logWithRateLimit(infoLogger, "启动分布式缓存服务器...")

	localCache := cache.NewCache(100)  //创建本地缓存
	getCh := make(chan GetRequest, 10) //带缓冲通道用于/get请求
	go handleGets(localCache, getCh)   //启动go协程处理并发查询

	// 修改：增加虚拟节点数为 100
	ring := cache.NewHashRing(100)
	ring.AddNode("127.0.0.1:8080") // 使用 IP 替代 localhost
	ring.AddNode("127.0.0.1:8081")
	ring.AddNode("127.0.0.1:8082")

	localAddr := fmt.Sprintf("127.0.0.1:%s", *port) // 修改：动态设置 localAddr
	nodes := ring.GetNodes()                        // 直接用 GetNodes。

	// 初始化心跳检测
	hb = cache.NewHeartbeat(ring, localAddr, validAPIKey, infoLogger, errorLogger, localCache)
	hb.Start()

	// 新增：健康检查接口
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

	//处理其他节点的同步请求
	http.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.URL.Query().Get("api_key")
		if apiKey != validAPIKey {
			logWithRateLimit(errorLogger, "Invalid API key: %s", apiKey)
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
			return
		}
		var body map[string]string                                    //存储请求体
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil { //从请求体读取JSON
			w.WriteHeader(http.StatusBadRequest) //400错误
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
		localCache.Set(key, value)                                                  //存入本地缓存
		logWithRateLimit(infoLogger, "Sync received: key=%s, value=%s", key, value) // 区分日志
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Status: "ok", Key: key, Value: value})
	})

	// 优化：异步广播goroutine池
	broadcast := func(nodes []string, localAddr, apiKey, key, value string) {
		workerPool := make(chan struct{}, 2) // 优化：限制为2个并发
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
					time.Sleep(100 * time.Millisecond) // 优化：间隔100ms，避免瞬时压力
				}(n)
			}
		}
		wg.Wait()
	}

	//处理set请求
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now() // 记录请求开始时间
		defer func() {
			duration := time.Since(start).Seconds()      // 计算本次请求耗时
			totalRequests := cache.CacheRequests.Value() // 获取总请求次数
			if totalRequests > 0 {                       // 避免除以零
				// 更新平均延迟：(旧平均值 * (n-1) + 新值) / n
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

		bodyBytes, err := io.ReadAll(r.Body) // 读取请求体
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(Response{Error: "Failed to read request body"})
			return
		}
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // 重置Body

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

		// 强制本地处理检测
		logWithRateLimit(infoLogger, "Node=%s, LocalAddr=%s, X-Forwarded=%s", node, localAddr, r.Header.Get("X-Forwarded"))
		if node == localAddr || r.Header.Get("X-Forwarded") == "true" {
			logWithRateLimit(infoLogger, "Set request handled locally: key=%s, value=%s, node=%s", key, value, node)
			localCache.Set(key, value)
			go broadcast(nodes, localAddr, apiKey, key, value)
			json.NewEncoder(w).Encode(Response{Status: "ok", Key: key, Value: value})
			return
		}

		// 添加 X-Forwarded 头
		r.Header.Set("X-Forwarded", "true")
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

	//处理get请求
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now() // 记录请求开始时间
		defer func() {
			duration := time.Since(start).Seconds()      // 计算本次请求耗时
			totalRequests := cache.CacheRequests.Value() // 获取总请求次数
			if totalRequests > 0 {                       // 避免除以零
				// 更新平均延迟：(旧平均值 * (n-1) + 新值) / n
				getLatency.Set((getLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests)) // 优化：修正为getLatency
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
		node := ring.GetNode(key) // 使用一致性哈希找到目标节点
		logWithRateLimit(infoLogger, "Get request: key=%s, target node=%s", key, node)
		w.Header().Set("Content-Type", "application/json")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(Response{Error: "Key cannot be empty"})
			return
		}

		if node == localAddr { // 如果目标节点是本地
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
		} else { // 如果目标节点不是本地，转发请求
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

	//处理delete请求
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

		if node == localAddr {
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

		// 本地处理所有键
		if ring.GetNode(keys[0]) == localAddr { // 假设所有键在同一节点
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
			// 转发到目标节点
			node := ring.GetNode(keys[0]) // 简化：假设所有键在同一节点
			logWithRateLimit(infoLogger, "Batch get request forwarded: keys=%v, target node=%s", keys, node)
			url := fmt.Sprintf("%s/batch_get?api_key=%s&keys=%s", node, apiKey, keysParam)
			resp, err := globalClient.Get(url)
			if err != nil {
				logWithRateLimit(errorLogger, "Failed to forward batch get to %s: %v", node, err)
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(Response{Error: "Failed to fetch from target node"})
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				logWithRateLimit(errorLogger, "Failed to read batch get response: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(Response{Error: "Failed to read response"})
				return
			}
			// 修复：解析为 []Response
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
		// 检查是否为同步请求，避免循环
		if r.Header.Get("X-Sync") == "true" {
			var pairs []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&pairs); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(Response{Error: "Invalid request body"})
				return
			}
			results := make([]Response, len(pairs))
			for i, pair := range pairs {
				if pair.Key == "" || pair.Value == "" {
					results[i] = Response{Error: "Key or value cannot be empty"}
				} else {
					localCache.Set(pair.Key, pair.Value)
					results[i] = Response{Status: "ok", Key: pair.Key}
				}
			}
			logWithRateLimit(infoLogger, "Sync batch set received: pairs=%v", pairs)
			json.NewEncoder(w).Encode(results)
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

		nodePairs := make(map[string][]struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		})
		for _, pair := range pairs {
			node := ring.GetNode(pair.Key)
			nodePairs[node] = append(nodePairs[node], pair)
		}

		results := make([]Response, len(pairs))
		resultIdx := 0

		for node, group := range nodePairs {
			if node == localAddr {
				for _, pair := range group {
					if pair.Key == "" || pair.Value == "" {
						results[resultIdx] = Response{Error: "Key or value cannot be empty"}
					} else {
						localCache.Set(pair.Key, pair.Value)
						results[resultIdx] = Response{Status: "ok", Key: pair.Key}
					}
					resultIdx++
				}
				logWithRateLimit(infoLogger, "Batch set local: pairs=%v", group)
			} else {
				jsonBody, err := json.Marshal(group)
				if err != nil {
					logWithRateLimit(errorLogger, "Failed to marshal group for %s: %v", node, err)
					w.WriteHeader(http.StatusInternalServerError)
					json.NewEncoder(w).Encode(Response{Error: "Failed to serialize request"})
					return
				}
				forwardedResults, err := forwardRequest(http.MethodPost, node+"/batch_set", apiKey, "", string(jsonBody))
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
			}
		}

		json.NewEncoder(w).Encode(results)
	})

	server := &http.Server{
		Addr: ":" + *port,
		TLSConfig: &tls.Config{
			InsecureSkipVerify:       true,
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
