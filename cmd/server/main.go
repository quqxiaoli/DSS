package main

import (
	"bytes" //处理POST请求的字节缓冲区
	"crypto/tls"
	"encoding/json" //处理JSON数据
	"flag"          //解析命令行参数
	"fmt"           //格式化输出
	"io"            //读取 HTTP 响应体
	"log"           //日志记录
	"net/http"      //实现HTTP服务器和客户端
	"os"            //文件操作
	"strings"       //检查字符串前缀
	"time"          //时间处理
    "expvar"        //暴露指标
	"github.com/quqxiaoli/distributed-cache/pkg/cache"
)

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
	infoLogger  *log.Logger //普通日志
	errorLogger *log.Logger //错误日志
)

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
func forwardRequest(method, url, apiKey, key, value string) (*Response, error) {
	var req *http.Request // 请求对象：待构造的 HTTP 请求
	var err error         // 错误变量：记录构造或发送请求时的错误

	// 确保 URL 以 https:// 开头
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "https://" + url
	}
	infoLogger.Printf("Forwarding request to: %s", url) // 新增：记录转发目标 URL

	// 根据请求方法构造不同的 HTTP 请求
	if method == http.MethodGet { // GET 请求：查询键值
		req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
	} else if method == http.MethodPost { // POST 请求：设置或同步键值
		body := map[string]string{"key": key, "value": value} // 请求体：键值对
		jsonBody, _ := json.Marshal(body)                     // 转换为 JSON
		req, err = http.NewRequest(method, url+"?api_key="+apiKey, bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json") // 设置请求头：指定 JSON 格式
		if strings.HasSuffix(url, "/set") {                // 新增：只对 /set 请求添加 X-Forwarded
			req.Header.Set("X-Forwarded", "true")
		}
	} else if method == http.MethodDelete { // DELETE 请求：删除键值
		req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
	}
	if err != nil { // 检查请求构造是否出错
		errorLogger.Printf("Failed to create request for %s: %v", url, err)
		return nil, err
	}

	// 创建 HTTP 客户端
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // 测试时跳过证书验证
		},
		Timeout: 5 * time.Second, // 设置超时时间为 5 秒
	}

	// 发送请求并计时
	start := time.Now()           // 新增：记录请求开始时间
	resp, err := client.Do(req)   // 发送请求并获取响应
	duration := time.Since(start) // 新增：计算请求耗时
	if err != nil {               // 检查请求是否失败
		errorLogger.Printf("Forward request to %s failed after %v: %v", url, duration, err)
		return nil, err
	}
	infoLogger.Printf("Forward request to %s succeeded in %v", url, duration) // 新增：记录成功信息

	// 处理响应
	defer resp.Body.Close()            // 延迟关闭响应体
	body, err := io.ReadAll(resp.Body) // 读取响应体内容
	if err != nil {                    // 检查读取是否出错
		errorLogger.Printf("Failed to read response body from %s: %v", url, err)
		return nil, err
	}

	// 解析响应
	var response Response                 // 存储反序列化结果
	err = json.Unmarshal(body, &response) // 将响应体反序列化为 Response 结构体
	if err != nil {                       // 检查解析是否出错
		errorLogger.Printf("Failed to unmarshal response from %s: %v", url, err)
		return nil, err
	}

	return &response, err // 返回响应和可能的错误
}

func main() {
	//解析命令行参数
	port := flag.String("port", "8080", "server port") //参数：服务器端口，默认8080
	flag.Parse()                                       //解析命令行输入

	//创建日志文件，按端口命名
	logFile, err := os.OpenFile(fmt.Sprintf("cache_%s.log", *port), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil { //检查文件打开是否失败
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	// 修改：前缀使用端口号，确保与文件名一致
	infoLogger = log.New(logFile, fmt.Sprintf("[INFO localhost:%s] ", *port), log.LstdFlags)
	errorLogger = log.New(logFile, fmt.Sprintf("[ERROR localhost:%s] ", *port), log.LstdFlags)
	log.SetFlags(log.LstdFlags | log.Lshortfile) //日志格式包含时间和文件行号
	infoLogger.Printf("启动分布式缓存服务器...")

	localCache := cache.NewCache(100)  //创建本地缓存
	getCh := make(chan GetRequest, 10) //带缓冲通道用于/get请求
	go handleGets(localCache, getCh)   //启动go协程处理并发查询

	// 修改：增加虚拟节点数为 100
	ring := cache.NewHashRing(100)
	ring.AddNode("localhost:8080")
	ring.AddNode("localhost:8081")
	ring.AddNode("localhost:8082")

	localAddr := fmt.Sprintf("localhost:%s", *port) // 修改：动态设置 localAddr
	nodes := ring.GetNodes()                        // 直接用 GetNodes。

	// 新增：健康检查接口
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.URL.Query().Get("api_key")
		if apiKey != validAPIKey {
			errorLogger.Printf("Invalid API key for health check: %s", apiKey)
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
			errorLogger.Printf("Invalid API key: %s", apiKey)
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
		localCache.Set(key, value)                                       //存入本地缓存
		infoLogger.Printf("Sync received: key=%s, value=%s", key, value) // 区分日志
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(Response{Status: "ok", Key: key, Value: value})
	})

	//处理set请求
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now() // 记录请求开始时间
        defer func() {
            duration := time.Since(start).Seconds()                  // 计算本次请求耗时
            totalRequests := cache.CacheRequests.Value()             // 获取总请求次数
            if totalRequests > 0 {                                   // 避免除以零
                // 更新平均延迟：(旧平均值 * (n-1) + 新值) / n
                getLatency.Set((getLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests))
            }
        }()
		
		apiKey := r.URL.Query().Get("api_key")
		if apiKey != validAPIKey {
			errorLogger.Printf("Invalid API key: %s", apiKey)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
			return
		}

		var key, value string
		if r.Method == http.MethodPost {
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(Response{Error: "Invalid request body"})
				return
			}
			key = body["key"]
			value = body["value"]
		} else {
			key = r.URL.Query().Get("key")
			value = r.URL.Query().Get("value")
		}

		node := ring.GetNode(key) //用一致性哈希确定目标节点
		w.Header().Set("Content-Type", "application/json")
		if key == "" || value == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(Response{Error: "Key or value cannot be empty"})
			return
		}

		if node == localAddr { //如果当前节点是目标节点
			infoLogger.Printf("Set request: key=%s, value=%s, target node=%s", key, value, node)
			localCache.Set(key, value) // 先存本地。
			go func() {                //异步广播给其他节点
				for _, n := range nodes { //遍历节点
					if n != localAddr {
						maxRetries := 3
						for i := 0; i < maxRetries; i++ {
							start := time.Now()
							_, err := forwardRequest(http.MethodPost, n+"/sync", apiKey, key, value)
							duration := time.Since(start)
							if err == nil {
								infoLogger.Printf("Sync to %s succeeded in %v", n, duration) // 修改：添加耗时
								break
							}
							// 记录：失败时打印尝试次数和错误。
							errorLogger.Printf("Failed to sync to %s (attempt %d/%d): %v, duration: %v", n, i+1, maxRetries, err, duration) // 修改：详细错误
							if i < maxRetries-1 {                                                                                           // 检查：不是最后一次则等待。
								time.Sleep(1 * time.Second) // 等待：1 秒后重试。
							}
						}
					}
				}
			}()
			json.NewEncoder(w).Encode(Response{Status: "ok", Key: key, Value: value})
		} else { //若当前节点不是目标节点
			resp, err := forwardRequest(http.MethodPost, node+"/set", apiKey, key, value) //转发给目标节点
			if err != nil {
				errorLogger.Printf("Failed to forward set request: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(Response{Error: "Failed to forward request"})
				return
			}
			localCache.Set(key, value)      // 转发后存本地
			json.NewEncoder(w).Encode(resp) // 响应转发结果
		}
	})

	//处理get请求
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now() // 记录请求开始时间
        defer func() {
            duration := time.Since(start).Seconds()                  // 计算本次请求耗时
            totalRequests := cache.CacheRequests.Value()             // 获取总请求次数
            if totalRequests > 0 {                                   // 避免除以零
                // 更新平均延迟：(旧平均值 * (n-1) + 新值) / n
                setLatency.Set((setLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests))
            }
        }()

		apiKey := r.URL.Query().Get("api_key")
		if apiKey != validAPIKey {
			errorLogger.Printf("Invalid API key: %s", apiKey)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
			return
		}
		key := r.URL.Query().Get("key")
		node := ring.GetNode(key) // 使用一致性哈希找到目标节点
		infoLogger.Printf("Get request: key=%s, target node=%s", key, node)
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
				errorLogger.Printf("Failed to forward get request to %s: %v", node, err)
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(Response{Error: "Failed to fetch from target node"})
				return
			}
			json.NewEncoder(w).Encode(resp)
		}
	})

	//处理delete请求
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
		node := ring.GetNode(key)
		infoLogger.Printf("Delete request: key=%s, node=%s", key, node)
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
				errorLogger.Printf("Failed to delete key: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(Response{Error: "Failed to delete key"})
				return
			}
			json.NewEncoder(w).Encode(Response{Status: "ok", Key: key})
		} else {
			resp, err := forwardRequest(http.MethodDelete, node+"/delete", apiKey, key, "")
			if err != nil {
				errorLogger.Printf("Failed to forward delete request: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(Response{Error: "Failed to forward request"})
				return
			}
			json.NewEncoder(w).Encode(resp)
		}
	})

	// 新增：启动心跳检测
	hb := cache.NewHeartbeat(ring, localAddr, validAPIKey, infoLogger, errorLogger, localCache)
	hb.Start()

	fmt.Printf("Server running on https://localhost:%s\n", *port)
	err = http.ListenAndServeTLS(":"+*port, "cert.pem", "key.pem", nil)
	if err != nil {
		log.Fatalf("Failed to start TLS server: %v", err)
	}
}
