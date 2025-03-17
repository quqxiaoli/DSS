package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/quqxiaoli/distributed-cache/internal/util"
	"github.com/quqxiaoli/distributed-cache/pkg/cache"
)

// GzipHandler 启用GZIP压缩，提升响应效率
func GzipHandler(h http.Handler) http.Handler {
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

// SetupRoutes 设置所有API路由
func SetupRoutes(ring *cache.HashRing, localCache *cache.Cache, getCh chan GetRequest, nodes []string, localAddr, apiKey string, logger *util.Logger, hb *cache.Heartbeat) {
	http.HandleFunc("/health", makeHealthHandler(apiKey, logger, hb, localAddr)) // 传入 localAddr
	http.HandleFunc("/sync", makeSyncHandler(apiKey, localCache, logger))
	http.HandleFunc("/set", makeSetHandler(ring, localCache, nodes, localAddr, apiKey, logger))
	http.HandleFunc("/get", makeGetHandler(ring, getCh, localAddr, apiKey, logger))
	http.HandleFunc("/delete", makeDeleteHandler(ring, localCache, localAddr, apiKey, logger))
	http.HandleFunc("/batch_get", makeBatchGetHandler(ring, getCh, localAddr, apiKey, logger))
	http.HandleFunc("/batch_set", makeBatchSetHandler(ring, localCache, nodes, localAddr, apiKey, logger, hb))
}

func makeHealthHandler(apiKey string, logger *util.Logger, hb *cache.Heartbeat, localAddr string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
        if r.URL.Query().Get("api_key") != apiKey {
            logger.Error("Invalid API key for health check: %s", r.URL.Query().Get("api_key"))
            writeJSON(w, http.StatusUnauthorized, Response{Error: "Invalid API key"}, logger)
            return
        }
        // 检查本地节点健康状态
        if !hb.IsNodeHealthy(localAddr) {
            logger.Error("Local node %s is unhealthy", localAddr)
            writeJSON(w, http.StatusServiceUnavailable, Response{Status: "unhealthy"}, logger)
            return
        }
		writeJSON(w, http.StatusOK, Response{Status: "ok"}, logger)
	}
}

func makeSyncHandler(apiKey string, localCache *cache.Cache, logger *util.Logger) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.URL.Query().Get("api_key") != apiKey {
            logger.Error("Invalid API key: %s", r.URL.Query().Get("api_key"))
            writeJSON(w, http.StatusUnauthorized, Response{Error: "Invalid API key"}, logger)
            return
        }

        var body interface{}
        if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
            writeJSON(w, http.StatusBadRequest, Response{Error: "Invalid request body"}, logger)
            return
        }

        switch v := body.(type) {
        case map[string]interface{}: // 单键值对
            key, ok := v["key"].(string)
            if !ok || key == "" {
                writeJSON(w, http.StatusBadRequest, Response{Error: "Key cannot be empty or invalid"}, logger)
                return
            }
            value, ok := v["value"].(string)
            if !ok || value == "" {
                writeJSON(w, http.StatusBadRequest, Response{Error: "Value cannot be empty or invalid"}, logger)
                return
            }
            logger.InfoNoLimit("Sync received: key=%s, value=%s", key, value)
            localCache.Set(key, value)
            writeJSON(w, http.StatusOK, Response{Status: "ok", Key: key, Value: value}, logger)
        case []interface{}: // 批量键值对
            if len(v) == 0 {
                writeJSON(w, http.StatusBadRequest, Response{Error: "Batch cannot be empty"}, logger)
                return
            }
            results := make([]Response, len(v))
            for i, item := range v {
                pair, ok := item.(map[string]interface{})
                if !ok {
                    results[i] = Response{Error: "Invalid pair format"}
                    continue
                }
                key, ok := pair["key"].(string)
                if !ok || key == "" {
                    results[i] = Response{Error: "Key cannot be empty or invalid"}
                    continue
                }
                value, ok := pair["value"].(string)
                if !ok || value == "" {
                    results[i] = Response{Error: "Value cannot be empty or invalid"}
                    continue
                }
                logger.InfoNoLimit("Sync received: key=%s, value=%s", key, value)
                localCache.Set(key, value)
                results[i] = Response{Status: "ok", Key: key, Value: value}
            }
            writeJSON(w, http.StatusOK, results, logger)
        default:
            writeJSON(w, http.StatusBadRequest, Response{Error: "Unsupported request body format"}, logger)
            return
        }
    }
}

func makeSetHandler(ring *cache.HashRing, localCache *cache.Cache, nodes []string, localAddr, apiKey string, logger *util.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			duration := time.Since(start).Seconds()
			totalRequests := cache.CacheRequests.Value()
			if totalRequests > 0 {
				SetLatency.Set((SetLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests))
			}
		}()

		if r.URL.Query().Get("api_key") != apiKey {
			logger.Error("Invalid API key: %s", r.URL.Query().Get("api_key"))
			writeJSON(w, http.StatusUnauthorized, Response{Error: "Invalid API key"}, logger)
			return
		}

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, Response{Error: "Failed to read request body"}, logger)
			return
		}
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, Response{Error: "Invalid request body"}, logger)
			return
		}
		key := body["key"]
		value := body["value"]
		if key == "" || value == "" {
			writeJSON(w, http.StatusBadRequest, Response{Error: "Key or value cannot be empty"}, logger)
			return
		}

		node := ring.GetNode(key)
		localAddrNormalized := strings.TrimPrefix(localAddr, "https://")
		logger.Info("Set request: key=%s, node=%s, localAddr=%s, X-Forwarded=%s", key, node, localAddrNormalized, r.Header.Get("X-Forwarded"))
		if node == localAddrNormalized || r.Header.Get("X-Forwarded") == "true" {
			logger.Info("Set request handled locally: key=%s, value=%s", key, value)
			localCache.Set(key, value)
			go broadcast(nodes, localAddr, apiKey, key, value, logger)
			writeJSON(w, http.StatusOK, Response{Status: "ok", Key: key, Value: value}, logger)
			return
		}

		resp, err := forwardRequest(http.MethodPost, "https://"+node+"/set", apiKey, key, value, logger)
		if err != nil {
			logger.Error("Failed to forward set request to %s: %v", node, err)
			writeJSON(w, http.StatusInternalServerError, Response{Error: "Failed to forward request"}, logger)
			return
		}
		localCache.Set(key, value)
		writeJSON(w, http.StatusOK, resp[0], logger)
	}
}

func makeGetHandler(ring *cache.HashRing, getCh chan GetRequest, localAddr, apiKey string, logger *util.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			duration := time.Since(start).Seconds()
			totalRequests := cache.CacheRequests.Value()
			if totalRequests > 0 {
				GetLatency.Set((GetLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests))
			}
		}()

		if r.URL.Query().Get("api_key") != apiKey {
			logger.Error("Invalid API key: %s", r.URL.Query().Get("api_key"))
			writeJSON(w, http.StatusUnauthorized, Response{Error: "Invalid API key"}, logger)
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			writeJSON(w, http.StatusBadRequest, Response{Error: "Key cannot be empty"}, logger)
			return
		}

		node := ring.GetNode(key)
		logger.Info("Get request: key=%s, target node=%s", key, node)
		if node == localAddr || r.Header.Get("X-Forwarded") == "true" {
			resultCh := make(chan struct {
				Value  string
				Exists bool
			})
			getCh <- GetRequest{Key: key, Result: resultCh}
			result := <-resultCh
			if result.Exists {
				writeJSON(w, http.StatusOK, Response{Status: "ok", Key: key, Value: result.Value}, logger)
			} else {
				writeJSON(w, http.StatusOK, Response{Status: "not found", Key: key}, logger)
			}
			return
		}

		resp, err := forwardRequest(http.MethodGet, "https://"+node+"/get", apiKey, key, "", logger)
		if err != nil {
			logger.Error("Failed to forward get request to %s: %v", node, err)
			writeJSON(w, http.StatusInternalServerError, Response{Error: "Failed to fetch from target node"}, logger)
			return
		}
		writeJSON(w, http.StatusOK, resp[0], logger)
	}
}

func makeDeleteHandler(ring *cache.HashRing, localCache *cache.Cache, localAddr, apiKey string, logger *util.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("api_key") != apiKey {
			logger.Error("Invalid API key: %s", r.URL.Query().Get("api_key"))
			writeJSON(w, http.StatusUnauthorized, Response{Error: "Invalid API key"}, logger)
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			writeJSON(w, http.StatusBadRequest, Response{Error: "Key cannot be empty"}, logger)
			return
		}

		node := ring.GetNode(key)
		logger.Info("Delete request: key=%s, node=%s", key, node)
		if node == localAddr || r.Header.Get("X-Forwarded") == "true" {
			ch := make(chan error)
			go func() {
				localCache.Delete(key)
				ch <- nil
			}()
			if err := <-ch; err != nil {
				logger.Error("Failed to delete key: %v", err)
				writeJSON(w, http.StatusInternalServerError, Response{Error: "Failed to delete key"}, logger)
				return
			}
			writeJSON(w, http.StatusOK, Response{Status: "ok", Key: key}, logger)
			return
		}

		resp, err := forwardRequest(http.MethodDelete, "https://"+node+"/delete", apiKey, key, "", logger)
		if err != nil {
			logger.Error("Failed to forward delete request: %v", err)
			writeJSON(w, http.StatusInternalServerError, Response{Error: "Failed to forward request"}, logger)
			return
		}
		writeJSON(w, http.StatusOK, resp[0], logger)
	}
}

func makeBatchGetHandler(ring *cache.HashRing, getCh chan GetRequest, localAddr, apiKey string, logger *util.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			duration := time.Since(start).Seconds()
			totalRequests := cache.CacheRequests.Value()
			if totalRequests > 0 {
				GetLatency.Set((GetLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests))
			}
		}()

		if r.URL.Query().Get("api_key") != apiKey {
			logger.Error("Invalid API key: %s", r.URL.Query().Get("api_key"))
			writeJSON(w, http.StatusUnauthorized, Response{Error: "Invalid API key"}, logger)
			return
		}

		keysParam := r.URL.Query().Get("keys")
		if keysParam == "" {
			writeJSON(w, http.StatusBadRequest, Response{Error: "Keys cannot be empty"}, logger)
			return
		}

		keys := strings.Split(keysParam, ",")
		node := ring.GetNode(keys[0])
		if node == localAddr || r.Header.Get("X-Forwarded") == "true" {
			results := make([]Response, len(keys))
			for i, key := range keys {
				resultCh := make(chan struct {
					Value  string
					Exists bool
				})
				getCh <- GetRequest{Key: key, Result: resultCh}
				result := <-resultCh
				if result.Exists {
					results[i] = Response{Status: "ok", Key: key, Value: result.Value}
				} else {
					results[i] = Response{Status: "not found", Key: key}
				}
			}
			logger.Info("Batch get request: keys=%v, target node=%s", keys, localAddr)
			writeJSON(w, http.StatusOK, results, logger)
			return
		}

		resp, err := forwardRequest(http.MethodGet, "https://"+node+"/batch_get?keys="+keysParam, apiKey, "", "", logger)
		if err != nil {
			logger.Error("Failed to forward batch get to %s: %v", node, err)
			writeJSON(w, http.StatusInternalServerError, Response{Error: "Failed to fetch from target node"}, logger)
			return
		}
		writeJSON(w, http.StatusOK, resp, logger)
	}
}

func makeBatchSetHandler(ring *cache.HashRing, localCache *cache.Cache, nodes []string, localAddr, apiKey string, logger *util.Logger, hb *cache.Heartbeat) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			duration := time.Since(start).Seconds()
			totalRequests := cache.CacheRequests.Value()
			if totalRequests > 0 {
				SetLatency.Set((SetLatency.Value()*float64(totalRequests-1) + duration) / float64(totalRequests))
			}
		}()

		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, Response{Error: "Method not allowed"}, logger)
			return
		}

		if r.URL.Query().Get("api_key") != apiKey {
			logger.Error("Invalid API key: %s", r.URL.Query().Get("api_key"))
			writeJSON(w, http.StatusUnauthorized, Response{Error: "Invalid API key"}, logger)
			return
		}

		var pairs []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&pairs); err != nil {
			writeJSON(w, http.StatusBadRequest, Response{Error: "Invalid request body"}, logger)
			return
		}
		if len(pairs) == 0 {
			writeJSON(w, http.StatusBadRequest, Response{Error: "Pairs cannot be empty"}, logger)
			return
		}

		nodePairs := make(map[string][]struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		})
		for _, pair := range pairs {
			node := ring.GetNode(pair.Key)
			logger.InfoNoLimit("Key %s assigned to node %s", pair.Key, node)
			nodePairs[node] = append(nodePairs[node], pair)
		}

		results := make([]Response, len(pairs))
		resultIdx := 0

		for node, group := range nodePairs {
			logger.InfoNoLimit("Processing batch for node %s: %v", node, group)
			if node == localAddr || r.Header.Get("X-Forwarded") == "true" {
				for _, pair := range group {
					if pair.Key == "" || pair.Value == "" {
						results[resultIdx] = Response{Error: "Key or value cannot be empty"}
					} else {
						localCache.Set(pair.Key, pair.Value)
						results[resultIdx] = Response{Status: "ok", Key: pair.Key}
					}
					resultIdx++
				}
				logger.InfoNoLimit("Batch set local: pairs=%v", group)
				// 改动：使用 broadcastBatch，只调用一次
				go broadcastBatch(nodes, localAddr, apiKey, group, logger)
			} else if hb.IsNodeHealthy(node) {
				jsonBody, err := json.Marshal(group)
				if err != nil {
					logger.Error("Failed to marshal group: %v", err)
					writeJSON(w, http.StatusInternalServerError, Response{Error: "Failed to serialize request"}, logger)
					return
				}
				logger.InfoNoLimit("Initiating forward to %s with body: %s", node, string(jsonBody))
				resp, err := forwardRequest(http.MethodPost, "https://"+node+"/batch_set", apiKey, "", string(jsonBody), logger)
				if err != nil {
					logger.Error("Failed to forward batch set to %s: %v", node, err)
					for range group {
						results[resultIdx] = Response{Error: "Failed to forward request to " + node}
						resultIdx++
					}
				} else {
					logger.InfoNoLimit("Forward to %s succeeded, response: %v", node, resp)
					for _, r := range resp {
						results[resultIdx] = r
						resultIdx++
					}
				}
			} else {
				logger.Error("Node %s is unhealthy, skipping", node)
				for range group {
					results[resultIdx] = Response{Error: "Target node " + node + " unhealthy"}
					resultIdx++
				}
			}
		}
		logger.InfoNoLimit("Batch set completed with results: %v", results)
		writeJSON(w, http.StatusOK, results, logger)
	}
}

func writeJSON(w http.ResponseWriter, status int, data interface{}, logger *util.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		logger.Error("Failed to write JSON response: %v", err)
	}
}

// forwardRequest 将请求转发到目标节点，带有重试机制和详细日志记录
func forwardRequest(method, url, apiKey, key, value string, logger *util.Logger) ([]Response, error) {
	maxRetries := 3                   // 定义最大重试次数
	client := util.GetHTTPClient()    // 获取全局 HTTP 客户端
	for i := 0; i < maxRetries; i++ { // 重试循环
		var req *http.Request // HTTP 请求对象
		var err error         // 错误变量

		// 规范化 URL：将 localhost 替换为 127.0.0.1，确保一致性
		url = strings.Replace(url, "localhost", "127.0.0.1", 1)
		// 确保 URL 以 https:// 开头
		if !strings.HasPrefix(url, "https://") {
			url = "https://" + url
		}
		// 改动：记录转发请求的详细信息，无限流
		logger.InfoNoLimit("Forwarding request to: %s (attempt %d/%d)", url, i+1, maxRetries)

		// 根据请求方法构造不同的 HTTP 请求
		switch method {
		case http.MethodGet:
			// GET 请求：将 key 和 api_key 作为查询参数
			req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
		case http.MethodPost:
			// POST 请求：根据 URL 判断是 batch_set 还是普通 set
			if strings.Contains(url, "/batch_set") {
				// batch_set 使用传入的 value 作为请求体
				req, err = http.NewRequest(method, url+"?api_key="+apiKey, bytes.NewBuffer([]byte(value)))
				req.Header.Set("Content-Type", "application/json")
			} else {
				// 普通 set 请求，构造键值对 JSON
				body := map[string]string{"key": key, "value": value}
				jsonBody, _ := json.Marshal(body)
				req, err = http.NewRequest(method, url+"?api_key="+apiKey, bytes.NewBuffer(jsonBody))
				req.Header.Set("Content-Type", "application/json")
			}
		case http.MethodDelete:
			// DELETE 请求：将 key 和 api_key 作为查询参数
			req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
		}
		if err != nil {
			// 创建请求失败，记录错误并返回
			logger.Error("Failed to create request for %s: %v", url, err)
			return nil, err
		}

		// 设置 X-Forwarded 头，防止循环转发
		req.Header.Set("X-Forwarded", "true")

		// 执行请求并计时
		start := time.Now()
		resp, err := client.Do(req)
		duration := time.Since(start)
		if err != nil {
			// 请求失败，记录错误并重试
			logger.Error("Forward request to %s failed after %v: %v", url, duration, err)
			if i < maxRetries-1 {
				// 指数退避：第1次 500ms，第2次 1s
				time.Sleep(time.Duration(1<<i) * 500 * time.Millisecond)
			}
			continue
		}
		defer resp.Body.Close() // 确保关闭响应体

		// 改动：记录转发成功的详细信息，无限流
		logger.InfoNoLimit("Forward request to %s succeeded in %v", url, duration)

		// 读取响应体
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			logger.Error("Failed to read response body from %s: %v", url, err)
			return nil, err
		}

		// 解析 JSON 响应
		var responses []Response
		err = json.Unmarshal(body, &responses)
		if err != nil {
			// 如果解析为数组失败，尝试解析为单个 Response
			var singleResp Response
			if jsonErr := json.Unmarshal(body, &singleResp); jsonErr == nil {
				responses = []Response{singleResp}
			} else {
				logger.Error("Failed to unmarshal response from %s: %v", url, err)
				return nil, err
			}
		}
		return responses, nil // 成功返回解析后的响应
	}
	// 重试次数耗尽，返回失败
	return nil, fmt.Errorf("failed to forward request to %s after %d retries", url, maxRetries)
}

func broadcast(nodes []string, localAddr, apiKey, key, value string, logger *util.Logger) {
	workerPool := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for _, n := range nodes {
		if n != localAddr {
			wg.Add(1)
			workerPool <- struct{}{}
			go func(node string) {
				defer wg.Done()
				defer func() { <-workerPool }()
				_, err := forwardRequest(http.MethodPost, "https://"+node+"/sync", apiKey, key, value, logger)
				if err != nil {
					logger.Error("Failed to sync to %s: %v", node, err)
				}
				time.Sleep(200 * time.Millisecond)
			}(n)
		}
	}
	wg.Wait()
}

func broadcastBatch(nodes []string, localAddr, apiKey string, pairs []struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}, logger *util.Logger) {
	// 将所有键值对序列化为 JSON
	jsonBody, err := json.Marshal(pairs)
	if err != nil {
		logger.Error("Failed to marshal pairs for batch broadcast: %v", err)
		return
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 2) // 并发限制为 2
	for _, node := range nodes {
		if node == localAddr {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(node string) {
			defer wg.Done()
			defer func() { <-sem }()
			logger.InfoNoLimit("Broadcasting batch sync to %s: %s", node, string(jsonBody))
			_, err := forwardRequest(http.MethodPost, "https://"+node+"/sync", apiKey, "", string(jsonBody), logger)
			if err != nil {
				logger.Error("Failed to batch sync to %s: %v", node, err)
			}
		}(node)
	}
	wg.Wait()
}
