package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/quqxiaoli/distributed-cache/internal/util"
	"github.com/quqxiaoli/distributed-cache/pkg/cache"
)

func TestSetHandler(t *testing.T) {
    // 初始化依赖
    logger, _ := util.NewLogger("test.log")
    defer logger.Close()
    ring := cache.NewHashRing(10)
    ring.AddNode("127.0.0.1:8080") // 添加本地节点，确保哈希命中
    localCache := cache.NewCache(10)
    nodes := []string{"127.0.0.1:8080"}
    localAddr := "127.0.0.1:8080"
    apiKey := "my-secret-key"

    // 创建 handler
    handler := makeSetHandler(ring, localCache, nodes, localAddr, apiKey, logger)

    // 构造请求
    body := map[string]string{"key": "test", "value": "123"}
    jsonBody, _ := json.Marshal(body)
    req, _ := http.NewRequest("POST", "/set?api_key=my-secret-key", bytes.NewBuffer(jsonBody))
    rr := httptest.NewRecorder()

    // 执行请求
    handler.ServeHTTP(rr, req)

    // 检查响应
    if rr.Code != http.StatusOK {
        t.Errorf("期望状态码 200，得到 %d", rr.Code)
    }
    var resp Response
    json.Unmarshal(rr.Body.Bytes(), &resp)
    if resp.Status != "ok" || resp.Key != "test" {
        t.Errorf("期望 status 'ok' 和 key 'test'，得到 %v", resp)
    }
}

func TestGetHandler(t *testing.T) {
    // 初始化依赖
    logger, _ := util.NewLogger("test.log")
    defer logger.Close()
    ring := cache.NewHashRing(10)
    ring.AddNode("127.0.0.1:8080") // 添加本地节点，确保哈希命中
    localCache := cache.NewCache(10)
    getCh := make(chan GetRequest, 10)
    localAddr := "127.0.0.1:8080"
    apiKey := "my-secret-key"

    // 模拟缓存数据
    localCache.Set("test", "123")
    go func() {
        for req := range getCh {
            value, exists := localCache.Get(req.Key)
            req.Result <- struct {
                Value  string
                Exists bool
            }{value, exists}
        }
    }()

    // 创建 handler
    handler := makeGetHandler(ring, getCh, localAddr, apiKey, logger)

    // 构造请求
    req, _ := http.NewRequest("GET", "/get?key=test&api_key=my-secret-key", nil)
    rr := httptest.NewRecorder()

    // 执行请求
    handler.ServeHTTP(rr, req)

    // 检查响应
    if rr.Code != http.StatusOK {
        t.Errorf("期望状态码 200，得到 %d", rr.Code)
    }
    var resp Response
    json.Unmarshal(rr.Body.Bytes(), &resp)
    if resp.Status != "ok" || resp.Value != "123" {
        t.Errorf("期望 status 'ok' 和 value '123'，得到 %v", resp)
    }
}

func TestHealthHandler(t *testing.T) {
    // 初始化 logger
    logger, _ := util.NewLogger("test.log")
    defer logger.Close()
    apiKey := "my-secret-key"

    // 初始化 Heartbeat 和 localAddr
    ring := cache.NewHashRing(100)
    localAddr := "127.0.0.1:8080"
    hb := cache.NewHeartbeat(ring, localAddr, apiKey, logger.InfoLogger(), logger.ErrorLogger(), cache.NewCache(100))

    // 创建 handler，添加 hb 和 localAddr 参数
    handler := makeHealthHandler(apiKey, logger, hb, localAddr)

    // 测试正确 API key
    t.Run("Valid API Key", func(t *testing.T) {
        req, _ := http.NewRequest("GET", "/health?api_key=my-secret-key", nil)
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusOK {
            t.Errorf("期望状态码 200，得到 %d", rr.Code)
        }
        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("解析响应失败: %v", err)
        }
        if resp.Status != "ok" {
            t.Errorf("期望 status 'ok'，得到 %v", resp)
        }
    })

    // 测试错误 API key
    t.Run("Invalid API Key", func(t *testing.T) {
        req, _ := http.NewRequest("GET", "/health?api_key=wrong-key", nil)
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusUnauthorized {
            t.Errorf("期望状态码 401，得到 %d", rr.Code)
        }
        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("解析响应失败: %v", err)
        }
        if resp.Error != "Invalid API key" {
            t.Errorf("期望 error 'Invalid API key'，得到 %v", resp)
        }
    })

    // 测试不健康状态（可选，验证混沌测试效果）
    t.Run("Unhealthy Node", func(t *testing.T) {
        hb.EnableChaos(1.0) // 强制触发故障
		hb.SetFailureCount(localAddr, 3)
        req, _ := http.NewRequest("GET", "/health?api_key=my-secret-key", nil)
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusServiceUnavailable {
            t.Errorf("期望状态码 503，得到 %d", rr.Code)
        }
        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("解析响应失败: %v", err)
        }
        if resp.Status != "unhealthy" {
            t.Errorf("期望 status 'unhealthy'，得到 %v", resp)
        }
    })
}

func TestDeleteHandler(t *testing.T) {
    // 初始化依赖
    logger, _ := util.NewLogger("test.log")
    defer logger.Close()
    ring := cache.NewHashRing(10)
    ring.AddNode("127.0.0.1:8080") // 确保本地处理
    localCache := cache.NewCache(10)
    localAddr := "127.0.0.1:8080"
    apiKey := "my-secret-key"

    // 创建 handler
    handler := makeDeleteHandler(ring, localCache, localAddr, apiKey, logger)

    // 测试成功删除
    t.Run("Delete Existing Key", func(t *testing.T) {
        // 先存入数据
        localCache.Set("test", "value")

        req, _ := http.NewRequest("GET", "/delete?key=test&api_key=my-secret-key", nil)
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusOK {
            t.Errorf("期望状态码 200，得到 %d", rr.Code)
        }
        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("解析响应失败: %v", err)
        }
        if resp.Status != "ok" || resp.Key != "test" {
            t.Errorf("期望 status 'ok' 和 key 'test'，得到 %v", resp)
        }

        // 验证缓存中已删除
        _, exists := localCache.Get("test")
        if exists {
            t.Errorf("期望 'test' 被删除，但仍存在")
        }
    })

    // 测试空 key
    t.Run("Empty Key", func(t *testing.T) {
        req, _ := http.NewRequest("GET", "/delete?key=&api_key=my-secret-key", nil)
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusBadRequest {
            t.Errorf("期望状态码 400，得到 %d", rr.Code)
        }
        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("解析响应失败: %v", err)
        }
        if resp.Error != "Key cannot be empty" {
            t.Errorf("期望 error 'Key cannot be empty'，得到 %v", resp)
        }
    })
}

func TestBatchGetHandler(t *testing.T) {
    // 初始化依赖
    logger, _ := util.NewLogger("test.log")
    defer logger.Close()
    ring := cache.NewHashRing(10)
    ring.AddNode("127.0.0.1:8080") // 确保本地处理
    localCache := cache.NewCache(10)
    localAddr := "127.0.0.1:8080"
    apiKey := "my-secret-key"
    getCh := make(chan GetRequest, 10) // 与 main.go 一致

    // 启动 getCh 处理协程
    go func() {
        for req := range getCh {
            value, exists := localCache.Get(req.Key)
            req.Result <- struct {
                Value  string
                Exists bool
            }{value, exists}
        }
    }()

    // 创建 handler
    handler := makeBatchGetHandler(ring, getCh, localAddr, apiKey, logger)

    // 测试批量获取
    t.Run("Batch Get Existing Keys", func(t *testing.T) {
        // 先存入数据
        localCache.Set("key1", "value1")
        localCache.Set("key2", "value2")

        req, _ := http.NewRequest("GET", "/batch_get?keys=key1,key2&api_key=my-secret-key", nil)
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusOK {
            t.Errorf("期望状态码 200，得到 %d", rr.Code)
        }
        var resp []Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("解析响应失败: %v", err)
        }
        if len(resp) != 2 {
            t.Errorf("期望返回 2 个结果，得到 %d 个", len(resp))
        }
        expected := map[string]string{"key1": "value1", "key2": "value2"}
        for _, r := range resp {
            if r.Status != "ok" || r.Value != expected[r.Key] {
                t.Errorf("期望 key=%s, status='ok', value=%s，得到 %v", r.Key, expected[r.Key], r)
            }
        }
    })

    // 测试空 keys
    t.Run("Empty Keys", func(t *testing.T) {
        req, _ := http.NewRequest("GET", "/batch_get?keys=&api_key=my-secret-key", nil)
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusBadRequest {
            t.Errorf("期望状态码 400，得到 %d", rr.Code)
        }
        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("解析响应失败: %v", err)
        }
        if resp.Error != "Keys cannot be empty" {
            t.Errorf("期望 error 'Keys cannot be empty'，得到 %v", resp)
        }
    })
}

func TestBatchSetHandler(t *testing.T) {
    logger, _ := util.NewLogger("test.log")
    defer logger.Close()
    ring := cache.NewHashRing(10)
    ring.AddNode("127.0.0.1:8080")
    localCache := cache.NewCache(10)
    localAddr := "127.0.0.1:8080"
    apiKey := "my-secret-key"
    nodes := []string{"127.0.0.1:8080"}
    hb := cache.NewHeartbeat(ring, localAddr, apiKey, logger.InfoLogger(), logger.ErrorLogger(), localCache)

    handler := makeBatchSetHandler(ring, localCache, nodes, localAddr, apiKey, logger, hb)

    t.Run("Batch Set Valid Pairs", func(t *testing.T) {
        pairs := []struct {
            Key   string `json:"key"`
            Value string `json:"value"`
        }{
            {"key1", "value1"},
            {"key2", "value2"},
        }
        body, _ := json.Marshal(pairs)
        req, _ := http.NewRequest("POST", "/batch_set?api_key=my-secret-key", bytes.NewBuffer(body))
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusOK {
            t.Errorf("期望状态码 200，得到 %d", rr.Code)
        }
        var resp []Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("解析响应失败: %v", err)
        }
        if len(resp) != 2 {
            t.Errorf("期望返回 2 个结果，得到 %d 个", len(resp))
        }
        for _, r := range resp {
            if r.Status != "ok" {
                t.Errorf("期望 status 'ok'，得到 %v", r)
            }
        }
        if v, ok := localCache.Get("key1"); !ok || v != "value1" {
            t.Errorf("期望 key1=value1，得到 %v, %v", v, ok)
        }
        if v, ok := localCache.Get("key2"); !ok || v != "value2" {
            t.Errorf("期望 key2=value2，得到 %v, %v", v, ok)
        }
    })

    t.Run("Empty Pairs", func(t *testing.T) {
        body := []byte(`[]`)
        req, _ := http.NewRequest("POST", "/batch_set?api_key=my-secret-key", bytes.NewBuffer(body))
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusBadRequest {
            t.Errorf("期望状态码 400，得到 %d", rr.Code)
        }
        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("解析响应失败: %v", err)
        }
        if resp.Error != "Pairs cannot be empty" {
            t.Errorf("期望 error 'Pairs cannot be empty'，得到 %v", resp)
        }
    })
}

func TestSyncHandler(t *testing.T) {
    logger, err := util.NewLogger("test.log")
    if err != nil {
        t.Fatalf("Failed to create logger: %v", err)
    }
    defer logger.Close()
    localCache := cache.NewCache(10)
    apiKey := "my-secret-key"
    handler := makeSyncHandler(apiKey, localCache, logger)

    t.Run("Sync_Valid_Data", func(t *testing.T) {
        body := map[string]string{"key": "sync-test", "value": "sync-value"}
        bodyBytes, _ := json.Marshal(body)
        req, _ := http.NewRequest("POST", "/sync?api_key="+apiKey, bytes.NewBuffer(bodyBytes))
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusOK {
            t.Errorf("Expected status code 200, got %d", rr.Code)
        }
        t.Logf("Actual response: %s", rr.Body.String())

        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("Failed to unmarshal response: %v", err)
        }
        expected := Response{Status: "ok", Key: "sync-test", Value: "sync-value"}
        if resp != expected {
            t.Errorf("Expected response %+v, got %+v", expected, resp)
        }
        if v, ok := localCache.Get("sync-test"); !ok || v != "sync-value" {
            t.Errorf("Expected cache to have sync-test=sync-value, got %v, %v", v, ok)
        }
    })

    t.Run("Sync_Batch_Data", func(t *testing.T) {
        body := []map[string]string{
            {"key": "batch-key1", "value": "batch-value1"},
            {"key": "batch-key2", "value": "batch-value2"},
        }
        bodyBytes, _ := json.Marshal(body)
        req, _ := http.NewRequest("POST", "/sync?api_key="+apiKey, bytes.NewBuffer(bodyBytes))
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusOK {
            t.Errorf("Expected status code 200, got %d", rr.Code)
        }
        t.Logf("Actual response: %s", rr.Body.String())

        var resp []Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("Failed to unmarshal response: %v", err)
        }
        expected := []Response{
            {Status: "ok", Key: "batch-key1", Value: "batch-value1"},
            {Status: "ok", Key: "batch-key2", Value: "batch-value2"},
        }
        if len(resp) != len(expected) || resp[0] != expected[0] || resp[1] != expected[1] {
            t.Errorf("Expected response %+v, got %+v", expected, resp)
        }
        if v, ok := localCache.Get("batch-key1"); !ok || v != "batch-value1" {
            t.Errorf("Expected cache to have batch-key1=batch-value1, got %v, %v", v, ok)
        }
        if v, ok := localCache.Get("batch-key2"); !ok || v != "batch-value2" {
            t.Errorf("Expected cache to have batch-key2=batch-value2, got %v, %v", v, ok)
        }
    })

    t.Run("Invalid_API_Key", func(t *testing.T) {
        body := map[string]string{"key": "sync-test", "value": "sync-value"}
        bodyBytes, _ := json.Marshal(body)
        req, _ := http.NewRequest("POST", "/sync?api_key=wrong-key", bytes.NewBuffer(bodyBytes))
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusUnauthorized {
            t.Errorf("Expected status code 401, got %d", rr.Code)
        }
        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("Failed to unmarshal response: %v", err)
        }
        if resp.Error != "Invalid API key" {
            t.Errorf("Expected error 'Invalid API key', got %v", resp.Error)
        }
    })

    t.Run("Invalid_Body", func(t *testing.T) {
        req, _ := http.NewRequest("POST", "/sync?api_key="+apiKey, bytes.NewBuffer([]byte("invalid json")))
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusBadRequest {
            t.Errorf("Expected status code 400, got %d", rr.Code)
        }
        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("Failed to unmarshal response: %v", err)
        }
        if resp.Error != "Invalid request body" {
            t.Errorf("Expected error 'Invalid request body', got %v", resp.Error)
        }
    })

    t.Run("Empty_Key_Or_Value", func(t *testing.T) {
        body := map[string]string{"key": "", "value": "sync-value"}
        bodyBytes, _ := json.Marshal(body)
        req, _ := http.NewRequest("POST", "/sync?api_key="+apiKey, bytes.NewBuffer(bodyBytes))
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusBadRequest {
            t.Errorf("Expected status code 400, got %d", rr.Code)
        }
        t.Logf("Actual response: %s", rr.Body.String())
        var resp Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("Failed to unmarshal response: %v", err)
        }
        if resp.Error != "Key cannot be empty or invalid" {
            t.Errorf("Expected error 'Key cannot be empty or invalid', got %v", resp.Error)
        }
    })

    t.Run("Batch_Empty_Value", func(t *testing.T) {
        body := []map[string]string{
            {"key": "batch-key1", "value": "batch-value1"},
            {"key": "batch-key2", "value": ""},
        }
        bodyBytes, _ := json.Marshal(body)
        req, _ := http.NewRequest("POST", "/sync?api_key="+apiKey, bytes.NewBuffer(bodyBytes))
        rr := httptest.NewRecorder()
        handler.ServeHTTP(rr, req)

        if rr.Code != http.StatusOK {
            t.Errorf("Expected status code 200, got %d", rr.Code)
        }
        t.Logf("Actual response: %s", rr.Body.String())
        var resp []Response
        if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
            t.Errorf("Failed to unmarshal response: %v", err)
        }
        expected := []Response{
            {Status: "ok", Key: "batch-key1", Value: "batch-value1"},
            {Error: "Value cannot be empty or invalid"},
        }
        if len(resp) != len(expected) || resp[0] != expected[0] || resp[1] != expected[1] {
            t.Errorf("Expected response %+v, got %+v", expected, resp)
        }
        if v, ok := localCache.Get("batch-key1"); !ok || v != "batch-value1" {
            t.Errorf("Expected cache to have batch-key1=batch-value1, got %v, %v", v, ok)
        }
    })
}