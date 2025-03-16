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