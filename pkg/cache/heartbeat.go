package cache

import (
    "crypto/tls"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "strings"
    "sync"
    "time"
)

// Heartbeat 结构体，用于管理节点健康检查和数据迁移
type Heartbeat struct {
    ring        *HashRing   // 一致性哈希环
    localAddr   string      // 本地节点地址，格式为"127.0.0.1:port"
    apiKey      string      // API密钥
    infoLogger  *log.Logger // 信息日志
    errorLogger *log.Logger // 错误日志
    failures    map[string]int // 节点失败计数
    client      *http.Client   // HTTP客户端，与main.go同步
    cache       *Cache         // 本地缓存实例
}

// IsNodeHealthy 公开方法，检查节点是否健康
func (h *Heartbeat) IsNodeHealthy(node string) bool {
    // 如果节点不在failures中或失败次数<3，则认为健康
    _, exists := h.failures[node]
    return !exists || h.failures[node] < 3
}

// NewHeartbeat 初始化Heartbeat实例，与main.go的globalClient保持一致
func NewHeartbeat(ring *HashRing, localAddr, apiKey string, infoLogger, errorLogger *log.Logger, cache *Cache) *Heartbeat {
    return &Heartbeat{
        ring:        ring,
        localAddr:   localAddr,
        apiKey:      apiKey,
        infoLogger:  infoLogger,
        errorLogger: errorLogger,
        failures:    make(map[string]int),
        client: &http.Client{
            Timeout: 8 * time.Second, // 与main.go同步，8秒超时
            Transport: &http.Transport{
                TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, // 测试用，生产移除
                MaxIdleConns:      50,          // 减少连接数，与main.go一致
                MaxIdleConnsPerHost: 20,        // 每个主机20个空闲连接
                IdleConnTimeout:   15 * time.Second, // 15秒超时，防止堆积
                DisableKeepAlives: false,       // 保持长连接
                TLSHandshakeTimeout: 2 * time.Second, // TLS握手超时
            },
        },
        cache: cache,
    }
}

// Start 启动心跳检测，优化检查频率和并发
func (h *Heartbeat) Start() {
    h.infoLogger.Printf("Waiting for nodes to initialize...")
    time.Sleep(15 * time.Second) // 初始延迟15秒，给节点启动时间

    go func() {
        for {
            nodes := h.ring.GetNodes()
            sem := make(chan struct{}, 2) // 限制并发检查为2，避免请求洪水
            var wg sync.WaitGroup

            for _, node := range nodes {
                if node != h.localAddr { // 跳过本地节点
                    wg.Add(1)
                    sem <- struct{}{}
                    go func(n string) {
                        defer wg.Done()
                        defer func() { <-sem }()

                        maxRetries := 3 // 重试3次，增加健壮性
                        for i := 0; i < maxRetries; i++ {
                            resp, err := h.CheckHealth(n)
                            if err != nil || resp.Status != "ok" {
                                h.failures[n]++
                                if err != nil {
                                    h.errorLogger.Printf("Node %s check failed: %v, attempt %d/%d, failures: %d", n, err, i+1, maxRetries, h.failures[n])
                                } else {
                                    h.errorLogger.Printf("Node %s status %s, attempt %d/%d, failures: %d", n, resp.Status, i+1, maxRetries, h.failures[n])
                                }
                                if h.failures[n] >= 3 { // 连续3次失败，移除节点
                                    h.errorLogger.Printf("Node %s confirmed unhealthy, removing from ring", n)
                                    h.ring.RemoveNode(n)
                                    h.migrateData(n) // 迁移数据
                                    delete(h.failures, n)
                                    break
                                }
                                // 指数退避：0.5s, 1s, 2s
                                time.Sleep(time.Duration(1<<i) * 500 * time.Millisecond)
                                continue
                            }
                            h.infoLogger.Printf("Node %s is healthy", n)
                            h.failures[n] = 0 // 重置失败计数
                            break
                        }
                    }(node)
                }
            }
            wg.Wait()
            time.Sleep(30 * time.Second) // 每30秒检查一次，降低频率
        }
    }()
}

// migrateData 从失败节点迁移数据，优化为分批处理
func (h *Heartbeat) migrateData(failedNode string) {
    h.infoLogger.Printf("Starting data migration from failed node %s", failedNode)

    // 获取本地缓存所有数据
    allData := h.cache.GetAll()
    var batch []struct {
        Key   string `json:"key"`
        Value string `json:"value"`
    }

    // 筛选需要迁移的键值对
    for key, value := range allData {
        oldNode := h.ring.GetNodeForKeyBeforeRemoval(key, failedNode)
        if oldNode == failedNode {
            newNode := h.ring.GetNode(key)
            if newNode != h.localAddr { // 只迁移到非本地节点
                batch = append(batch, struct {
                    Key   string `json:"key"`
                    Value string `json:"value"`
                }{key, value})
            }
        }
    }

    // 分批迁移，减轻请求压力
    if len(batch) > 0 {
        batchSize := 50 // 每批50个键值对
        for i := 0; i < len(batch); i += batchSize {
            end := i + batchSize
            if end > len(batch) {
                end = len(batch)
            }
            currentBatch := batch[i:end]

            jsonBody, err := json.Marshal(currentBatch)
            if err != nil {
                h.errorLogger.Printf("Failed to marshal batch for migration: %v", err)
                continue
            }

            // 确定目标节点（基于第一个键）
            targetNode := h.ring.GetNode(currentBatch[0].Key)
            url := fmt.Sprintf("https://%s/batch_set?api_key=%s", targetNode, h.apiKey)
            req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(jsonBody)))
            if err != nil {
                h.errorLogger.Printf("Failed to create migration request to %s: %v", targetNode, err)
                continue
            }
            req.Header.Set("Content-Type", "application/json")
            req.Header.Set("X-Forwarded", "true") // 防止循环转发

            resp, err := h.client.Do(req)
            if err != nil {
                h.errorLogger.Printf("Batch migration to %s failed: %v", targetNode, err)
            } else {
                defer resp.Body.Close() // 确保关闭连接
                if resp.StatusCode == http.StatusOK {
                    h.infoLogger.Printf("Migrated %d keys to %s from %s", len(currentBatch), targetNode, failedNode)
                } else {
                    h.errorLogger.Printf("Migration to %s returned status %d", targetNode, resp.StatusCode)
                }
            }
            time.Sleep(200 * time.Millisecond) // 批次间延迟，减轻压力
        }
    }

    h.infoLogger.Printf("Data migration from %s completed", failedNode)
}

// CheckHealth 检查节点健康状态，添加状态码处理
func (h *Heartbeat) CheckHealth(node string) (*Response, error) {
    url := fmt.Sprintf("https://%s/health?api_key=%s", node, h.apiKey)
    req, err := http.NewRequest(http.MethodGet, url, nil)
    if err != nil {
        return nil, fmt.Errorf("failed to create health check request: %v", err)
    }

    resp, err := h.client.Do(req)
    if err != nil {
        h.errorLogger.Printf("Health check to %s failed: %v", node, err)
        return nil, err
    }
    defer resp.Body.Close() // 确保释放连接
    h.infoLogger.Printf("Health check to %s returned status code: %d", node, resp.StatusCode)

    // 处理特定状态码
    switch resp.StatusCode {
    case http.StatusOK:
        // 正常，继续解析响应
    case 429, 503: // 请求过多或服务不可用，临时错误
        return nil, fmt.Errorf("temporary error, status: %d", resp.StatusCode)
    case 500, 502, 504: // 服务器错误
        return nil, fmt.Errorf("server error, status: %d", resp.StatusCode)
    default:
        return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
    }

    var response Response
    err = json.NewDecoder(resp.Body).Decode(&response)
    if err != nil {
        h.errorLogger.Printf("Failed to decode response from %s: %v", node, err)
        return nil, err
    }
    h.infoLogger.Printf("Health check to %s returned status: %s", node, response.Status)
    return &response, nil
}

// Response 定义健康检查响应结构
type Response struct {
    Status string `json:"status"`
}