// pkg/cache/heartbeat.go
package cache

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

// Heartbeat 结构体保存心跳检测的状态和配置
type Heartbeat struct {
    ring        *HashRing   // 一致性哈希环，用于获取节点列表
    localAddr   string      // 本地节点地址
    apiKey      string      // API 密钥，用于健康检查请求
    infoLogger  *log.Logger // 信息日志
    errorLogger *log.Logger // 错误日志
}

// NewHeartbeat 创建一个心跳检测实例
func NewHeartbeat(ring *HashRing, localAddr, apiKey string, infoLogger, errorLogger *log.Logger) *Heartbeat {
    return &Heartbeat{
        ring:        ring,
        localAddr:   localAddr,
        apiKey:      apiKey,
        infoLogger:  infoLogger,
        errorLogger: errorLogger,
    }
}

// Start 开始心跳检测
func (h *Heartbeat) Start() {
    go func() { // 在后台运行
        for {
            nodes := h.ring.GetNodes() // 获取所有节点
            for _, node := range nodes {
                if node != h.localAddr { // 不检查自己
                    // 发送健康检查请求
                    resp, err := h.checkHealth(node)
                    if err != nil {
                        h.errorLogger.Printf("Node %s is unhealthy: %v", node, err)
                    } else if resp.Status != "ok" {
                        h.errorLogger.Printf("Node %s returned unexpected status: %s", node, resp.Status)
                    } else {
                        h.infoLogger.Printf("Node %s is healthy", node)
                    }
                }
            }
            time.Sleep(5 * time.Second) // 每 5 秒检查一次
        }
    }()
}

// checkHealth 发送健康检查请求到指定节点
func (h *Heartbeat) checkHealth(node string) (*Response, error) {
    url := "https://" + node + "/health"
    if !strings.HasPrefix(node, "http://") && !strings.HasPrefix(node, "https://") {
        url = "https://" + node + "/health"
    }

    req, err := http.NewRequest(http.MethodGet, url+"?api_key="+h.apiKey, nil)
    if err != nil {
        return nil, err
    }

    client := &http.Client{
        Timeout: 2 * time.Second, // 设置 2 秒超时
        Transport: &http.Transport{
            TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // 跳过证书验证
        },
    }
    resp, err := client.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var response Response
    err = json.NewDecoder(resp.Body).Decode(&response)
    return &response, err
}

// Response 结构体，用于解析健康检查响应
type Response struct {
    Status string `json:"status"`
}