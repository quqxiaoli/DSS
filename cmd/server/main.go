package main

import (
	"context"
	"os/signal"
	"syscall"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/quqxiaoli/distributed-cache/config"
	"github.com/quqxiaoli/distributed-cache/internal/api"
	"github.com/quqxiaoli/distributed-cache/internal/util"
	"github.com/quqxiaoli/distributed-cache/pkg/cache"
)

func main() {
	rand.Seed(time.Now().UnixNano()) // 添加随机种子
	// 定义一个命令行参数 "port"，默认值为 "8080"，用于指定服务器端口。
	port := flag.String("port", "8080", "server port")
	// 解析命令行参数。
	flag.Parse()

	// 从 "config/config.yaml" 文件加载配置，并传入当前端口号。
	cfg, err := config.LoadConfig("config/config.yaml", *port)
	// 检查配置加载是否失败，如果失败则记录错误并终止程序。
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 根据配置信息初始化日志记录器。
	logger, err := util.NewLogger(cfg.Logging)
	// 检查日志记录器初始化是否失败，如果失败则记录错误并终止程序。
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	// 确保在函数结束时关闭日志记录器。
	defer logger.Close()

	// 记录服务器启动信息，包含端口号。
	logger.Info("Starting distributed cache server on port %s", *port)

	// 创建一个容量为 100 的本地缓存实例。
	localCache := cache.NewCache(100, logger)
	// 创建一个缓冲通道，用于接收 GetRequest 请求。
	getCh := make(chan api.GetRequest, 10)
	// 启动一个 goroutine 来处理 getCh 通道中的请求。
	go func() {
		// 从通道中接收请求，直到通道关闭。
		for req := range getCh {
			// 从本地缓存中获取指定键的值。
			value, exists := localCache.Get(req.Key)
			// 将获取的结果发送到请求的结果通道中。
			req.Result <- struct {
				Value  string
				Exists bool
			}{value, exists}
		}
	}()

	// 创建一个虚拟节点数量为 100 的哈希环。
	ring := cache.NewHashRing(100)
	// 遍历配置文件中的节点列表，将每个节点添加到哈希环中。
	for _, node := range cfg.Nodes {
		ring.AddNode(node)
	}

	// 构造本地服务器的地址。
	localAddr := fmt.Sprintf("127.0.0.1:%s", *port)
	// 创建一个心跳机制实例，用于监控节点的健康状态。
	hb := cache.NewHeartbeat(ring, localAddr, cfg.Server.APIKey, logger.InfoLogger(), logger.ErrorLogger(), localCache)
	// 启动一个 goroutine 来启动心跳机制。
	go hb.Start()

	// // 启用混沌测试
     if cfg.Chaos.Enabled {
         hb.EnableChaos(cfg.Chaos.FailureRate)
     }

	// 设置 HTTP 路由，处理各种请求。
	api.SetupRoutes(ring, localCache, getCh, cfg.Nodes, localAddr, cfg.Server.APIKey, logger, hb)

	// 创建一个 HTTP 服务器实例。
	server := &http.Server{
		// 指定服务器监听的地址。
		Addr: ":" + *port,
		// 使用 Gzip 处理程序包装默认的 HTTP 多路复用器。
		Handler: api.GzipHandler(http.DefaultServeMux),
		// 配置 TLS 连接。
		TLSConfig: &tls.Config{
			// 跳过客户端证书验证，此设置在生产环境中不安全。
			InsecureSkipVerify: true,
			// 使用 LRU 缓存来存储客户端会话。
			ClientSessionCache: tls.NewLRUClientSessionCache(5000),
			// 启用会话票据。
			SessionTicketsDisabled: false,
			// 最低支持的 TLS 版本为 TLS 1.2。
			MinVersion: tls.VersionTLS12,
			// 优先使用服务器指定的加密套件。
			PreferServerCipherSuites: true,
			// 优先使用的椭圆曲线。
			CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256},
		},
		// 设置读取超时时间。
		ReadTimeout: 10 * time.Second,
		// 设置写入超时时间。
		WriteTimeout: 10 * time.Second,
		// 设置空闲超时时间。
		IdleTimeout: 20 * time.Second,
	}

	// 优雅关闭
    stop := make(chan os.Signal, 1)
    signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        logger.Info("Server running on https://localhost:%s", *port) // 改为 logger 输出
        if err := server.ListenAndServeTLS("config/cert.pem", "config/key.pem"); err != nil && err != http.ErrServerClosed {
            logger.Error("Failed to start TLS server: %v", err)
            os.Exit(1)
        }
    }()

    <-stop
    logger.Info("Shutting down server...")

    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()
    if err := server.Shutdown(ctx); err != nil {
        logger.Error("Server shutdown failed: %v", err)
    }
    logger.Info("Server gracefully stopped")
}