package main

import (
    "crypto/tls"
    "flag"
    "fmt"
    "log"
    "net/http"
    "os"
    "time"

    "github.com/quqxiaoli/distributed-cache/config"
    "github.com/quqxiaoli/distributed-cache/internal/api"
    "github.com/quqxiaoli/distributed-cache/internal/util"
    "github.com/quqxiaoli/distributed-cache/pkg/cache"
)

func main() {
    port := flag.String("port", "8080", "server port")
    flag.Parse()

    cfg, err := config.LoadConfig("config/config.yaml", *port)
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    logger, err := util.NewLogger(cfg.Logging)
    if err != nil {
        log.Fatalf("Failed to initialize logger: %v", err)
    }
    defer logger.Close()

    logger.Info("Starting distributed cache server on port %s", *port)

    localCache := cache.NewCache(100)
    getCh := make(chan struct {
        key    string
        result chan struct {
            value  string
            exists bool
        }
    }, 10)
    go func() {
        for req := range getCh {
            value, exists := localCache.Get(req.key)
            req.result <- struct {
                value  string
                exists bool
            }{value, exists}
        }
    }()

    ring := cache.NewHashRing(100)
    for _, node := range cfg.Nodes {
        ring.AddNode(node)
    }

    localAddr := fmt.Sprintf("127.0.0.1:%s", *port)
    hb := cache.NewHeartbeat(ring, localAddr, cfg.Server.APIKey, logger.InfoLogger(), logger.ErrorLogger(), localCache)
    go hb.Start()

    api.SetupRoutes(ring, localCache, getCh, cfg.Nodes, localAddr, cfg.Server.APIKey, logger)

    server := &http.Server{
        Addr:    ":" + *port,
        Handler: api.GzipHandler(http.DefaultServeMux),
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
    err = server.ListenAndServeTLS("config/cert.pem", "config/key.pem")
    if err != nil {
        logger.Error("Failed to start TLS server: %v", err)
        os.Exit(1)
    }
}