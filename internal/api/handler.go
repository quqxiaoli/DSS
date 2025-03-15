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

func SetupRoutes(ring *cache.HashRing, localCache *cache.Cache, getCh chan struct {
    key    string
    result chan struct {
        value  string
        exists bool
    }
}, nodes []string, localAddr, apiKey string, logger *util.Logger) {
    http.HandleFunc("/health", makeHealthHandler(apiKey, logger))
    http.HandleFunc("/sync", makeSyncHandler(apiKey, localCache, logger))
    http.HandleFunc("/set", makeSetHandler(ring, localCache, nodes, localAddr, apiKey, logger))
    http.HandleFunc("/get", makeGetHandler(ring, getCh, localAddr, apiKey, logger))
    http.HandleFunc("/delete", makeDeleteHandler(ring, localCache, localAddr, apiKey, logger))
    http.HandleFunc("/batch_get", makeBatchGetHandler(ring, getCh, localAddr, apiKey, logger))
    http.HandleFunc("/batch_set", makeBatchSetHandler(ring, localCache, nodes, localAddr, apiKey, logger))
}

func makeHealthHandler(apiKey string, logger *util.Logger) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        if r.URL.Query().Get("api_key") != apiKey {
            logger.Error("Invalid API key for health check: %s", r.URL.Query().Get("api_key"))
            writeJSON(w, http.StatusUnauthorized, Response{Error: "Invalid API key"}, logger)
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
        localCache.Set(key, value)
        logger.Info("Sync received: key=%s, value=%s", key, value)
        writeJSON(w, http.StatusOK, Response{Status: "ok", Key: key, Value: value}, logger)
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

func makeGetHandler(ring *cache.HashRing, getCh chan struct {
    key    string
    result chan struct {
        value  string
        exists bool
    }
}, localAddr, apiKey string, logger *util.Logger) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        defer func() {
            duration := time.Since(start).Seconds()
            totalRequests := cache.CacheRequests.Value()
            if totalRequests > 0 {
                GetLatency.Set((GetLatency.Value()*float64(totalRequests-1) + duration) / float64(brasil))
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
                value  string
                exists bool
            })
            getCh <- struct {
                key    string
                result chan struct {
                    value  string
                    exists bool
                }
            }{key, resultCh}
            result := <-resultCh
            if result.exists {
                writeJSON(w, http.StatusOK, Response{Status: "ok", Key: key, Value: result.value}, logger)
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

func makeBatchGetHandler(ring *cache.HashRing, getCh chan struct {
    key    string
    result chan struct {
        value  string
        exists bool
    }
}, localAddr, apiKey string, logger *util.Logger) http.HandlerFunc {
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
                    value  string
                    exists bool
                })
                getCh <- struct {
                    key    string
                    result chan struct {
                        value  string
                        exists bool
                    }
                }{key, resultCh}
                result := <-resultCh
                if result.exists {
                    results[i] = Response{Status: "ok", Key: key, Value: result.value}
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

func makeBatchSetHandler(ring *cache.HashRing, localCache *cache.Cache, nodes []string, localAddr, apiKey string, logger *util.Logger) http.HandlerFunc {
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
            logger.Info("Batch set local: pairs=%v", pairs)
            go broadcast(nodes, localAddr, apiKey, pairs[0].Key, pairs[0].Value, logger)
            writeJSON(w, http.StatusOK, results, logger)
            return
        }

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
                logger.Error("Failed to marshal batch: %v", err)
                writeJSON(w, http.StatusInternalServerError, Response{Error: "Failed to serialize request"}, logger)
                return
            }
            resp, err := forwardRequest(http.MethodPost, "https://"+node+"/batch_set", apiKey, "", string(jsonBody), logger)
            if err != nil {
                logger.Error("Failed to forward batch set to %s: %v", node, err)
                writeJSON(w, http.StatusInternalServerError, Response{Error: "Failed to forward request"}, logger)
                return
            }
            for _, r := range resp {
                results[resultIdx] = r
                resultIdx++
            }
            time.Sleep(200 * time.Millisecond)
        }
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

func forwardRequest(method, url, apiKey, key, value string, logger *util.Logger) ([]Response, error) {
    maxRetries := 3
    client := util.GetHTTPClient()
    for i := 0; i < maxRetries; i++ {
        var req *http.Request
        var err error

        url = strings.Replace(url, "localhost", "127.0.0.1", 1)
        if !strings.HasPrefix(url, "https://") {
            url = "https://" + url
        }
        logger.Info("Forwarding request to: %s (attempt %d/%d)", url, i+1, maxRetries)

        switch method {
        case http.MethodGet:
            req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
        case http.MethodPost:
            if strings.Contains(url, "/batch_set") {
                req, err = http.NewRequest(method, url+"?api_key="+apiKey, bytes.NewBuffer([]byte(value)))
                req.Header.Set("Content-Type", "application/json")
            } else {
                body := map[string]string{"key": key, "value": value}
                jsonBody, _ := json.Marshal(body)
                req, err = http.NewRequest(method, url+"?api_key="+apiKey, bytes.NewBuffer(jsonBody))
                req.Header.Set("Content-Type", "application/json")
            }
        case http.MethodDelete:
            req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
        }
        if err != nil {
            logger.Error("Failed to create request for %s: %v", url, err)
            return nil, err
        }

        req.Header.Set("X-Forwarded", "true")
        start := time.Now()
        resp, err := client.Do(req)
        duration := time.Since(start)
        if err != nil {
            logger.Error("Forward request to %s failed after %v: %v", url, duration, err)
            if i < maxRetries-1 {
                time.Sleep(time.Duration(1<<i) * 500 * time.Millisecond)
            }
            continue
        }
        defer resp.Body.Close()
        logger.Info("Forward request to %s succeeded in %v", url, duration)

        body, err := io.ReadAll(resp.Body)
        if err != nil {
            logger.Error("Failed to read response body from %s: %v", url, err)
            return nil, err
        }

        var responses []Response
        err = json.Unmarshal(body, &responses)
        if err != nil {
            var singleResp Response
            if jsonErr := json.Unmarshal(body, &singleResp); jsonErr == nil {
                responses = []Response{singleResp}
            } else {
                logger.Error("Failed to unmarshal response from %s: %v", url, err)
                return nil, err
            }
        }
        return responses, nil
    }
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