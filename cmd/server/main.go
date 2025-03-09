package main

import (
    "bytes"             //处理POST请求的字节缓冲区
    "encoding/json"     //处理JSON数据
    "flag"              //解析命令行参数
    "fmt"               //格式化输出
    "io"                //读取 HTTP 响应体
    "log"               //日志记录
    "net/http"          //实现HTTP服务器和客户端
    "os"                //文件操作
    "github.com/quqxiaoli/distributed-cache/pkg/cache"
)

//HTTP相应的JSON结构
type Response struct {
    Status string `json:"status"`
    Key    string `json:"key,omitempty"`
    Value  string `json:"value,omitempty"`
    Error  string `json:"error,omitempty"`
}

//封装/get请求的参数，用于并发处理
type GetRequest struct {
    key    string
    result chan struct {  //结果通道，返回查询结果
        value  string
        exists bool
    }
}

//全局日志变量，延迟初始化
var (
    infoLogger  *log.Logger //普通日志
    errorLogger *log.Logger //错误日志
)

const validAPIKey = "my-secret-key" //密匙

//后台go协程处理/get请求，减少锁竞争
func handleGets(localCache *cache.Cache, ch chan GetRequest) {
    for req := range ch { 
        value, exists := localCache.Get(req.key) //从本地缓存查询键
        req.result <- struct { //将结果发送到通道
            value  string
            exists bool
        }{value, exists}
    }
}

//forwaedRequest 转发HTTP 请求到其他节点
func forwardRequest(method, url, apiKey, key, value string) (*Response, error) {
    var req *http.Request //请求对象：待构造的HTTP 请求
    var err error         //错误变量：记录构造或发送请求时的错误
	//根据请求方法构造不同的HTTP请求
    if method == http.MethodGet { // GET请求查键值
        req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
    } else if method == http.MethodPost { //POST设置或同步键值
        body := map[string]string{"key": key, "value": value} //请求体：键值对
        jsonBody, _ := json.Marshal(body)                     //转换为JSON
        req, err = http.NewRequest(method, url+"?api_key="+apiKey, bytes.NewBuffer(jsonBody))
        req.Header.Set("Content-Type", "application/json")//设置头：指定JSON格式
    } else if method == http.MethodDelete {//delete请求
        req, err = http.NewRequest(method, url+"?key="+key+"&api_key="+apiKey, nil)
    }
    if err != nil {//检查是否出错
        return nil, err
    }
    client := &http.Client{}//客户端用于发送HTTP请求
    resp, err := client.Do(req)//发送请求并获取响应
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()//延迟关闭
    body, err := io.ReadAll(resp.Body)//读取响应体内容
    if err != nil {
        return nil, err
    }
    var response Response//存储反序列化结果
    err = json.Unmarshal(body, &response)//反序列化响应体
    return &response, err//返回响应和可能的错误
}

func main() {
	//解析命令行参数
    port := flag.String("port", "8080", "server port")//参数：服务器端口，默认8080
    addr := flag.String("addr", "localhost:8080", "local address")//本地地址默认localhost：8080
    flag.Parse()//解析命令行输入

	//创建日志文件，按端口命名
    logFile, err := os.OpenFile(fmt.Sprintf("cache_%s.log", *port), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {//检查文件打开是否失败
        log.Fatalf("Failed to open log file: %v", err)
    }
    defer logFile.Close()

	//初始化日志记录器
    infoLogger = log.New(logFile, fmt.Sprintf("[INFO %s] ", *addr), log.LstdFlags)//信息日志：写入文件
    errorLogger = log.New(logFile, fmt.Sprintf("[ERROR %s] ", *addr), log.LstdFlags)//错误日志：写入文件
    log.SetFlags(log.LstdFlags | log.Lshortfile)//日志格式包含时间和文件行号
    infoLogger.Printf("启动分布式缓存服务器...")

    localCache := cache.NewCache()//创建本地缓存
    getCh := make(chan GetRequest, 10)//带缓冲通道用于/get请求
    go handleGets(localCache, getCh)//启动go协程处理并发查询

    ring := cache.NewHashRing(3)//创建一致性哈希环，三个副本
    ring.AddNode("localhost:8080")
    ring.AddNode("localhost:8081")
    ring.AddNode("localhost:8082")

    localAddr := *addr//当前节点地址
    nodes := ring.GetNodes() // 直接用 GetNodes。

	//处理其他节点的同步请求
    http.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
        apiKey := r.URL.Query().Get("api_key")
        if apiKey != validAPIKey {
            errorLogger.Printf("Invalid API key: %s", apiKey)
            w.WriteHeader(http.StatusUnauthorized)
            json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
            return
        }
        var body map[string]string //存储请求体
        if err := json.NewDecoder(r.Body).Decode(&body); err != nil {//从请求体读取JSON
            w.WriteHeader(http.StatusBadRequest)//400错误
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
        localCache.Set(key, value)//存入本地缓存
        infoLogger.Printf("Sync request: key=%s, value=%s", key, value)//同步日志
        w.Header().Set("Content-Type", "application/json")//设置响应头为JSON
        json.NewEncoder(w).Encode(Response{Status: "ok", Key: key, Value: value})
    })

	//处理get请求
    http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
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

        node := ring.GetNode(key)//用一致性哈希确定目标节点
        infoLogger.Printf("Set request: key=%s, value=%s, node=%s", key, value, node)
        w.Header().Set("Content-Type", "application/json")
        if key == "" || value == "" {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Key or value cannot be empty"})
            return
        }

        if node == localAddr {//如果当前节点是目标节点
            localCache.Set(key, value) // 先存本地。
            go func() {//异步广播给其他节点
                for _, n := range nodes {//遍历节点
                    if n != localAddr {
                        _, err := forwardRequest(http.MethodPost, "http://"+n+"/sync", apiKey, key, value)//转发同步请求
                        if err != nil {
                            errorLogger.Printf("Failed to sync to %s: %v", n, err)
                        }
                    }
                }
            }()
            json.NewEncoder(w).Encode(Response{Status: "ok", Key: key, Value: value})
        } else {//若当前节点不是目标节点
            resp, err := forwardRequest(http.MethodPost, "http://"+node+"/set", apiKey, key, value)//转发给目标节点
            if err != nil {
                errorLogger.Printf("Failed to forward set request: %v", err)
                w.WriteHeader(http.StatusInternalServerError)
                json.NewEncoder(w).Encode(Response{Error: "Failed to forward request"})
                return
            }
            localCache.Set(key, value) // 转发后存本地。
            json.NewEncoder(w).Encode(resp)//响应转发结果
        }
    })

	//处理get请求
    http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
        apiKey := r.URL.Query().Get("api_key")
        if apiKey != validAPIKey {
            errorLogger.Printf("Invalid API key: %s", apiKey)
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusUnauthorized)
            json.NewEncoder(w).Encode(Response{Error: "Invalid API key"})
            return
        }
        key := r.URL.Query().Get("key")
        infoLogger.Printf("Get request: key=%s", key) // 直接本地取，不查 node。
        w.Header().Set("Content-Type", "application/json")
        if key == "" {
            w.WriteHeader(http.StatusBadRequest)
            json.NewEncoder(w).Encode(Response{Error: "Key cannot be empty"})
            return
        }

        resultCh := make(chan struct{ value string; exists bool })//结果通道
        getCh <- GetRequest{key, resultCh}//发送查询请求到通道
        result := <-resultCh//从通道获取结果
        if result.exists {
            json.NewEncoder(w).Encode(Response{Status: "ok", Key: key, Value: result.value})
        } else {
            json.NewEncoder(w).Encode(Response{Status: "not found", Key: key})
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
            resp, err := forwardRequest(http.MethodDelete, "http://"+node+"/delete", apiKey, key, "")
            if err != nil {
                errorLogger.Printf("Failed to forward delete request: %v", err)
                w.WriteHeader(http.StatusInternalServerError)
                json.NewEncoder(w).Encode(Response{Error: "Failed to forward request"})
                return
            }
            json.NewEncoder(w).Encode(resp)
        }
    })

    fmt.Printf("Server running on :%s\n", *port)
    http.ListenAndServe(":"+*port, nil)
}