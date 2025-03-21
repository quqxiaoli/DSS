package api

import "expvar"

type Response struct {
	Status string `json:"status"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
	Error  string `json:"error,omitempty"`
}

var (
	GetLatency = expvar.NewFloat("get_latency")
	SetLatency = expvar.NewFloat("set_latency")
)

// GetRequest 定义异步获取请求的结构
type GetRequest struct {
	Key    string
	Result chan struct {
		Value  string
		Exists bool
	}
}
