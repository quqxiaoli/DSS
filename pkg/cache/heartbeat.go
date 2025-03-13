package cache

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

type Heartbeat struct {
	ring        *HashRing
	localAddr   string
	apiKey      string
	infoLogger  *log.Logger
	errorLogger *log.Logger
	failures    map[string]int
	client      *http.Client
	cache       *Cache
}

func NewHeartbeat(ring *HashRing, localAddr, apiKey string, infoLogger, errorLogger *log.Logger, cache *Cache) *Heartbeat {
	return &Heartbeat{
		ring:        ring,
		localAddr:   localAddr,
		apiKey:      apiKey,
		infoLogger:  infoLogger,
		errorLogger: errorLogger,
		failures:    make(map[string]int),
		client: &http.Client{
			Timeout: 3 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
				MaxIdleConns:      100,
				IdleConnTimeout:   90 * time.Second,
				DisableKeepAlives: false,
			},
		},
		cache: cache,
	}
}

func (h *Heartbeat) Start() {
	h.infoLogger.Printf("Waiting for nodes to initialize...")
	time.Sleep(5 * time.Second)
	go func() {
		for {
			nodes := h.ring.GetNodes()
			for _, node := range nodes {
				if node != h.localAddr {
					resp, err := h.checkHealth(node)
					if err != nil || resp.Status != "ok" {
						h.failures[node]++
						if err != nil {
							h.errorLogger.Printf("Node %s check failed: %v, failures: %d", node, err, h.failures[node])
						} else {
							h.errorLogger.Printf("Node %s status %s, failures: %d", node, resp.Status, h.failures[node])
						}
						if h.failures[node] >= 3 {
							h.errorLogger.Printf("Node %s confirmed unhealthy, removing from ring", node)
							h.ring.RemoveNode(node)
							h.migrateData(node)
							delete(h.failures, node)
						}
					} else {
						h.infoLogger.Printf("Node %s is healthy", node)
						h.failures[node] = 0
					}
				}
			}
			time.Sleep(5 * time.Second)
		}
	}()
}

func (h *Heartbeat) migrateData(failedNode string) {
	h.infoLogger.Printf("Starting data migration from failed node %s", failedNode)

	for key, value := range h.cache.GetAll() {
		oldNode := h.ring.GetNodeForKeyBeforeRemoval(key, failedNode)
		if oldNode == failedNode {
			newNode := h.ring.GetNode(key)
			if newNode != h.localAddr {
				err := h.sendDataToNode(newNode, key, value)
				if err != nil {
					h.errorLogger.Printf("Failed to migrate key %s to %s: %v", key, newNode, err)
				} else {
					h.infoLogger.Printf("Migrated key %s from %s to %s", key, failedNode, newNode)
				}
			} else {
				h.infoLogger.Printf("Key %s stays on local node %s", key, h.localAddr)
			}
		}
	}
	h.infoLogger.Printf("Data migration from %s completed", failedNode)
}

func (h *Heartbeat) sendDataToNode(node, key, value string) error {
	url := "https://" + node + "/set"
	reqBody := fmt.Sprintf(`{"key": "%s", "value": "%s"}`, key, value)
	req, err := http.NewRequest(http.MethodPost, url+"?api_key="+h.apiKey, strings.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func (h *Heartbeat) checkHealth(node string) (*Response, error) {
	url := "https://" + node + "/health"
	if !strings.HasPrefix(node, "http://") && !strings.HasPrefix(node, "https://") {
		url = "https://" + node + "/health"
	}
	req, err := http.NewRequest(http.MethodGet, url+"?api_key="+h.apiKey, nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.client.Do(req)
	if err != nil {
		h.errorLogger.Printf("Health check to %s failed: %v", node, err)
		return nil, err
	}
	defer resp.Body.Close()
	h.infoLogger.Printf("Health check to %s returned status code: %d", node, resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
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

type Response struct {
	Status string `json:"status"`
}
