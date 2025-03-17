package cache

import (
	"hash/fnv"
	"testing"
	"time"
)

// 测试 Set 和 Get
func TestCacheSetGet(t *testing.T) {
	c := NewCache(2)
	c.Set("key1", "value1")
	val, exists := c.Get("key1")
	if !exists {
		t.Errorf("key1 should exist, but does not")
	}
	if val != "value1" {
		t.Errorf("value for key1 should be value1, but got %s", val)
	}
}

//测试TTL
func TestCacheTTL(t *testing.T) {
	c := NewCache(2)
	c.Set("key1", "value1")
    time.Sleep(6 * time.Second) // 等待 6 秒，超过 5 秒 TTL
    _, exists := c.Get("key1")
    if exists {
        t.Errorf("key1 应该过期，但仍然存在")
    }
}

// 测试 LRU 淘汰
func TestCacheLRU(t *testing.T) {
    c := NewCache(2) // 容量为 2
    c.Set("key1", "value1")
    c.Set("key2", "value2")
    c.Set("key3", "value3") // 超出容量，key1 应被淘汰
    _, exists := c.Get("key1")
    if exists {
        t.Errorf("key1 应该被淘汰，但仍然存在")
    }
    val, exists := c.Get("key2")
    if !exists || val != "value2" {
        t.Errorf("key2 应该存在且值为 value2，得到 %s", val)
    }
}



func TestHashRingDistribution(t *testing.T) {
    ring := NewHashRing(10)
    nodes := []string{"node1", "node2", "node3"}
    for _, node := range nodes {
        ring.AddNode(node)
    }

    // 使用更多样化的 key，覆盖不同哈希范围
    keys := []string{
        "user1",        // 不同前缀
        "product42",    // 带数字
        "session-abc",  // 带符号
        "order12345",   // 较长字符串
        "cacheXYZ",     // 大写
        "test",         // 短字符串
        "data-2023",    // 带年份
    }
    nodeCount := make(map[string]int)
    for _, key := range keys {
        hash := ring.hashKey(key) // 用 hashKey 方法调试
        node := ring.GetNode(key)
        nodeCount[node]++
        t.Logf("Key %s: hash=%d, node=%s", key, hash, node)
    }

    // 期望至少分布到 2 个节点
    if len(nodeCount) < 2 {
        t.Errorf("期望分布到多个节点，但只用了 %d 个: %v", len(nodeCount), nodeCount)
    } else {
        t.Logf("分布结果: %v", nodeCount)
    }
}

// 添加 hashKey 方法，与 GetNode 一致
func (h *HashRing) hashKey(key string) uint32 {
    f := fnv.New32a()
    f.Write([]byte(key))
    return f.Sum32()
}

func TestHashRingRemoveNode(t *testing.T) {
    ring := NewHashRing(10)
    nodes := []string{"node1", "node2", "node3"}
    for _, node := range nodes {
        ring.AddNode(node)
    }

    // 选择一个测试 key
    key := "test-key"
    nodeBefore := ring.GetNode(key)
    t.Logf("Before removal: %s -> %s (hash=%d)", key, nodeBefore, ring.hashKey(key))

    // 移除 node2
    ring.RemoveNode("node2")
    nodeAfter := ring.GetNode(key)
    t.Logf("After removal: %s -> %s (hash=%d)", key, nodeAfter, ring.hashKey(key))

    // 检查 nodes 列表
    remainingNodes := ring.GetNodes()
    t.Logf("Remaining nodes: %v", remainingNodes)

    // 验证
    if nodeBefore == "node2" && nodeAfter == "node2" {
        t.Errorf("移除 node2 后，key %s 仍分配到 node2", key)
    }
    if len(remainingNodes) != 2 {
        t.Errorf("期望剩余 2 个节点，实际 %d 个: %v", len(remainingNodes), remainingNodes)
    }
    foundNode2 := false
    for _, n := range remainingNodes {
        if n == "node2" {
            foundNode2 = true
            break
        }
    }
    if foundNode2 {
        t.Errorf("node2 未被正确移除，仍存在于节点列表: %v", remainingNodes)
    }
}