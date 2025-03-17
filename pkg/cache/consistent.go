package cache

import (
    "hash/fnv"
    "sort"
    "strconv"
    "sync"
)

// 定义一致性哈希环的结构体
type HashRing struct {
    nodes       []string          // 存储实际节点的列表
    ring        map[uint32]string // 哈希环，key是哈希值，value是对应节点
    virtualNums int               // 每个节点的虚拟节点数量
    sortedKeys  []uint32          // 优化：缓存排序后的哈希值
    mu          sync.RWMutex      // 读写锁
}

// 创建一个新的哈希环实例
func NewHashRing(virtualNums int) *HashRing {
    return &HashRing{
        ring:        make(map[uint32]string),
        virtualNums: virtualNums,
        sortedKeys:  make([]uint32, 0),
        mu:          sync.RWMutex{},
    }
}

// 将一个节点添加到哈希环中
func (h *HashRing) AddNode(node string) {
    h.mu.Lock() // 写锁
    defer h.mu.Unlock()
    for i := 0; i < h.virtualNums; i++ {
        virtualKey := node + "-" + strconv.Itoa(i)
        f := fnv.New32a()
        f.Write([]byte(virtualKey))
        hash := f.Sum32()
        h.ring[hash] = node
    }
    h.updateNodes()
    h.updateSortedKeys()
}

// 从哈希环中移除一个节点
func (h *HashRing) RemoveNode(node string) {
    h.mu.Lock() // 写锁
    defer h.mu.Unlock()
    for i := 0; i < h.virtualNums; i++ {
        virtualKey := node + "-" + strconv.Itoa(i)
        f := fnv.New32a()
        f.Write([]byte(virtualKey))
        hash := f.Sum32()
        delete(h.ring, hash)
    }
    h.updateNodes()
    h.updateSortedKeys()
}

// 更新哈希环中实际节点列表
func (h *HashRing) updateNodes() {
    // 注意：此方法应在调用者加锁的情况下使用，无需单独加锁
    h.nodes = make([]string, 0, len(h.ring))
    seen := make(map[string]bool)
    for _, node := range h.ring {
        if !seen[node] {
            h.nodes = append(h.nodes, node)
            seen[node] = true
        }
    }
    sort.Strings(h.nodes)
}

// 更新排序后的哈希值缓存
func (h *HashRing) updateSortedKeys() {
    // 注意：此方法应在调用者加锁的情况下使用，无需单独加锁
    h.sortedKeys = make([]uint32, 0, len(h.ring))
    for k := range h.ring {
        h.sortedKeys = append(h.sortedKeys, k)
    }
    sort.Slice(h.sortedKeys, func(i, j int) bool { return h.sortedKeys[i] < h.sortedKeys[j] })
}

// 根据key获取对应目标节点
func (h *HashRing) GetNode(key string) string {
    h.mu.RLock() // 读锁
    defer h.mu.RUnlock()
    if len(h.ring) == 0 {
        return ""
    }
    f := fnv.New32a()
    f.Write([]byte(key))
    hash := f.Sum32()
    idx := sort.Search(len(h.sortedKeys), func(i int) bool { return h.sortedKeys[i] >= hash })
    if idx == len(h.sortedKeys) {
        idx = 0
    }
    return h.ring[h.sortedKeys[idx]]
}

// 返回哈希环中所有实际节点列表
func (h *HashRing) GetNodes() []string {
    h.mu.RLock() // 读锁
    defer h.mu.RUnlock()
    return append([]string{}, h.nodes...) // 返回副本，避免外部修改
}

// 返回键在移除某节点前的归属节点
func (h *HashRing) GetNodeForKeyBeforeRemoval(key, removedNode string) string {
    h.mu.RLock() // 读锁保护 h.ring 和 h.sortedKeys
    defer h.mu.RUnlock()

    // 临时加回 removedNode 的虚拟节点
    tempRing := make(map[uint32]string)
    for k, v := range h.ring {
        tempRing[k] = v
    }
    for i := 0; i < h.virtualNums; i++ {
        virtualKey := removedNode + "-" + strconv.Itoa(i)
        f := fnv.New32a()
        f.Write([]byte(virtualKey))
        hash := f.Sum32()
        tempRing[hash] = removedNode
    }

    // 计算键的哈希值
    f := fnv.New32a()
    f.Write([]byte(key))
    hash := f.Sum32()

    // 为 tempRing 生成排序后的键
    keys := make([]uint32, 0, len(tempRing))
    for k := range tempRing {
        keys = append(keys, k)
    }
    sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

    // 找到第一个大于等于 key 哈希值的节点
    idx := sort.Search(len(keys), func(i int) bool { return keys[i] >= hash })
    if idx == len(keys) {
        idx = 0
    }
    return tempRing[keys[idx]]
}

// getSortedKeys 仅用于临时副本（如 GetNodeForKeyBeforeRemoval），不直接访问 h.ring
// func (h *HashRing) getSortedKeys() []uint32 {
//     // 注意：此方法仅在特定场景使用，已被 h.sortedKeys 替代
//     h.mu.RLock()
//     defer h.mu.RUnlock()
//     keys := make([]uint32, 0, len(h.ring))
//     for k := range h.ring {
//         keys = append(keys, k)
//     }
//     sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
//     return keys
// }