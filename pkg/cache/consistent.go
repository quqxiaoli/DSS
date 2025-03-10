package cache

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashRing struct {
	nodes    []int
	nodeMap  map[int]string
	replicas int
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		nodes:    []int{},
		nodeMap:  make(map[int]string),
		replicas: replicas,
	}
}

// AddNode 向哈希环中添加一个节点，并为该节点创建指定数量的虚拟节点。
// 参数 node 是要添加的节点的名称或地址。
func (h *HashRing) AddNode(node string) {
	// 为每个节点创建多个虚拟节点，数量由 h.replicas 指定
	for i := 0; i < h.replicas; i++ {
		// 计算虚拟节点的哈希值，通过将节点名和副本编号拼接后计算 CRC32 哈希
		hash := int(crc32.ChecksumIEEE([]byte(node + strconv.Itoa(i))))
		// 将虚拟节点的哈希值添加到节点哈希列表中
		h.nodes = append(h.nodes, hash)
		// 将虚拟节点的哈希值映射到实际节点
		h.nodeMap[hash] = node
	}
	// 对节点哈希列表进行排序，确保后续查找节点时可以使用二分查找
	sort.Ints(h.nodes)
}

func (h *HashRing) GetNode(key string) string {
	if len(h.nodes) == 0 {
		return ""
	}
	hash := int(crc32.ChecksumIEEE([]byte(key)))
	idx := sort.SearchInts(h.nodes, hash)
	if idx == len(h.nodes) {
		idx = 0
	}
	return h.nodeMap[h.nodes[idx]]
}
func (h *HashRing) GetNodes() []string {
	nodes := make([]string, 0, len(h.nodeMap))
	seen := make(map[string]bool) // 去重。
	for _, node := range h.nodeMap {
		if !seen[node] {
			nodes = append(nodes, node)
			seen[node] = true
		}
	}
	return nodes
}
