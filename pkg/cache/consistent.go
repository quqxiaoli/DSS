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

func (h *HashRing) AddNode(node string) {
	for i := 0; i < h.replicas; i++ {
		hash := int(crc32.ChecksumIEEE([]byte(node + strconv.Itoa(i))))
		h.nodes = append(h.nodes, hash)
		h.nodeMap[hash] = node
	}
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
