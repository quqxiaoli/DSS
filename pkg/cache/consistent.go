package cache

import (
    "hash/fnv"
    "sort"
    "strconv"
)

type HashRing struct {
    nodes       []string
    ring        map[uint32]string
    virtualNums int
}

func NewHashRing(virtualNums int) *HashRing {
    return &HashRing{
        ring:        make(map[uint32]string),
        virtualNums: virtualNums,
    }
}

func (h *HashRing) AddNode(node string) {
    for i := 0; i < h.virtualNums; i++ {
        virtualKey := node + "-" + strconv.Itoa(i)
        f := fnv.New32a()
        f.Write([]byte(virtualKey))
        hash := f.Sum32()
        h.ring[hash] = node
    }
    h.updateNodes()
}

func (h *HashRing) updateNodes() {
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

func (h *HashRing) GetNode(key string) string {
    if len(h.ring) == 0 {
        return ""
    }
    f := fnv.New32a()
    f.Write([]byte(key))
    hash := f.Sum32()
    keys := h.sortedKeys()
    idx := sort.Search(len(keys), func(i int) bool { return keys[i] >= hash })
    if idx == len(keys) {
        idx = 0
    }
    return h.ring[keys[idx]]
}

func (h *HashRing) sortedKeys() []uint32 {
    keys := make([]uint32, 0, len(h.ring))
    for k := range h.ring {
        keys = append(keys, k)
    }
    sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
    return keys
}

func (h *HashRing) GetNodes() []string {
    return h.nodes
}