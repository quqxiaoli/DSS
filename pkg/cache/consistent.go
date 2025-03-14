package cache

import (
	"hash/fnv" //计算 FNV-1 哈希值
	"sort"     //排序
	"strconv"  //字符串转换
)

// 定义一致性哈希环的结构体
type HashRing struct {
	nodes       []string          //存储实际节点的列表
	ring        map[uint32]string //哈希环，key是哈希值，value是对应节点
	virtualNums int               //每个及诶单的虚拟节点数量
	sortedKeys  []uint32          // 优化：缓存排序后的哈希值
}

// 创建一个新的哈希环实例
func NewHashRing(virtualNums int) *HashRing {
	return &HashRing{
		ring:        make(map[uint32]string), //初始化哈希环的map
		virtualNums: virtualNums,             //设置虚拟节点数量
		sortedKeys:  make([]uint32, 0),       // 优化：初始化sortedKeys
	}
}

// 将一个集节点添加到哈希环中
func (h *HashRing) AddNode(node string) { //node是要添加节点的名称
	//为每个节点添加虚拟节点
	for i := 0; i < h.virtualNums; i++ {
		virtualKey := node + "-" + strconv.Itoa(i) //生成虚拟节点的key
		f := fnv.New32a()                          //创建一个新的32为哈希对象
		f.Write([]byte(virtualKey))                //将虚拟节点写入哈希对象
		hash := f.Sum32()                          //计算哈希值
		h.ring[hash] = node                        //加入哈希环中
	}
	h.updateNodes()      //更新nodes列表，去重并且排序
	h.updateSortedKeys() // 优化：更新缓存的排序keys
}

// 更新哈希环中实际节点列表
func (h *HashRing) updateNodes() {
	h.nodes = make([]string, 0, len(h.ring)) //初始化nodes切片
	seen := make(map[string]bool)            //使用map去重
	for _, node := range h.ring {
		if !seen[node] { //节点未出现过添加
			h.nodes = append(h.nodes, node)
			seen[node] = true
		}
	}
	sort.Strings(h.nodes)
}

// 优化：更新排序后的哈希值缓存
func (h *HashRing) updateSortedKeys() {
	h.sortedKeys = make([]uint32, 0, len(h.ring))
	for k := range h.ring {
		h.sortedKeys = append(h.sortedKeys, k)
	}
	sort.Slice(h.sortedKeys, func(i, j int) bool { return h.sortedKeys[i] < h.sortedKeys[j] })
}

// 根据key获取对应目标节点
func (h *HashRing) GetNode(key string) string {
	if len(h.ring) == 0 { //空返回空
		return ""
	}
	f := fnv.New32a()    //创建一个新的哈希对象
	f.Write([]byte(key)) //将key写入
	hash := f.Sum32()    //计算其哈希值
	// 优化：使用缓存的sortedKeys，避免每次排序
	idx := sort.Search(len(h.sortedKeys), func(i int) bool { return h.sortedKeys[i] >= hash })
	if idx == len(h.sortedKeys) { //超出范围则选择第一个
		idx = 0
	}
	return h.ring[h.sortedKeys[idx]]
}

// 返回哈希环中所有哈希值的排序切片
func (h *HashRing) getSortedKeys() []uint32 { // 修改：重命名为getSortedKeys，避免冲突
	keys := make([]uint32, 0, len(h.ring)) //初始化哈希值切片，预分配容量
	for k := range h.ring {                //便利哈希环，收集所有哈希值
		keys = append(keys, k)
	}
	//对哈希值升序排序，确保一致性哈希顺序查找
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// 从哈希环中移除一个节点
func (h *HashRing) RemoveNode(node string) {
	for i := 0; i < h.virtualNums; i++ {
		virtualKey := node + "-" + strconv.Itoa(i)
		f := fnv.New32a()
		f.Write(([]byte(virtualKey)))
		hash := f.Sum32()
		delete(h.ring, hash) //从哈希环删除虚拟节点
	}
	h.updateNodes()      //更新nodes列表
	h.updateSortedKeys() // 优化：更新缓存的排序keys
}

// 返回哈希环中所有实际节点列表
func (h *HashRing) GetNodes() []string {
	return h.nodes
}

// GetNodeForKeyBeforeRemoval 返回键在移除某节点前的归属节点
func (h *HashRing) GetNodeForKeyBeforeRemoval(key, removedNode string) string {
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

	// 获取排序后的哈希值列表
	keys := h.getSortedKeys() // 修改：调用重命名后的getSortedKeys
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// 找到第一个大于等于 key 哈希值的节点
	idx := sort.Search(len(keys), func(i int) bool { return keys[i] >= hash })
	if idx == len(keys) {
		idx = 0
	}
	return tempRing[keys[idx]]
}
