package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

// HashFunc 定义用于生成哈希码的函数类型
type HashFunc func(data []byte) uint32

// Map 存储节点并允许从中选择节点的数据结构
type Map struct {
	hashFunc HashFunc       // 用于生成哈希的函数
	replicas int            // 每个节点的虚拟节点数（副本数），增加分布均匀性
	keys     []int          // 已排序的哈希环上的哈希值列表
	hashMap  map[int]string // 哈希值到节点名称的映射
}

// New 创建一个新的哈希环
func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		replicas: replicas,
		hashFunc: fn,
		hashMap:  make(map[int]string),
	}
	// 如果没有传入哈希函数，默认使用 crc32.ChecksumIEEE 算法作为哈希函数，
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty 检查哈希环是否为空
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// AddNode 将给定的节点添加到一致性哈希环中
func (m *Map) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			// 为每个节点生成多个虚拟节点以增加其在哈希环上的分布
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys) // 对哈希环上的哈希值进行排序，以便后续快速查找
}

// getPartitionKey 支持哈希标签，从带有{}的键中提取实际用于哈希的部分
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key // 不含标签，直接返回原键
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key // 标签格式不正确，返回原键
	}
	return key[beg+1 : end] // 返回{}内的内容作为键的一部分
}

// PickNode 根据提供的键选择最近的节点
func (m *Map) PickNode(key string) string {
	if m.IsEmpty() {
		return "" // 哈希环为空，返回空字符串
	}

	partitionKey := getPartitionKey(key) // 获取实际用于哈希的键部分
	hash := int(m.hashFunc([]byte(partitionKey)))

	// 使用二分搜索找到适当的虚拟节点
	//返回的是节点的序号
	//例如 【12343, 23412, 46446】,此时进来的哈希值为18989 则返回的index为2即插入到23412后面
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// 如果搜索到的索引等于键的长度，说明应环绕到哈希环的起点
	if idx == len(m.keys) {
		//如果落在最后则去0号节点
		idx = 0
	}

	return m.hashMap[m.keys[idx]] // 返回找到的节点名称
}
