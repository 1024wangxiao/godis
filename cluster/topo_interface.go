package cluster

import (
	"hash/crc32"
	"strings"
	"time"

	"github.com/hdt3213/godis/redis/protocol"
)

// Slot 代表一个哈希槽，此处的槽就是用于解决一致性哈希方案，每个槽负责存储一部分数据
// 集群中的节点可以负责一个或者多个槽，此处的槽就对应着虚拟节点。
type Slot struct {
	ID     uint32 // 槽ID，范围从0到16383
	NodeID string // 托管此槽的节点ID
	Flags  uint32 // 存储槽的更多信息，比如迁移状态等
}

// getPartitionKey 从键中提取哈希标签，如果存在的话
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key // 如果没有发现 '{'，返回整个键
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key // 如果没有发现 '}' 或者 '{}' 是空的，返回整个键
	}
	return key[beg+1 : end] // 返回 '{}' 中间的部分作为哈希标签
}

// getSlot 函数：计算给定键的哈希槽ID。使用 CRC32 算法对键进行哈希，然后将哈希值对槽数量取模。
func getSlot(key string) uint32 {
	partitionKey := getPartitionKey(key)                                // 获取分区键
	return crc32.ChecksumIEEE([]byte(partitionKey)) % uint32(slotCount) // 使用CRC32计算哈希，然后取模
}

// Node 结构：表示集群中的节点，包含节点ID、地址和持有的槽列表。
type Node struct {
	ID        string    // 节点ID
	Addr      string    // 节点地址
	Slots     []*Slot   // 持有的槽，按槽ID升序排列
	Flags     uint32    // 存储节点的更多信息，比如是否可用等
	lastHeard time.Time // 最后一次从此节点接收到消息的时间
}

// topology 接口：定义了集群拓扑的操作，包括获取节点信息、设置槽的归属节点等。
type topology interface {
	GetSelfNodeID() string                                          // 获取当前节点的ID
	GetNodes() []*Node                                              // 所有节点的复制列表
	GetNode(nodeID string) *Node                                    // 根据节点ID获取节点
	GetSlots() []*Slot                                              // 获取所有槽的信息
	StartAsSeed(addr string) protocol.ErrorReply                    // 将节点设置为种子节点并开始集群
	SetSlot(slotIDs []uint32, newNodeID string) protocol.ErrorReply // 设置槽的归属节点
	LoadConfigFile() protocol.ErrorReply                            // 加载配置文件
	Join(seed string) protocol.ErrorReply                           // 将当前节点加入到由种子节点指定的集群
	Close() error                                                   // 关闭拓扑管理，释放资源
}
