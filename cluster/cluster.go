// Package cluster provides a server side cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster
package cluster

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/hdt3213/rdb/core"

	"os"
	"path"
	"sync"

	"github.com/hdt3213/godis/config"
	database2 "github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/idgenerator"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

// Cluster represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
type Cluster struct {
	self          string                   // 本节点的标识
	addr          string                   // 当前节点的公告地址
	db            database.DBEngine        // 数据库引擎接口
	transactions  *dict.SimpleDict         // 事务字典，映射事务ID到事务对象
	transactionMu sync.RWMutex             // 事务的读写锁
	topology      topology                 // 集群的拓扑结构,定义了集群拓扑的操作，包括获取节点信息、设置槽的归属节点等。
	slotMu        sync.RWMutex             // Slot的读写锁
	slots         map[uint32]*hostSlot     // Slot映射，每个Slot分配给一个host
	idGenerator   *idgenerator.IDGenerator // ID生成器

	clientFactory clientFactory // 客户端工厂，用于管理和创建与其他节点的连接
}

// peerClient 负责发送命令并接收回复，
type peerClient interface {
	Send(args [][]byte) redis.Reply
}

// peerStream 负责管理数据流
type peerStream interface {
	Stream() <-chan *parser.Payload
	Close() error
}

// clientFactory 负责创建和管理这些客户端和流。
type clientFactory interface {
	GetPeerClient(peerAddr string) (peerClient, error)
	ReturnPeerClient(peerAddr string, peerClient peerClient) error
	NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error)
	Close() error
}

const (
	slotStateHost      = iota // Slot 状态：本节点持有
	slotStateImporting        // Slot 状态：正在导入
	slotStateMovingOut        // Slot 状态：正在迁出
)

// hostSlot 存储当前节点托管的主机状态
type hostSlot struct {
	state        uint32       // 表示 slot 的状态（例如：托管、导入中、迁出中）
	mu           sync.RWMutex // 用于并发控制，保护 slot 状态和数据的修改
	oldNodeID    string       // 正在迁出此 slot 的节点ID，只在导入时有效
	newNodeID    string       // 正在导入此 slot 的节点ID，只在迁出时有效
	importedKeys *set.Set     // 在迁移过程中存储已导入的键，用于在迁移中识别和处理键
	keys         *set.Set     // 存储此 slot 中所有键，用于跟踪和管理键的存在
}

var allowFastTransaction = true // 如果事务只涉及一个节点，就直接执行命令，不应用两阶段提交（TCC）协议
// MakeCluster 创建并启动一个集群节点
func MakeCluster() *Cluster {
	cluster := &Cluster{
		self:          config.Properties.Self,                            // 当前节点的标识，从配置文件读入
		addr:          config.Properties.AnnounceAddress(),               // 当前节点的公告地址
		db:            database2.NewStandaloneServer(),                   // 创建独立服务器实例作为数据库，单节点的db
		transactions:  dict.MakeSimple(),                                 // 初始化事务字典
		idGenerator:   idgenerator.MakeGenerator(config.Properties.Self), // 初始化 ID 生成器
		clientFactory: newDefaultClientFactory(),                         // 初始化客户端工厂
	}
	topologyPersistFile := path.Join(config.Properties.Dir, config.Properties.ClusterConfigFile) // 拓扑配置文件路径
	cluster.topology = newRaft(cluster, topologyPersistFile)                                     // 初始化 Raft 协议管理拓扑
	cluster.db.SetKeyInsertedCallback(cluster.makeInsertCallback())                              // 设置键插入回调
	cluster.db.SetKeyDeletedCallback(cluster.makeDeleteCallback())                               // 设置键删除回调
	cluster.slots = make(map[uint32]*hostSlot)
	// 根据配置决定如何初始化集群节点（作为种子节点启动或加入已有集群）                                             // 初始化 slot 映射
	var err error
	if topologyPersistFile != "" && fileExists(topologyPersistFile) {
		err = cluster.LoadConfig() // 加载已有配置
	} else if config.Properties.ClusterAsSeed {
		err = cluster.startAsSeed(config.Properties.AnnounceAddress()) // 作为种子节点启动
	} else {
		err = cluster.Join(config.Properties.ClusterSeed) // 加入已有集群
	}
	if err != nil {
		panic(err)
	}
	return cluster
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply

// Close 停止当前集群节点
func (cluster *Cluster) Close() {
	_ = cluster.topology.Close()  // 关闭拓扑管理
	cluster.db.Close()            // 关闭数据库
	cluster.clientFactory.Close() // 关闭客户端工厂
}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

// Exec 在集群中执行命令
func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "info" {
		if ser, ok := cluster.db.(*database2.Server); ok {
			return database2.Info(ser, cmdLine[1:]) // 处理 info 命令
		}
	}
	if cmdName == "auth" {
		return database2.Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}

	if cmdName == "dbsize" {
		if ser, ok := cluster.db.(*database2.Server); ok {
			return database2.DbSize(c, ser)
		}
	}

	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return database2.StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return database2.DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(cluster, c, nil)
	} else if cmdName == "select" {
		return protocol.MakeErrReply("select not supported in cluster")
	}
	if c != nil && c.InMultiState() {
		return database2.EnqueueCmd(c, cmdLine)
	}
	cmdFunc, ok := router[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, cmdLine)
	return
}

// AfterClientClose 当客户端关闭连接后进行一些清理工作
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	// 调用数据库的 AfterClientClose 方法进行清理，这可能包括移除任何客户端特定的状态或数据
	cluster.db.AfterClientClose(c)
}

func (cluster *Cluster) LoadRDB(dec *core.Decoder) error {
	return cluster.db.LoadRDB(dec)
}

// makeInsertCallback 返回处理键插入事件的回调函数
func (cluster *Cluster) makeInsertCallback() database.KeyEventCallback {
	return func(dbIndex int, key string, entity *database.DataEntity) {
		// 根据键计算其应该属于的 slot
		slotId := getSlot(key)
		// 使用读锁来访问 slots 映射，以保证线程安全
		cluster.slotMu.RLock()
		slot, ok := cluster.slots[slotId]
		cluster.slotMu.RUnlock()
		// 如果 slot 存在，对其加锁进行修改，确保键插入操作的原子性
		if ok {
			slot.mu.Lock()
			defer slot.mu.Unlock()
			// 将新键添加到对应 slot 的 keys 集合中
			slot.keys.Add(key)
		}
	}
}

// makeDeleteCallback 返回处理键删除事件的回调函数
func (cluster *Cluster) makeDeleteCallback() database.KeyEventCallback {
	return func(dbIndex int, key string, entity *database.DataEntity) {
		slotId := getSlot(key) // 根据键计算其应该属于的 slot
		cluster.slotMu.RLock()
		slot, ok := cluster.slots[slotId]
		cluster.slotMu.RUnlock()
		// As long as the command is executed, we should update slot.keys regardless of slot.state
		if ok {
			slot.mu.Lock()
			defer slot.mu.Unlock()
			slot.keys.Remove(key)
		}
	}
}

// fileExists 检查指定文件是否存在并且不是目录
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}
