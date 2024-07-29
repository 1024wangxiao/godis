package cluster

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

// relay 函数根据 peerId 将命令转发到相应的节点或在本节点执行。
func (cluster *Cluster) relay(peerId string, c redis.Connection, cmdLine [][]byte) redis.Reply {
	// 如果是转发到自己，直接在本地执行命令
	if peerId == cluster.self {
		// to self db
		return cluster.Exec(c, cmdLine)
	}
	// 从客户端工厂获取与对应节点的连接
	cli, err := cluster.clientFactory.GetPeerClient(peerId)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		// 命令执行完毕后返回连接到池中
		_ = cluster.clientFactory.ReturnPeerClient(peerId, cli)
	}()
	return cli.Send(cmdLine) // 发送命令并返回结果
}

// relayByKey 根据键值的哈希槽转发命令到相应的节点。
func (cluster *Cluster) relayByKey(routeKey string, c redis.Connection, args [][]byte) redis.Reply {
	slotId := getSlot(routeKey)
	peer := cluster.pickNode(slotId)
	return cluster.relay(peer.ID, c, args)
}

// broadcast 广播命令到集群的所有节点。
func (cluster *Cluster) broadcast(c redis.Connection, args [][]byte) map[string]redis.Reply {
	result := make(map[string]redis.Reply)
	for _, node := range cluster.topology.GetNodes() {
		reply := cluster.relay(node.ID, c, args)
		result[node.Addr] = reply
	}
	return result
}

/*
ensureKey和ensureKeyWithoutLock函数用于处理键的迁移，
当集群中的一个哈希槽正在被迁移到当前节点时，它会从旧节点获取键的值并将其导入到当前节点。
这种机制对于维护集群中数据的一致性非常关键。
*/
// ensureKey 确保键在当前节点，如果不在则进行迁移。
func (cluster *Cluster) ensureKey(key string) protocol.ErrorReply {
	slotId := getSlot(key)
	cluster.slotMu.RLock()
	slot := cluster.slots[slotId]
	cluster.slotMu.RUnlock()
	if slot == nil {
		return nil
	}
	if slot.state != slotStateImporting || slot.importedKeys.Has(key) {
		return nil // 如果槽不需要迁入或键已迁入，直接返回
	}
	// 请求旧节点的键数据
	resp := cluster.relay(slot.oldNodeID, connection.NewFakeConn(), utils.ToCmdLine("DumpKey_", key))
	if protocol.IsErrorReply(resp) {
		return resp.(protocol.ErrorReply)
	}
	if protocol.IsEmptyMultiBulkReply(resp) {
		slot.importedKeys.Add(key)
		return nil
	}
	dumpResp := resp.(*protocol.MultiBulkReply)
	if len(dumpResp.Args) != 2 {
		return protocol.MakeErrReply("illegal dump key response")
	}
	// 使用复制命令将数据复制到本节点
	resp = cluster.db.Exec(connection.NewFakeConn(), [][]byte{
		[]byte("CopyTo"), []byte(key), dumpResp.Args[0], dumpResp.Args[1],
	})
	if protocol.IsErrorReply(resp) {
		return resp.(protocol.ErrorReply)
	}
	slot.importedKeys.Add(key)
	return nil
}

// ensureKeyWithoutLock 在无锁状态下保证键在当前节点。
func (cluster *Cluster) ensureKeyWithoutLock(key string) protocol.ErrorReply {
	// 加锁保护键
	cluster.db.RWLocks(0, []string{key}, nil)
	defer cluster.db.RWUnLocks(0, []string{key}, nil)
	return cluster.ensureKey(key)
}
