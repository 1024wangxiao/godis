package cluster

import (
	"github.com/hdt3213/godis/interface/redis"
)

// ping 执行ping命令，在当前节点的数据库上执行
func ping(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	return cluster.db.Exec(c, cmdLine)
}

// info 执行info命令，在当前节点的数据库上执行
func info(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	return cluster.db.Exec(c, cmdLine)
}

// randomkey 执行randomkey命令，在当前节点的数据库上执行
func randomkey(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	return cluster.db.Exec(c, cmdLine)
}

/*----- utils -------*/
// makeArgs 构造命令行参数
func makeArgs(cmd string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(cmd)
	for i, arg := range args {
		result[i+1] = []byte(arg)
	}
	return result
}

// groupBy 根据键对命令进行分组，返回每个节点应处理的键的集合
func (cluster *Cluster) groupBy(keys []string) map[string][]string {
	result := make(map[string][]string)
	for _, key := range keys {
		peer := cluster.pickNodeAddrByKey(key) //找到节点地址
		group, ok := result[peer]
		if !ok {
			group = make([]string, 0)
		}
		group = append(group, key)
		result[peer] = group
	}
	return result
}

// pickNode 根据槽ID选择节点，考虑槽可能正在迁移的情况
func (cluster *Cluster) pickNode(slotID uint32) *Node {
	// check cluster.slot to avoid errors caused by inconsistent status on follower nodes during raft commits
	// see cluster.reBalance()
	hSlot := cluster.getHostSlot(slotID)
	if hSlot != nil {
		switch hSlot.state {
		case slotStateMovingOut:
			return cluster.topology.GetNode(hSlot.newNodeID)
		case slotStateImporting, slotStateHost:
			return cluster.topology.GetNode(cluster.self) // 如果槽正在导入或已是主机状态，返回当前节点
		}
	}

	slot := cluster.topology.GetSlots()[int(slotID)]
	node := cluster.topology.GetNode(slot.NodeID)
	return node
}

// pickNodeAddrByKey 根据键找到相应的节点地址,先找到key所在的槽，再调用pickNode找到节点然后返回地址。
func (cluster *Cluster) pickNodeAddrByKey(key string) string {
	slotId := getSlot(key)
	return cluster.pickNode(slotId).Addr
}

// modifyCmd 修改命令行的命令部分
func modifyCmd(cmdLine CmdLine, newCmd string) CmdLine {
	var cmdLine2 CmdLine
	cmdLine2 = append(cmdLine2, cmdLine...)
	cmdLine2[0] = []byte(newCmd)
	return cmdLine2
}
