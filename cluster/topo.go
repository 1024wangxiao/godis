package cluster

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

// startAsSeed 初始化集群的种子节点，设置所有槽的初始状态
func (cluster *Cluster) startAsSeed(listenAddr string) protocol.ErrorReply {
	err := cluster.topology.StartAsSeed(listenAddr) // 启动种子节点
	if err != nil {
		return err
	}
	for i := 0; i < slotCount; i++ {
		cluster.initSlot(uint32(i), slotStateHost) // 初始化所有槽
	}
	return nil
}

// Join 将当前节点加入到已存在的集群中
func (cluster *Cluster) Join(seed string) protocol.ErrorReply {
	err := cluster.topology.Join(seed) // 发送加入请求到集群的种子节点
	if err != nil {
		return nil
	}
	// 异步启动槽迁移
	go func() {
		time.Sleep(time.Second) // 延迟确保集群启动完成
		cluster.reBalance()     // 重新平衡槽分配
	}()
	return nil
}

var errConfigFileNotExist = protocol.MakeErrReply("cluster config file not exist")

// LoadConfig 加载集群配置文件并尝试重新加入集群
func (cluster *Cluster) LoadConfig() protocol.ErrorReply {
	err := cluster.topology.LoadConfigFile()
	if err != nil {
		return err
	}
	selfNodeId := cluster.topology.GetSelfNodeID()
	selfNode := cluster.topology.GetNode(selfNodeId)
	if selfNode == nil {
		return protocol.MakeErrReply("ERR self node info not found")
	}
	for _, slot := range selfNode.Slots {
		cluster.initSlot(slot.ID, slotStateHost) // 重新初始化节点的槽
	}
	return nil
}

// reBalance 重新平衡集群中的槽
func (cluster *Cluster) reBalance() {
	nodes := cluster.topology.GetNodes()
	var slotIDs []uint32
	var slots []*Slot
	reqDonateCmdLine := utils.ToCmdLine("gcluster", "request-donate", cluster.self)
	for _, node := range nodes {
		if node.ID == cluster.self {
			continue
		}
		node := node
		peerCli, err := cluster.clientFactory.GetPeerClient(node.Addr)
		if err != nil {
			logger.Errorf("get client of %s failed: %v", node.Addr, err)
			continue
		}
		resp := peerCli.Send(reqDonateCmdLine) // 请求其他节点捐赠槽
		payload, ok := resp.(*protocol.MultiBulkReply)
		if !ok {
			logger.Errorf("request donate to %s failed: %v", node.Addr, err)
			continue
		}
		for _, bin := range payload.Args {
			slotID64, err := strconv.ParseUint(string(bin), 10, 64)
			if err != nil {
				continue
			}
			slotID := uint32(slotID64)
			slotIDs = append(slotIDs, slotID)
			slots = append(slots, &Slot{
				ID:     slotID,
				NodeID: node.ID,
			})
			// Raft cannot guarantee the simultaneity and order of submissions to the source and destination nodes
			// In some cases the source node thinks the slot belongs to the destination node, and the destination node thinks the slot belongs to the source node
			// To avoid it, the source node and the destination node must reach a consensus  before propose to raft
			cluster.setLocalSlotImporting(slotID, node.ID)
		}
	}
	if len(slots) == 0 {
		return
	}
	logger.Infof("received %d donated slots", len(slots))

	// change route
	err := cluster.topology.SetSlot(slotIDs, cluster.self)
	if err != nil {
		logger.Errorf("set slot route failed: %v", err)
		return
	}
	slotChan := make(chan *Slot, len(slots))
	for _, slot := range slots {
		slotChan <- slot
	}
	close(slotChan)
	for i := 0; i < 4; i++ {
		i := i
		go func() {
			for slot := range slotChan {
				logger.Info("start import slot ", slot.ID)
				err := cluster.importSlot(slot)
				if err != nil {
					logger.Error(fmt.Sprintf("import slot %d error: %v", slot.ID, err))
					// delete all imported keys in slot
					cluster.cleanDroppedSlot(slot.ID)
					// todo: recover route
					return
				}
				logger.Infof("finish import slot: %d, about %d slots remains", slot.ID, len(slotChan))
			}
			logger.Infof("import worker %d exited", i)
		}()
	}
}

// importSlot 执行槽的迁移操作，将槽从旧节点迁移到当前节点
func (cluster *Cluster) importSlot(slot *Slot) error {
	node := cluster.topology.GetNode(slot.NodeID)

	/* 获取迁移流 */
	migrateCmdLine := utils.ToCmdLine(
		"gcluster", "migrate", strconv.Itoa(int(slot.ID)))
	migrateStream, err := cluster.clientFactory.NewStream(node.Addr, migrateCmdLine)
	if err != nil {
		return err
	}
	defer migrateStream.Close()

	fakeConn := connection.NewFakeConn()
slotLoop:
	for proto := range migrateStream.Stream() { // 处理迁移流中的数据
		if proto.Err != nil {
			return fmt.Errorf("set slot %d error: %v", slot.ID, err)
		}
		switch reply := proto.Data.(type) {
		case *protocol.MultiBulkReply:
			// todo: handle exec error
			keys, _ := database.GetRelatedKeys(reply.Args)
			// assert len(keys) == 1
			key := keys[0]
			// key may be imported by Cluster.ensureKey or by former failed migrating try
			if !cluster.isImportedKey(key) {
				cluster.setImportedKey(key)
				_ = cluster.db.Exec(fakeConn, reply.Args)
			}
		case *protocol.StatusReply:
			if protocol.IsOKReply(reply) {
				break slotLoop
			} else {
				// todo: return slot to former host node
				msg := fmt.Sprintf("migrate slot %d error: %s", slot.ID, reply.Status)
				logger.Errorf(msg)
				return protocol.MakeErrReply(msg)
			}
		case protocol.ErrorReply:
			// todo: return slot to former host node
			msg := fmt.Sprintf("migrate slot %d error: %s", slot.ID, reply.Error())
			logger.Errorf(msg)
			return protocol.MakeErrReply(msg)
		}
	}
	cluster.finishSlotImport(slot.ID) // 完成槽的导入

	// 结束迁移模式
	peerCli, err := cluster.clientFactory.GetPeerClient(node.Addr)
	if err != nil {
		return err
	}
	defer cluster.clientFactory.ReturnPeerClient(node.Addr, peerCli)
	peerCli.Send(utils.ToCmdLine("gcluster", "migrate-done", strconv.Itoa(int(slot.ID))))
	return nil
}
