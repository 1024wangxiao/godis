package cluster

import (
	"github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
)

// isImportedKey 检查指定的键是否已经被导入到当前节点
func (cluster *Cluster) isImportedKey(key string) bool {
	slotId := getSlot(key) // 计算键对应的槽ID
	cluster.slotMu.RLock()
	slot := cluster.slots[slotId]
	cluster.slotMu.RUnlock()
	return slot.importedKeys.Has(key) // 检查键是否存在于importedKeys集合中
}

// setImportedKey 将指定键标记为已导入
func (cluster *Cluster) setImportedKey(key string) {
	slotId := getSlot(key)
	cluster.slotMu.Lock()
	slot := cluster.slots[slotId]
	cluster.slotMu.Unlock()
	slot.importedKeys.Add(key) // 添加键到importedKeys集合
}

// initSlot 初始化一个槽，通常用于作为种子节点启动或从其他节点导入槽
func (cluster *Cluster) initSlot(slotId uint32, state uint32) {
	cluster.slotMu.Lock()
	defer cluster.slotMu.Unlock()
	// 设置槽的状态和创建键集合
	cluster.slots[slotId] = &hostSlot{
		importedKeys: set.Make(),
		keys:         set.Make(),
		state:        state,
	}
}

// getHostSlot 获取指定ID的槽
func (cluster *Cluster) getHostSlot(slotId uint32) *hostSlot {
	cluster.slotMu.RLock()
	defer cluster.slotMu.RUnlock()
	return cluster.slots[slotId]
}

// finishSlotImport 完成槽的导入过程，将其状态改为host
func (cluster *Cluster) finishSlotImport(slotID uint32) {
	cluster.slotMu.Lock()
	defer cluster.slotMu.Unlock()
	slot := cluster.slots[slotID]
	slot.state = slotStateHost // 更新槽状态
	slot.importedKeys = nil    // 清除importedKeys集合
	slot.oldNodeID = ""
}

// setLocalSlotImporting 设置槽的状态为正在导入，并记录旧节点ID
func (cluster *Cluster) setLocalSlotImporting(slotID uint32, oldNodeID string) {
	cluster.slotMu.Lock()
	defer cluster.slotMu.Unlock()
	slot := cluster.slots[slotID]
	if slot == nil {
		// 如果槽不存在，则初始化一个
		slot = &hostSlot{
			importedKeys: set.Make(),
			keys:         set.Make(),
		}
		cluster.slots[slotID] = slot
	}
	slot.state = slotStateImporting
	slot.oldNodeID = oldNodeID
}

// setSlotMovingOut 设置槽的状态为正在迁出，并记录新节点ID
func (cluster *Cluster) setSlotMovingOut(slotID uint32, newNodeID string) {
	cluster.slotMu.Lock()
	defer cluster.slotMu.Unlock()
	slot := cluster.slots[slotID]
	if slot == nil {
		slot = &hostSlot{
			importedKeys: set.Make(),
			keys:         set.Make(),
		}
		cluster.slots[slotID] = slot
	}
	slot.state = slotStateMovingOut
	slot.newNodeID = newNodeID
}

// cleanDroppedSlot 清理已迁出或导入失败的槽中的键
func (cluster *Cluster) cleanDroppedSlot(slotID uint32) {
	cluster.slotMu.RLock()
	if cluster.slots[slotID] == nil {
		cluster.slotMu.RUnlock()
		return
	}
	keys := cluster.slots[slotID].importedKeys
	cluster.slotMu.RUnlock()
	c := connection.NewFakeConn()
	go func() {
		if keys != nil {
			keys.ForEach(func(key string) bool {
				cluster.db.Exec(c, utils.ToCmdLine("DEL", key))
				return true
			})
		}
	}()
}
