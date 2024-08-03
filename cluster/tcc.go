package cluster

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/redis/protocol"
)

// prepareFuncMap 存储不同命令名称到它们的准备函数的映射，用于事务的准备阶段。
var prepareFuncMap = make(map[string]CmdFunc)

// registerPrepareFunc 用于注册命令和对应的准备函数。
func registerPrepareFunc(cmdName string, fn CmdFunc) {
	prepareFuncMap[strings.ToLower(cmdName)] = fn
}

// Transaction 代表一个分布式事务。
type Transaction struct {
	id      string           // 事务 ID
	cmdLine [][]byte         // 命令行
	cluster *Cluster         // 集群实例
	conn    redis.Connection // 客户端连接
	dbIndex int              // 数据库索引

	writeKeys  []string  // 需要写入的键
	readKeys   []string  // 需要读取的键
	keysLocked bool      // 键是否已锁定
	undoLog    []CmdLine // 撤销日志

	status int8        // 事务状态
	mu     *sync.Mutex // 用于同步的互斥锁
}

const (
	maxLockTime       = 3 * time.Second // 锁定的最大时间
	waitBeforeCleanTx = 2 * maxLockTime // 清理事务前的等待时间

	createdStatus    = 0 // 创建状态
	preparedStatus   = 1 // 准备状态
	committedStatus  = 2 // 提交状态
	rolledBackStatus = 3 // 回滚状态
)

// genTaskKey 生成用于时间轮的事务键
func genTaskKey(txID string) string {
	return "tx:" + txID
}

// NewTransaction 创建一个新的分布式事务实例。
func NewTransaction(cluster *Cluster, c redis.Connection, id string, cmdLine [][]byte) *Transaction {
	return &Transaction{
		id:      id,
		cmdLine: cmdLine,
		cluster: cluster,
		conn:    c,
		dbIndex: c.GetDBIndex(),
		status:  createdStatus,
		mu:      new(sync.Mutex),
	}
}

// lockKeys 锁定事务涉及的键。
func (tx *Transaction) lockKeys() {
	if !tx.keysLocked {
		tx.cluster.db.RWLocks(tx.dbIndex, tx.writeKeys, tx.readKeys) //读写锁定一组键
		tx.keysLocked = true
	}
}

// unLockKeys 解锁事务涉及的键。
func (tx *Transaction) unLockKeys() {
	if tx.keysLocked {
		tx.cluster.db.RWUnLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = false
	}
}

// prepare 准备事务，锁定键，并确定事务是否可以继续。
func (tx *Transaction) prepare() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.writeKeys, tx.readKeys = database.GetRelatedKeys(tx.cmdLine)
	// lock writeKeys
	tx.lockKeys()

	for _, key := range tx.writeKeys {
		err := tx.cluster.ensureKey(key)
		if err != nil {
			return err
		}
	}
	for _, key := range tx.readKeys {
		err := tx.cluster.ensureKey(key)
		if err != nil {
			return err
		}
	}
	// build undoLog
	tx.undoLog = tx.cluster.db.GetUndoLogs(tx.dbIndex, tx.cmdLine)
	tx.status = preparedStatus
	taskKey := genTaskKey(tx.id)
	timewheel.Delay(maxLockTime, taskKey, func() {
		if tx.status == preparedStatus { // rollback transaction uncommitted until expire
			logger.Info("abort transaction: " + tx.id)
			tx.mu.Lock()
			defer tx.mu.Unlock()
			_ = tx.rollbackWithLock()
		}
	})
	return nil
}

// rollbackWithLock 回滚事务，并确保状态不变。
func (tx *Transaction) rollbackWithLock() error {
	curStatus := tx.status

	if tx.status != curStatus { // ensure status not changed by other goroutine
		return fmt.Errorf("tx %s status changed", tx.id)
	}
	if tx.status == rolledBackStatus { // no need to rollback a rolled-back transaction
		return nil
	}
	tx.lockKeys()
	for _, cmdLine := range tx.undoLog {
		tx.cluster.db.ExecWithLock(tx.conn, cmdLine)
	}
	tx.unLockKeys()
	tx.status = rolledBackStatus
	return nil
}

// execPrepare 处理分布式事务的准备阶段。
func execPrepare(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'prepare' command")
	}
	txID := string(cmdLine[1])
	cmdName := strings.ToLower(string(cmdLine[2]))
	tx := NewTransaction(cluster, c, txID, cmdLine[2:])
	cluster.transactionMu.Lock()
	cluster.transactions.Put(txID, tx)
	cluster.transactionMu.Unlock()
	err := tx.prepare()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	prepareFunc, ok := prepareFuncMap[cmdName]
	if ok {
		return prepareFunc(cluster, c, cmdLine[2:])
	}
	return &protocol.OkReply{}
}

// execRollback 处理事务的回滚。
func execRollback(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rollback' command")
	}
	txID := string(cmdLine[1])
	cluster.transactionMu.RLock()
	raw, ok := cluster.transactions.Get(txID)
	cluster.transactionMu.RUnlock()
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	tx.mu.Lock()
	defer tx.mu.Unlock()
	err := tx.rollbackWithLock()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	// clean transaction
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactionMu.Lock()
		cluster.transactions.Remove(tx.id)
		cluster.transactionMu.Unlock()
	})
	return protocol.MakeIntReply(1)
}

// execCommit 处理事务的提交。
func execCommit(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'commit' command")
	}
	txID := string(cmdLine[1])
	cluster.transactionMu.RLock()
	raw, ok := cluster.transactions.Get(txID)
	cluster.transactionMu.RUnlock()
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	tx.mu.Lock()
	defer tx.mu.Unlock()

	result := cluster.db.ExecWithLock(c, tx.cmdLine)

	if protocol.IsErrorReply(result) {
		// failed
		err2 := tx.rollbackWithLock()
		return protocol.MakeErrReply(fmt.Sprintf("err occurs when rollback: %v, origin err: %s", err2, result))
	}
	// after committed
	tx.unLockKeys()
	tx.status = committedStatus
	// clean finished transaction
	// do not clean immediately, in case rollback
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactionMu.Lock()
		cluster.transactions.Remove(tx.id)
		cluster.transactionMu.Unlock()
	})
	return result
}

// requestCommit 作为协调者请求所有节点提交事务。
func requestCommit(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) ([]redis.Reply, protocol.ErrorReply) {
	var errReply protocol.ErrorReply
	txIDStr := strconv.FormatInt(txID, 10)
	respList := make([]redis.Reply, 0, len(groupMap))
	for node := range groupMap {
		resp := cluster.relay(node, c, makeArgs("commit", txIDStr))
		if protocol.IsErrorReply(resp) {
			errReply = resp.(protocol.ErrorReply)
			break
		}
		respList = append(respList, resp)
	}
	if errReply != nil {
		requestRollback(cluster, c, txID, groupMap)
		return nil, errReply
	}
	return respList, nil
}

// requestRollback 作为协调者请求所有节点回滚事务。
func requestRollback(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) {
	txIDStr := strconv.FormatInt(txID, 10)
	for node := range groupMap {
		cluster.relay(node, c, makeArgs("rollback", txIDStr))
	}
}
