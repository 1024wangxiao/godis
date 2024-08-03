package database

import (
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// Watch 设置需要监视的键
func Watch(db *DB, conn redis.Connection, args [][]byte) redis.Reply {
	// 获取连接当前监视的键的映射
	watching := conn.GetWatching()
	// 遍历所有参数（键），将它们的当前版本号记录到监视映射中
	for _, bkey := range args {
		key := string(bkey)
		watching[key] = db.GetVersion(key)
	}
	return protocol.MakeOkReply()
}

// execGetVersion 执行获取键的版本号的命令
func execGetVersion(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	ver := db.GetVersion(key)
	return protocol.MakeIntReply(int64(ver)) // 返回键的版本号
}

// init 注册 GetVer 命令及其处理函数
func init() {
	registerCommand("GetVer", execGetVersion, readAllKeys, nil, 2, flagReadOnly)
}

// isWatchingChanged 检查被监视的键是否已被修改
func isWatchingChanged(db *DB, watching map[string]uint32) bool {
	for key, ver := range watching {
		currentVersion := db.GetVersion(key)
		if ver != currentVersion {
			return true // 如果当前版本与监视时的版本不同，返回 true
		}
	}
	return false
}

// StartMulti 开始一个多命令事务
func StartMulti(conn redis.Connection) redis.Reply {
	if conn.InMultiState() {
		return protocol.MakeErrReply("ERR MULTI calls can not be nested")
	}
	conn.SetMultiState(true)
	return protocol.MakeOkReply()
}

// EnqueueCmd 将命令行加入到事务的等待队列中
func EnqueueCmd(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		err := protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
		conn.AddTxError(err)
		return err
	}
	if cmd.prepare == nil {
		err := protocol.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
		conn.AddTxError(err)
		return err
	}
	if !validateArity(cmd.arity, cmdLine) {
		err := protocol.MakeArgNumErrReply(cmdName)
		conn.AddTxError(err)
		return err
	}
	conn.EnqueueCmd(cmdLine)
	return protocol.MakeQueuedReply()
}

// execMulti 执行所有队列中的命令
func execMulti(db *DB, conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	if len(conn.GetTxErrors()) > 0 {
		return protocol.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
	}
	cmdLines := conn.GetQueuedCmdLine()
	return db.ExecMulti(conn, conn.GetWatching(), cmdLines)
}

// ExecMulti 实际执行多命令事务，保证原子性和隔离性
func (db *DB) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	// 准备阶段：计算涉及的读写键
	writeKeys := make([]string, 0) // may contains duplicate
	readKeys := make([]string, 0)
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		cmd := cmdTable[cmdName]
		prepare := cmd.prepare
		write, read := prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}
	// 加锁监视和读取的键
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	readKeys = append(readKeys, watchingKeys...)
	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)
	// 如果监视的键被修改，中止事务
	if isWatchingChanged(db, watching) { // watching keys changed, abort
		return protocol.MakeEmptyMultiBulkReply()
	}
	// 执行阶段：逐条执行命令
	results := make([]redis.Reply, 0, len(cmdLines))
	aborted := false
	undoCmdLines := make([][]CmdLine, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		undoCmdLines = append(undoCmdLines, db.GetUndoLogs(cmdLine))
		result := db.execWithLock(cmdLine)
		if protocol.IsErrorReply(result) {
			aborted = true
			// don't rollback failed commands
			undoCmdLines = undoCmdLines[:len(undoCmdLines)-1]
			break
		}
		results = append(results, result)
	}
	if !aborted { // 如果成功，提交事务
		db.addVersion(writeKeys...)
		return protocol.MakeMultiRawReply(results)
	}
	// 如果执行中断，执行回滚
	size := len(undoCmdLines)
	for i := size - 1; i >= 0; i-- {
		curCmdLines := undoCmdLines[i]
		if len(curCmdLines) == 0 {
			continue
		}
		for _, cmdLine := range curCmdLines {
			db.execWithLock(cmdLine)
		}
	}
	return protocol.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
}

// DiscardMulti 丢弃所有队列中的命令
func DiscardMulti(conn redis.Connection) redis.Reply {
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR DISCARD without MULTI")
	}
	conn.ClearQueuedCmds()
	conn.SetMultiState(false)
	return protocol.MakeOkReply()
}

// GetUndoLogs 获取用于回滚的命令
func (db *DB) GetUndoLogs(cmdLine [][]byte) []CmdLine {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	undo := cmd.undo
	if undo == nil {
		return nil
	}
	return undo(db, cmdLine[1:])
}

// GetRelatedKeys 分析命令行涉及的相关键
func GetRelatedKeys(cmdLine [][]byte) ([]string, []string) {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil, nil
	}
	prepare := cmd.prepare
	if prepare == nil {
		return nil, nil
	}
	return prepare(cmdLine[1:])
}
