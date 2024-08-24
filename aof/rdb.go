package aof

import (
	"os"
	"strconv"
	"time"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	List "github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/datastruct/set"
	SortedSet "github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	rdb "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
)

// todo: forbid concurrent rewrite

// GenerateRDB 从AOF文件生成RDB文件
func (persister *Persister) GenerateRDB(rdbFilename string) error {
	ctx, err := persister.startGenerateRDB(nil, nil)
	if err != nil {
		return err
	}
	err = persister.generateRDB(ctx)
	if err != nil {
		return err
	}
	err = ctx.tmpFile.Close() // 关闭临时文件
	if err != nil {
		return err
	}
	err = os.Rename(ctx.tmpFile.Name(), rdbFilename) // 重命名临时文件为目标RDB文件名
	if err != nil {
		return err
	}
	return nil
}

// GenerateRDBForReplication asynchronously generates rdb file from aof file and returns a channel to receive following data
// parameter listener would receive following updates of rdb
// parameter hook allows you to do something during aof pausing
// GenerateRDBForReplication 异步生成RDB文件，用于复制场景
func (persister *Persister) GenerateRDBForReplication(rdbFilename string, listener Listener, hook func()) error {
	ctx, err := persister.startGenerateRDB(listener, hook)
	if err != nil {
		return err
	}

	err = persister.generateRDB(ctx)
	if err != nil {
		return err
	}
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(ctx.tmpFile.Name(), rdbFilename)
	if err != nil {
		return err
	}
	return nil
}

// startGenerateRDB 启动RDB生成过程，包括暂停AOF日志写入，同步当前AOF文件等
func (persister *Persister) startGenerateRDB(newListener Listener, hook func()) (*RewriteCtx, error) {
	persister.pausingAof.Lock() // pausing aof
	defer persister.pausingAof.Unlock()

	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// 获取当前AOF文件大小
	fileInfo, _ := os.Stat(persister.aofFilename)
	filesize := fileInfo.Size()
	// 创建临时文件
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	if newListener != nil {
		persister.listeners[newListener] = struct{}{}
	}
	if hook != nil {
		hook()
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
	}, nil
}

// generateRDB generates rdb file from aof file
func (persister *Persister) generateRDB(ctx *RewriteCtx) error {
	// 初始化一个用于重写操作的处理器，用于加载并解析AOF文件
	tmpHandler := persister.newRewriteHandler()
	tmpHandler.LoadAof(int(ctx.fileSize))

	encoder := rdb.NewEncoder(ctx.tmpFile).EnableCompress()
	err := encoder.WriteHeader()
	if err != nil {
		return err
	}
	// 准备辅助信息映射表，用于RDB文件头部
	auxMap := map[string]string{
		"redis-ver":    "6.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10),
	}

	// 根据配置确定是否将AOF前导标识设置为1
	if config.Properties.AofUseRdbPreamble {
		auxMap["aof-preamble"] = "1"
	}
	// 遍历辅助信息映射表，并写入到RDB文件中
	for k, v := range auxMap {
		err := encoder.WriteAux(k, v)
		if err != nil {
			return err
		}
	}
	// 遍历数据库，根据数据库索引i进行处理
	for i := 0; i < config.Properties.Databases; i++ {
		// 获取数据库的键和TTL（过期时间）计数
		keyCount, ttlCount := tmpHandler.db.GetDBSize(i)
		if keyCount == 0 {
			continue // 如果数据库为空，则跳过此数据库
		}
		// 写入数据库头信息
		err = encoder.WriteDBHeader(uint(i), uint64(keyCount), uint64(ttlCount))
		if err != nil {
			return err
		}
		// 对数据库中的每个键进行遍历处理
		var err2 error
		tmpHandler.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			var opts []interface{}
			// 如果存在过期时间，则添加TTL选项
			if expiration != nil {
				opts = append(opts, rdb.WithTTL(uint64(expiration.UnixNano()/1e6)))
			}
			// 根据数据类型使用相应的RDB编码方法
			switch obj := entity.Data.(type) {
			case []byte: // 字符串类型
				err = encoder.WriteStringObject(key, obj, opts...)
			case List.List: // 列表类型
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(i int, v interface{}) bool {
					bytes, _ := v.([]byte)
					vals = append(vals, bytes)
					return true
				})
				err = encoder.WriteListObject(key, vals, opts...)
			case *set.Set: // 集合类型
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(m string) bool {
					vals = append(vals, []byte(m))
					return true
				})
				err = encoder.WriteSetObject(key, vals, opts...)
			case dict.Dict: // 哈希表类型
				hash := make(map[string][]byte)
				obj.ForEach(func(key string, val interface{}) bool {
					bytes, _ := val.([]byte)
					hash[key] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, hash, opts...)
			case *SortedSet.SortedSet: // 有序集合类型
				var entries []*model.ZSetEntry
				obj.ForEachByRank(int64(0), obj.Len(), true, func(element *SortedSet.Element) bool {
					entries = append(entries, &model.ZSetEntry{
						Member: element.Member,
						Score:  element.Score,
					})
					return true
				})
				err = encoder.WriteZSetObject(key, entries, opts...)
			}
			if err != nil {
				err2 = err
				return false
			}
			return true
		})
		if err2 != nil {
			return err2
		}
	}
	// 结束写入，将所有数据写入RDB文件
	err = encoder.WriteEnd()
	if err != nil {
		return err
	}
	return nil
}
