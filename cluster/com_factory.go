package cluster

import (
	"errors"
	"fmt"
	"net"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/pool"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/client"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

// 主要实现了一个Redis客户端工厂，用于管理与Redis集群节点的连接

// defaultClientFactory结构体，维护一个节点连接的字典。
type defaultClientFactory struct {
	nodeConnections dict.Dict // map[string]*pool.Pool
}

// 连接池的配置。
var connectionPoolConfig = pool.Config{
	MaxIdle:   1,  // 最大空闲连接数
	MaxActive: 16, // 最大活跃连接数
}

// GetPeerClient 从池中获取与集群节点的客户端连接。
func (factory *defaultClientFactory) GetPeerClient(peerAddr string) (peerClient, error) {
	var connectionPool *pool.Pool
	raw, ok := factory.nodeConnections.Get(peerAddr)
	if !ok {
		// 如果不存在，创建新的连接池
		creator := func() (interface{}, error) {
			c, err := client.MakeClient(peerAddr)
			if err != nil {
				return nil, err
			}
			c.Start()
			// 验证密码
			if config.Properties.RequirePass != "" {
				authResp := c.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
				if !protocol.IsOKReply(authResp) {
					return nil, fmt.Errorf("auth failed, resp: %s", string(authResp.ToBytes()))
				}
			}
			return c, nil
		}
		// 连接关闭时的清理函数
		finalizer := func(x interface{}) {
			logger.Debug("destroy client")
			cli, ok := x.(client.Client)
			if !ok {
				return
			}
			cli.Close()
		}
		connectionPool = pool.New(creator, finalizer, connectionPoolConfig)
		factory.nodeConnections.Put(peerAddr, connectionPool)
	} else {
		connectionPool = raw.(*pool.Pool)
	}
	raw, err := connectionPool.Get()
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection pool make wrong type")
	}
	return conn, nil
}

// ReturnPeerClient 将使用完的客户端连接返回到池中。
func (factory *defaultClientFactory) ReturnPeerClient(peer string, peerClient peerClient) error {
	raw, ok := factory.nodeConnections.Get(peer)
	if !ok {
		return errors.New("connection pool not found")
	}
	raw.(*pool.Pool).Put(peerClient)
	return nil
}

type tcpStream struct {
	conn net.Conn
	ch   <-chan *parser.Payload
}

func (s *tcpStream) Stream() <-chan *parser.Payload {
	return s.ch
}

func (s *tcpStream) Close() error {
	return s.conn.Close()
}

func (factory *defaultClientFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	// todo: reuse connection
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect with %s failed: %v", peerAddr, err)
	}
	ch := parser.ParseStream(conn)
	send2node := func(cmdLine CmdLine) redis.Reply {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		resp := <-ch
		if resp.Err != nil {
			return protocol.MakeErrReply(resp.Err.Error())
		}
		return resp.Data
	}
	if config.Properties.RequirePass != "" {
		authResp := send2node(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
		if !protocol.IsOKReply(authResp) {
			return nil, fmt.Errorf("auth failed, resp: %s", string(authResp.ToBytes()))
		}
	}
	req := protocol.MakeMultiBulkReply(cmdLine)
	_, err = conn.Write(req.ToBytes())
	if err != nil {
		return nil, protocol.MakeErrReply("send cmdLine failed: " + err.Error())
	}
	return &tcpStream{
		conn: conn,
		ch:   ch,
	}, nil
}

// newDefaultClientFactory 创建一个新的defaultClientFactory实例。
func newDefaultClientFactory() *defaultClientFactory {
	return &defaultClientFactory{
		nodeConnections: dict.MakeConcurrent(1),
	}
}

// Close 关闭所有连接池。
func (factory *defaultClientFactory) Close() error {
	factory.nodeConnections.ForEach(func(key string, val interface{}) bool {
		val.(*pool.Pool).Close()
		return true
	})
	return nil
}
