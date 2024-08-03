package client

import (
	"errors"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/wait"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

// 实现 pipeline 模式的 redis 客户端
const (
	created = iota
	running
	closed
)

// 客户端结构定义，使用pipeline模式
type Client struct {
	conn        net.Conn        // TCP连接
	pendingReqs chan *request   // 等待发送的请求队列
	waitingReqs chan *request   // 等待响应的请求队列
	ticker      *time.Ticker    // 定时器，用于定期发送心跳
	addr        string          // Redis服务器地址
	status      int32           // 客户端状态
	working     *sync.WaitGroup // 用于跟踪未完成请求的同步等待组
}

// 请求结构定义
type request struct {
	id        uint64      // 请求id
	args      [][]byte    // 请求参数
	reply     redis.Reply // 响应
	heartbeat bool        // 是否为心跳请求
	waiting   *wait.Wait  // 等待响应的同步机制
	err       error
}

const (
	chanSize = 256             // 通道缓冲区大小
	maxWait  = 3 * time.Second // 最长等待时间
)

// 初始化客户端，建立TCP连接并返回客户端实例
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr) // 建立与Redis服务器的连接
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize), // 初始化请求队列
		waitingReqs: make(chan *request, chanSize), // 初始化响应队列
		working:     &sync.WaitGroup{},             // 初始化同步等待组
	}, nil
}

// 启动客户端，初始化处理协程
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second) // 每10秒发送一次心跳
	go client.handleWrite()                          // 启动写处理协程
	go client.handleRead()                           // 启动读处理协程
	go client.heartbeat()                            // 启动心跳检测协程
	atomic.StoreInt32(&client.status, running)       // 设置客户端状态为运行中
}

// 关闭客户端，停止协程和连接
func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed)
	client.ticker.Stop()      // 停止定时器
	close(client.pendingReqs) // 关闭请求通道
	client.working.Wait()     // 等待所有未完成的请求处理完毕

	// clean
	_ = client.conn.Close()   // 关闭连接
	close(client.waitingReqs) // 关闭响应通道
}

// 自动重连机制，尝试重新连接Redis服务器
func (client *Client) reconnect() {
	logger.Info("reconnect with: " + client.addr) // 记录重连日志
	_ = client.conn.Close()                       // 忽略可能的重复关闭错误

	var conn net.Conn
	for i := 0; i < 3; i++ { // 尝试三次重新连接
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("reconnect error: " + err.Error()) // 记录重连错误日志
			time.Sleep(time.Second)                         // 等待1秒后重试
			continue
		} else {
			break
		}
	}
	if conn == nil { // 达到最大重试次数，关闭客户端
		client.Close()
		return
	}
	client.conn = conn

	close(client.waitingReqs) // 关闭旧的响应通道
	for req := range client.waitingReqs {
		req.err = errors.New("connection closed") // 对于所有等待响应的请求，标记为连接关闭错误
		req.waiting.Done()
	}
	client.waitingReqs = make(chan *request, chanSize) // 重新初始化响应通道
	go client.handleRead()                             // 重新启动读处理协程
}

// 定时发送心跳检测，保持连接活跃
func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

// 处理写操作，发送请求数据到服务器
func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

// 发送请求到Redis服务器并等待响应
func (client *Client) Send(args [][]byte) redis.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return protocol.MakeErrReply("client closed")
	}
	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- req // 将请求添加到待发送队列
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrReply("server time out")
	}
	if req.err != nil {
		return protocol.MakeErrReply("request failed " + req.err.Error())
	}
	return req.reply
}

func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")}, // PING命令用于心跳检测
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- request            // 将心跳请求添加到待发送队列
	request.waiting.WaitWithTimeout(maxWait) // 等待心跳响应
}
func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	re := protocol.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	var err error
	for i := 0; i < 3; i++ { // 重试最多3次
		_, err = client.conn.Write(bytes)
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") && // 只重试超时错误
				!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}
	if err == nil {
		client.waitingReqs <- req // 请求成功，添加到等待响应队列
	} else {
		req.err = err
		req.waiting.Done()
	}
}

// 完成请求，设置响应并标记完成
func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	request := <-client.waitingReqs // 获取等待响应的请求
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done() // 标记请求完成
	}
}

// 读取响应数据
func (client *Client) handleRead() {
	ch := parser.ParseStream(client.conn) // 解析响应数据流
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			client.reconnect()
			return
		}
		client.finishRequest(payload.Data) // 完成请求，设置响应数据
	}
}
