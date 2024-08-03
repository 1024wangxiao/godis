package pool

import (
	"errors"
	"sync"
)

var (
	ErrClosed = errors.New("pool closed")                // 池已关闭错误
	ErrMax    = errors.New("reach max connection limit") // 达到最大连接数错误
)

// request 是用于等待空闲对象的请求通道类型
type request chan interface{}

// Config 存储池配置
type Config struct {
	MaxIdle   uint // 最大空闲对象数
	MaxActive uint // 最大活跃（正在使用中）对象数
}

// Pool 用于存储和重用对象，例如Redis连接
type Pool struct {
	Config
	factory     func() (interface{}, error) // 对象创建函数
	finalizer   func(x interface{})         // 对象销毁函数
	idles       chan interface{}            // 空闲对象通道
	waitingReqs []request                   // 等待对象的请求队列
	activeCount uint                        // 当前活跃对象数
	mu          sync.Mutex                  // 保护共享资源的互斥锁
	closed      bool                        // 池是否已关闭
}

// New 创建一个新的池
func New(factory func() (interface{}, error), finalizer func(x interface{}), cfg Config) *Pool {
	return &Pool{
		factory:     factory,                             // 设置对象创建函数
		finalizer:   finalizer,                           // 设置对象销毁函数
		idles:       make(chan interface{}, cfg.MaxIdle), // 初始化空闲对象通道
		waitingReqs: make([]request, 0),                  // 初始化等待队列
		Config:      cfg,
	}
}

// getOnNoIdle 处理没有空闲对象时的逻辑
func (pool *Pool) getOnNoIdle() (interface{}, error) {
	if pool.activeCount >= pool.MaxActive {
		// waiting for connection being returned
		req := make(chan interface{}, 1)
		pool.waitingReqs = append(pool.waitingReqs, req)
		pool.mu.Unlock()
		x, ok := <-req
		if !ok {
			return nil, ErrMax
		}
		return x, nil
	}

	// 创建新的对象
	pool.activeCount++ // 预留位置
	pool.mu.Unlock()
	x, err := pool.factory()
	if err != nil {
		// 创建失败，释放预留位置
		pool.mu.Lock()
		pool.activeCount-- // release the holding place
		pool.mu.Unlock()
		return nil, err
	}
	return x, nil
}

// Get 获取一个对象
func (pool *Pool) Get() (interface{}, error) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrClosed
	}
	// 尝试从空闲对象通道获取对象
	select {
	case item := <-pool.idles:
		pool.mu.Unlock()
		return item, nil
	default:
		// 没有空闲对象，尝试创建或等待
		return pool.getOnNoIdle()
	}
}

// Put 返回一个对象到池中
func (pool *Pool) Put(x interface{}) {
	pool.mu.Lock()

	if pool.closed {
		// 如果池已关闭，直接销毁对象
		pool.mu.Unlock()
		pool.finalizer(x)
		return
	}
	// 尝试将对象传递给等待中的请求
	if len(pool.waitingReqs) > 0 {
		req := pool.waitingReqs[0]
		copy(pool.waitingReqs, pool.waitingReqs[1:])
		pool.waitingReqs = pool.waitingReqs[:len(pool.waitingReqs)-1]
		req <- x
		pool.mu.Unlock()
		return
	}
	// 尝试放回空闲队列
	select {
	case pool.idles <- x:
		pool.mu.Unlock()
		return
	default:
		// 空闲队列已满，销毁多余对象
		pool.mu.Unlock()
		pool.activeCount--
		pool.finalizer(x)
	}
}

// Close 关闭池并清理所有对象
func (pool *Pool) Close() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true
	close(pool.idles)
	pool.mu.Unlock()

	for x := range pool.idles {
		pool.finalizer(x)
	}
}
