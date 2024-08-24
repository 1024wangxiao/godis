package timewheel

import (
	"container/list"
	"time"

	"github.com/hdt3213/godis/lib/logger"
)

// 实现了一个名为 TimeWheel 的时间轮定时器，用于高效地处理定时任务
type location struct {
	slot  int
	etask *list.Element
}

// TimeWheel 时间轮，用于执行等待指定持续时间后的任务
type TimeWheel struct {
	interval time.Duration // 时间轮的时间间隔
	ticker   *time.Ticker  // 定时器
	slots    []*list.List  // 时间槽，存储任务的链表

	timer             map[string]*location // 任务映射表，便于快速查找和删除任务
	currentPos        int                  // 当前时间槽的位置
	slotNum           int                  // 时间槽的数量
	addTaskChannel    chan task            // 添加任务的通道
	removeTaskChannel chan string          // 删除任务的通道
	stopChannel       chan bool            // 停止时间轮的通道
}

// task 代表一个定时任务
type task struct {
	delay  time.Duration // 延迟时间
	circle int           // 周期数，表示任务需要跳过的时间轮转数
	key    string
	job    func() // 实际执行的任务函数
}

// New 创建一个新的时间轮
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	// 初始化时间轮
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]*location),
		currentPos:        0,
		slotNum:           slotNum,
		addTaskChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	tw.initSlots() // 初始化时间槽

	return tw
}

// initSlots 初始化时间槽，每个槽都是一个新的链表
func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// Start 启动时间轮
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop 停止时间轮
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob 添加新任务到队列
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{delay: delay, key: key, job: job}
}

// RemoveJob 从队列中移除任务
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

// start 开始时间轮的循环处理
func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler() // 处理时间滴答
		case task := <-tw.addTaskChannel:
			tw.addTask(&task) // 添加任务
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key) // 删除任务
		case <-tw.stopChannel:
			tw.ticker.Stop() // 停止定时器
			return
		}
	}
}

// tickHandler 处理每一个时间间隔的逻辑
func (tw *TimeWheel) tickHandler() {
	l := tw.slots[tw.currentPos] // 获取当前槽的任务链表
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0 // 如果是最后一个槽，回到起始位置
	} else {
		tw.currentPos++ // 否则移动到下一个槽
	}
	go tw.scanAndRunTask(l) // 执行并移除已到期的任务
}

// scanAndRunTask 执行和移除链表中到期的任务
func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	for e := l.Front(); e != nil; {
		task := e.Value.(*task)
		if task.circle > 0 {
			task.circle-- // 如果周期数还未到，减少周期数
			e = e.Next()
			continue
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			job := task.job
			job()
		}()
		next := e.Next()
		l.Remove(e)
		if task.key != "" {
			delete(tw.timer, task.key)
		}
		e = next
	}
}

// addTask 添加任务到正确的槽和计算周期
func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	e := tw.slots[pos].PushBack(task)
	loc := &location{
		slot:  pos,
		etask: e,
	}
	if task.key != "" {
		_, ok := tw.timer[task.key]
		if ok {
			tw.removeTask(task.key) // 如果任务已存在，先删除
		}
	}
	tw.timer[task.key] = loc // 更新映射表
}

// getPositionAndCircle 计算任务应该放入的槽和周期数
func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum

	return
}

// removeTask 从时间轮中移除指定的任务
func (tw *TimeWheel) removeTask(key string) {
	pos, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[pos.slot]
	l.Remove(pos.etask)
	delete(tw.timer, key)
}
