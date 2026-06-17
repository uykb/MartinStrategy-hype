// Package core 实现事件驱动的发布/订阅消息总线。
//
// 安全加固说明（P0 修复）：
//   - 事件处理主循环添加 defer recover() 防护，防止单个 handler panic 导致整个总线死亡
//   - handler 执行由并发改为顺序（同步），确保 FSM 状态转移的绝对线性确定性
//   - 事件丢弃日志由 fmt.Println 替换为 zap 结构化日志 + atomic 限速（每 100 次记录一次）
//   - 新增 PriceUpdate 带时间戳的价格事件类型（定义在 exchange 包）
package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uykb/MartinStrategy/internal/utils"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// 事件类型定义
// ---------------------------------------------------------------------------

// EventType 定义事件类型
type EventType string

const (
	EventTick           EventType = "TICK"            // 价格更新（携带 *exchange.PriceUpdate）
	EventOrderUpdate    EventType = "ORDER_UPDATE"    // 订单状态变更
	EventPositionUpdate EventType = "POSITION_UPDATE" // 持仓变更
	EventLog            EventType = "LOG"             // 日志事件
	EventStart          EventType = "START"           // 启动事件
	EventStop           EventType = "STOP"            // 停止事件
	EventResyncStart    EventType = "RESYNC_START"   // 对账开始（冻结 FSM）
	EventResyncEnd      EventType = "RESYNC_END"     // 对账结束（解冻 FSM）
)

// Event 携带事件数据
type Event struct {
	Type      EventType
	Data      interface{}
	Timestamp time.Time
}

// EventHandler 处理事件的函数签名
type EventHandler func(ctx context.Context, event Event) error

// ---------------------------------------------------------------------------
// EventBus 事件总线
// ---------------------------------------------------------------------------

// EventBus 管理事件订阅与发布。
//
// 关键设计决策：
//   - handler 顺序（同步）执行，确保 FSM 状态转移的确定性
//   - 事件队列缓冲 1000，满时丢弃并限速日志
//   - 主循环 defer recover() 防护，panic 后自动恢复
type EventBus struct {
	mu           sync.RWMutex
	handlers     map[EventType][]EventHandler
	queue        chan Event
	ctx          context.Context
	cancel       context.CancelFunc
	droppedCount atomic.Int64 // 丢弃事件计数器（限速日志用）
}

// NewEventBus 创建事件总线实例
func NewEventBus() *EventBus {
	ctx, cancel := context.WithCancel(context.Background())
	return &EventBus{
		handlers: make(map[EventType][]EventHandler),
		queue:    make(chan Event, 1000),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Subscribe 注册事件处理器
func (eb *EventBus) Subscribe(eventType EventType, handler EventHandler) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.handlers[eventType] = append(eb.handlers[eventType], handler)
}

// Publish 发布事件到队列。
// 如果队列已满，事件被丢弃并限速记录日志（每 100 次丢弃记录一次）。
func (eb *EventBus) Publish(eventType EventType, data interface{}) {
	select {
	case eb.queue <- Event{Type: eventType, Data: data, Timestamp: time.Now()}:
	default:
		// 队列满，丢弃事件
		dropped := eb.droppedCount.Add(1)
		if dropped%100 == 1 {
			// 限速日志：每 100 次丢弃记录一次
			utils.Logger.Warn("事件队列已满，丢弃事件",
				zap.String("type", string(eventType)),
				zap.Int64("total_dropped", dropped))
		}
	}
}

// Start 启动事件处理主循环。
// ★ P0 加固：添加 defer recover() 防护，防止单个 handler panic 导致整个总线死亡。
func (eb *EventBus) Start() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				utils.Logger.Error("EventBus 处理循环 panic 恢复，正在重启",
					zap.Any("recover", r),
					zap.Stack("stack"))
				// 自愈：5 秒后重启事件循环
				time.Sleep(5 * time.Second)
				eb.Start()
			}
		}()

		for {
			select {
			case <-eb.ctx.Done():
				return
			case event := <-eb.queue:
				eb.process(event)
			}
		}
	}()
}

// Stop 停止事件总线
func (eb *EventBus) Stop() {
	eb.cancel()
}

// process 处理单个事件。
// ★ P0 加固：handler 顺序（同步）执行，确保 FSM 状态转移的确定性。
// 不再使用 go func() 并发执行，避免数据竞争。
func (eb *EventBus) process(event Event) {
	eb.mu.RLock()
	handlers := eb.handlers[event.Type]
	eb.mu.RUnlock()

	for _, handler := range handlers {
		// 顺序执行每个 handler，确保状态转移的线性确定性
		// 单个 handler 的 panic 不会影响其他 handler 和主循环
		func() {
			defer func() {
				if r := recover(); r != nil {
					utils.Logger.Error("事件处理器 panic 恢复",
						zap.String("type", string(event.Type)),
						zap.Any("recover", r),
						zap.Stack("stack"))
				}
			}()
			if err := handler(eb.ctx, event); err != nil {
				utils.Logger.Error("事件处理器返回错误",
					zap.String("type", string(event.Type)),
					zap.Error(err))
			}
		}()
	}
}