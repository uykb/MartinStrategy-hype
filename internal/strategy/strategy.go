// Package strategy 实现马丁格尔网格策略的有限状态机 (FSM)。
//
// 安全加固说明（P0/P1 修复）：
//   - 所有常驻 goroutine 添加 defer recover() + 5秒延迟自愈重启
//   - handleTick 锁模式优化：网络 I/O 移出锁外
//   - updateTP 锁模式优化：fetchATR 网络请求移出 RLock
//   - 引入 PriceUpdate 时间戳防滑点：丢弃超过 2 秒的过期行情
//   - 添加 Stop() 方法 + context 取消，支持优雅关闭
//   - FSM 状态转移逻辑完全保留，未做任何修改
package strategy

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uykb/MartinStrategy/internal/config"
	"github.com/uykb/MartinStrategy/internal/core"
	"github.com/uykb/MartinStrategy/internal/exchange"
	"github.com/uykb/MartinStrategy/internal/storage"
	"github.com/uykb/MartinStrategy/internal/utils"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// FSM 状态定义（完全保留，未修改）
// ---------------------------------------------------------------------------

// State 定义 FSM 状态
type State string

const (
	StateIdle        State = "IDLE"         // 等待入场
	StateInPosition  State = "IN_POSITION"  // 持仓中 + 网格已放置
	StatePlacingGrid State = "PLACING_GRID" // 入场单已下，等待成交后放置网格
	StateClosing     State = "CLOSING"      // 平仓中
)

// MinOrderValue 是最低下单金额（Hyperliquid 为 USDC，最低约 10 USDC）
const MinOrderValue = 10.0

// MaxTickStaleness 行情最大允许延迟（超过此时间的行情视为过期，丢弃不处理）
const MaxTickStaleness = 2 * time.Second

// ---------------------------------------------------------------------------
// MartingaleStrategy 核心结构
// ---------------------------------------------------------------------------

// MartingaleStrategy 马丁格尔网格策略 FSM
type MartingaleStrategy struct {
	cfg      *config.StrategyConfig
	exchange exchange.ExchangeAdapter
	storage  *storage.Database
	bus      *core.EventBus

	mu               sync.RWMutex
	currentState     State
	position         *exchange.Position
	activeOrders     map[int64]*exchange.OpenOrder
	currentTPOrderID int64

	// ★ TP 状态跟踪：用于检测仓位变化，避免无变化的冗余更新
	// lastTPQty / lastTPPrice 以 quantityPrecision 截断后的值存储，
	// 与实际下单精度一致，避免浮点精度差异误判。
	lastTPQty   float64 // 上次 TP 下单数量（精度截断后）
	lastTPPrice float64 // 上次 TP 下单价格（5 位有效数字截断后）

	// tpDirty 标志：并发场景下新的 updateTP 请求被跳过时标记 dirty，
	// 当前 updateTP 完成后检查 dirty 并重跑，确保 TP 始终与仓位一致。
	tpDirty atomic.Bool

	// 交易对精度信息
	quantityPrecision int
	pricePrecision    int
	minQty            float64
	stepSize          float64
	tickSize          float64
	szDecimals        int
	maxPriceDecimals  int

	// 防重入锁
	gridMu sync.Mutex // placeGridOrders 防并发
	tpMu   sync.Mutex // updateTP 防并发

	// waitForFillAndPlaceGrid stops when this channel is closed
	waitStopCh chan struct{}

	// 监控计数器
	gridSkipCount int64
	tpSkipCount   int64

	// 状态标志
	gridPlaced bool

	// ★ P2 加固：对账冻结标志（atomic，对账期间 FSM 暂停处理 tick 和 orderUpdate）
	frozen atomic.Bool

	// ★ P1 加固：生命周期控制
	ctx    context.Context
	cancel context.CancelFunc

	// ★ 运行时修复：初始同步完成标志。
	// Hyperliquid WS 订阅 orderUpdates 后会持续推送历史订单状态（含已成交），
	// 推送可能持续数秒且顺序错乱（如先推 SELL 再推 BUY）。
	// 时间窗口不可靠，改用标志位：syncState + 3s 延迟后才允许处理成交事件。
	initialSyncDone atomic.Bool

	// 入场单跟踪：用于等待 GTC 限价单完全成交后再放置网格。
	// entryOrderID = 0 表示无活动入场单。
	entryOrderID          int64   // 当前入场单的订单 ID
	entrySubmittedQty     float64 // 入场单提交的数量
	entryCumulativeFilled float64 // 入场单累计已成交数量
}

// NewMartingaleStrategy 创建策略实例
func NewMartingaleStrategy(cfg *config.StrategyConfig, ex exchange.ExchangeAdapter, st *storage.Database, bus *core.EventBus) *MartingaleStrategy {
	ctx, cancel := context.WithCancel(context.Background())
	return &MartingaleStrategy{
		cfg:          cfg,
		exchange:     ex,
		storage:      st,
		bus:          bus,
		currentState: StateIdle,
		activeOrders: make(map[int64]*exchange.OpenOrder),
		waitStopCh:   make(chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Stop 优雅停止策略（P1 加固：支持 context 取消）
func (s *MartingaleStrategy) Stop() {
	s.cancel()
}

// CurrentState 返回当前 FSM 状态字符串（供健康检查查询）
func (s *MartingaleStrategy) CurrentState() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return string(s.currentState)
}

// IsFrozen 返回 FSM 是否处于对账冻结状态（供健康检查查询）
func (s *MartingaleStrategy) IsFrozen() bool {
	return s.frozen.Load()
}

// ---------------------------------------------------------------------------
// 启动与监控
// ---------------------------------------------------------------------------

// Start 启动策略：初始化交易对信息 + 订阅事件 + 同步状态
func (s *MartingaleStrategy) Start() {
	// 初始化交易对精度信息
	if err := s.initSymbolInfo(); err != nil {
		utils.Logger.Fatal("初始化交易对信息失败", zap.Error(err))
	}

	// 订阅事件
	s.bus.Subscribe(core.EventTick, s.handleTick)
	s.bus.Subscribe(core.EventOrderUpdate, s.handleOrderUpdate)

	// ★ 订阅持仓更新事件（REST 对账后用实际持仓校准 TP）
	s.bus.Subscribe(core.EventPositionUpdate, s.handlePositionUpdate)

	// ★ P2 加固：订阅对账冻结/解冻事件
	s.bus.Subscribe(core.EventResyncStart, s.handleResyncStart)
	s.bus.Subscribe(core.EventResyncEnd, s.handleResyncEnd)

	// 初始状态同步
	s.syncState()

	// ★ 运行时修复：syncState 完成后 3 秒标记"初始同步完成"。
	// 给 WS 足够时间推送完所有历史事件，之后再收到的成交才是真正的新成交。
	go func() {
		time.Sleep(3 * time.Second)
		s.initialSyncDone.Store(true)
		utils.Logger.Info("初始同步完成，开始处理实时成交事件")
	}()

	// 后台协程：定期检查持仓状态
	go s.monitorPositionStatus()
}

// monitorPositionStatus 定期检查持仓状态
// ★ P0 加固：添加 defer recover() + 5秒自愈重启 + context 取消
func (s *MartingaleStrategy) monitorPositionStatus() {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("monitorPositionStatus panic 恢复",
				zap.Any("recover", r),
				zap.Stack("stack"))
			// 自愈：5 秒后重启
			go func() {
				time.Sleep(5 * time.Second)
				s.monitorPositionStatus()
			}()
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			utils.Logger.Info("monitorPositionStatus: 收到停止信号，退出")
			return
		case <-ticker.C:
			s.mu.RLock()
			state := s.currentState
			s.mu.RUnlock()

			// 仅在 IN_POSITION 状态下检查
			if state != StateInPosition {
				continue
			}

			pos, err := s.exchange.GetPosition()
			if err != nil {
				utils.Logger.Error("monitorPositionStatus: 获取持仓失败", zap.Error(err))
				continue
			}

			if math.Abs(pos.Size) == 0 {
				utils.Logger.Info("monitorPositionStatus: 持仓已清零（可能手动平仓），重置状态为 IDLE")
				s.mu.Lock()
				s.currentState = StateIdle
				s.gridPlaced = false
				s.currentTPOrderID = 0
				s.lastTPQty = 0
				s.lastTPPrice = 0
				s.position = nil
				s.activeOrders = make(map[int64]*exchange.OpenOrder)
				s.entryOrderID = 0
				s.entrySubmittedQty = 0
				s.entryCumulativeFilled = 0
				s.mu.Unlock()

				s.exchange.CancelAllOrders()
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 交易对信息初始化
// ---------------------------------------------------------------------------

// initSymbolInfo 初始化交易对精度信息
func (s *MartingaleStrategy) initSymbolInfo() error {
	info, err := s.exchange.GetSymbolInfo()
	if err != nil {
		return fmt.Errorf("获取交易对信息失败: %w", err)
	}

	s.quantityPrecision = info.QuantityPrecision
	s.pricePrecision = info.PricePrecision
	s.minQty = info.MinQty
	s.stepSize = info.StepSize
	s.tickSize = info.TickSize
	s.szDecimals = info.SzDecimals
	s.maxPriceDecimals = info.MaxPriceDecimals

	utils.Logger.Info("交易对信息初始化完成",
		zap.String("symbol", s.exchange.GetSymbol()),
		zap.Int("price_prec", s.pricePrecision),
		zap.Int("qty_prec", s.quantityPrecision),
		zap.Float64("step_size", s.stepSize),
		zap.Float64("tick_size", s.tickSize),
		zap.Float64("min_qty", s.minQty),
		zap.Int("sz_decimals", s.szDecimals),
		zap.Int("max_price_decimals", s.maxPriceDecimals),
	)
	return nil
}

// ---------------------------------------------------------------------------
// 状态同步
// ---------------------------------------------------------------------------

// syncState 初始化时同步 FSM 状态
func (s *MartingaleStrategy) syncState() {
	s.mu.Lock()

	pos, err := s.exchange.GetPosition()
	if err != nil {
		utils.Logger.Error("同步持仓状态失败", zap.Error(err))
		s.mu.Unlock()
		return
	}
	s.position = pos

	if pos != nil && math.Abs(pos.Size) > 0 {
		s.currentState = StateInPosition
		// ★ 业务逻辑：重启时有持仓，gridPlaced 始终设为 true。
		// 不检测网格完整性、不重新放置——链上已有的网格订单是唯一的真相。
		// 如果部分网格已成交（如 5/9），重启后重新放置 9 层会导致总共 14 层，
		// 杠杆过高造成爆仓风险。缺失的网格层级是可接受的（少一层保护而已）。
		s.gridPlaced = true
		utils.Logger.Info("状态同步：检测到持仓",
			zap.String("state", string(s.currentState)),
			zap.Float64("size", pos.Size),
			zap.Float64("entry_price", pos.EntryPrice))

		orders, err := s.exchange.GetOpenOrders()
		if err != nil {
			utils.Logger.Error("获取挂单列表失败", zap.Error(err))
			s.mu.Unlock()
		} else {
			hasTP := false
			gridCount := 0
			var liveTPQty float64 // 交易所端 TP 的实际委托数量
			for _, o := range orders {
				if o.Side == exchange.OrderSideBuy {
					gridCount++
				}
				if o.Side == exchange.OrderSideSell && o.Type == exchange.OrderTypeLimit {
					hasTP = true
					s.currentTPOrderID = o.OrderID
					liveTPQty = o.Quantity
					utils.Logger.Info("发现已有 TP 订单",
						zap.Int64("id", o.OrderID),
						zap.Float64("live_qty", o.Quantity))
				}
			}

			utils.Logger.Info("网格订单状态（不修改，保持链上原样）",
				zap.Int("grid_count", gridCount),
				zap.Int("max_safety_orders", s.cfg.MaxSafetyOrders))

			if !hasTP {
				utils.Logger.Warn("检测到持仓但无 TP 订单，正在恢复 TP...")
				s.mu.Unlock()
				go func() {
					defer func() {
						if r := recover(); r != nil {
							utils.Logger.Error("syncState updateTP goroutine panic", zap.Any("recover", r))
						}
					}()
					time.Sleep(100 * time.Millisecond)
					s.safeUpdateTP()
				}()
			} else {
				// ★ 修复：lastTPQty 初始化为交易所端 TP 的实际委托数量（Floor 截断），
				// 而非当前持仓量。这样若 TP 与持仓不一致（如历史浮点 bug 遗留 2.52 vs 2.53），
				// 下次 updateTP 的仓位变化检测能识别差异并触发 modify 自愈修正。
				// 原实现用持仓量初始化会让本地误认为 TP 已对齐，跳过更新，不一致无法自愈。
				s.lastTPQty = utils.FloorToDecimals(liveTPQty, s.quantityPrecision)
				utils.Logger.Info("状态已恢复，TP 订单存在",
					zap.Int("open_orders", len(orders)),
					zap.Int("grid_orders", gridCount),
					zap.Float64("live_tp_qty", liveTPQty),
					zap.Float64("initialized_lastTPQty", s.lastTPQty))
				s.mu.Unlock()
			}
		}
	} else {
		s.currentState = StateIdle
		s.gridPlaced = false
		s.currentTPOrderID = 0
		s.lastTPQty = 0
		s.lastTPPrice = 0
		s.position = nil
		s.activeOrders = make(map[int64]*exchange.OpenOrder)
		s.entryOrderID = 0
		s.entrySubmittedQty = 0
		s.entryCumulativeFilled = 0
		s.mu.Unlock()
		utils.Logger.Info("状态同步：无持仓", zap.String("state", string(s.currentState)))
	}
}

// ---------------------------------------------------------------------------
// 事件处理器（FSM 状态转移逻辑完全保留）
// ---------------------------------------------------------------------------

// handleTick 处理价格更新事件
// ★ P1 加固：PriceUpdate 时间戳防滑点 + 锁模式优化（网络 I/O 移出锁外）
// ★ P2 加固：对账冻结期间丢弃 tick，防止 FSM 在对账期间误触发状态转移
func (s *MartingaleStrategy) handleTick(ctx context.Context, event core.Event) error {
	// ★ P2 加固：对账冻结期间丢弃 tick
	if s.frozen.Load() {
		return nil
	}

	// ★ P1 加固：解析 PriceUpdate 并检查行情新鲜度
	priceUpdate, ok := event.Data.(*exchange.PriceUpdate)
	if !ok {
		return fmt.Errorf("无效的 tick 数据: 期望 *exchange.PriceUpdate, 得到 %T", event.Data)
	}

	// ★ P1 加固：丢弃过期行情（超过 2 秒的陈旧价格）
	if priceUpdate.IsStale(MaxTickStaleness) {
		utils.Logger.Debug("丢弃过期 tick",
			zap.Float64("price", priceUpdate.Price),
			zap.Int64("timestamp_ms", priceUpdate.Timestamp))
		return nil
	}

	price := priceUpdate.Price

	utils.Logger.Info("收到 Tick",
		zap.Float64("price", price),
		zap.String("state", string(s.currentState)),
		zap.Bool("gridPlaced", s.gridPlaced))

	// ★ P1 加固：锁模式优化
	// 在锁内完成状态检查和变更，在锁外执行网络 I/O
	s.mu.Lock()
	if s.currentState != StateIdle {
		s.mu.Unlock()
		return nil
	}
	utils.Logger.Info("状态为 IDLE，启动入场序列")
	s.currentState = StatePlacingGrid
	s.gridPlaced = false

	// 关闭旧的 waitForFillAndPlaceGrid，启动新的
	if s.waitStopCh != nil {
		close(s.waitStopCh)
	}
	s.waitStopCh = make(chan struct{})
	s.mu.Unlock() // ★ 在网络调用前释放锁

	// 网络请求在锁外执行
	if err := s.enterLong(price); err != nil {
		// 下单失败，恢复状态
		s.mu.Lock()
		s.currentState = StateIdle
		s.mu.Unlock()
		utils.Logger.Error("enterLong 失败，重置为 IDLE", zap.Error(err))
		return err
	}

	// 等待订单成交，然后放置网格
	go s.waitForFillAndPlaceGrid()

	return nil
}

// waitForFillAndPlaceGrid 等待入场单成交后放置网格订单
// ★ P0 加固：添加 defer recover() + 5秒自愈重启
func (s *MartingaleStrategy) waitForFillAndPlaceGrid() {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("waitForFillAndPlaceGrid panic 恢复",
				zap.Any("recover", r),
				zap.Stack("stack"))
			// 自愈：5 秒后重新检查持仓状态
			go func() {
				time.Sleep(5 * time.Second)
				s.mu.RLock()
				state := s.currentState
				s.mu.RUnlock()
				if state == StatePlacingGrid {
					// 仍在 PLACING_GRID 状态，重新尝试
					s.waitForFillAndPlaceGrid()
				}
			}()
		}
	}()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	timeout := time.After(30 * time.Second)

	for {
		select {
		case <-s.ctx.Done():
			s.cancelEntryOrder()
			return
		case <-s.waitStopCh:
			utils.Logger.Info("waitForFillAndPlaceGrid: 通过 channel 停止")
			s.cancelEntryOrder()
			return
		case <-timeout:
			utils.Logger.Warn("waitForFillAndPlaceGrid: 超时，未检测到持仓")
			s.mu.Lock()
			s.currentState = StateIdle
			s.mu.Unlock()
			s.cancelEntryOrder()
			return
		case <-ticker.C:
			s.mu.RLock()
			state := s.currentState
			s.mu.RUnlock()

			if state != StatePlacingGrid {
				utils.Logger.Info("waitForFillAndPlaceGrid: 状态已变更，中止",
					zap.String("state", string(state)))
				s.cancelEntryOrder()
				return
			}

			pos, err := s.exchange.GetPosition()
			if err != nil {
				utils.Logger.Error("获取持仓失败", zap.Error(err))
				continue
			}

			if math.Abs(pos.Size) > 0 {
				utils.Logger.Info("检测到持仓，开始放置网格订单",
					zap.Float64("size", pos.Size),
					zap.Float64("entry_price", pos.EntryPrice))
				s.mu.Lock()
				s.entryOrderID = 0
				s.entrySubmittedQty = 0
				s.entryCumulativeFilled = 0
				s.mu.Unlock()
				s.placeGridOrders()
				return
			}
		}
	}
}

// handleOrderUpdate 处理订单状态更新事件
// ★ FSM 状态转移：IN_POSITION → IDLE（逻辑完全保留）
// ★ P2 加固：对账冻结期间丢弃订单更新，防止 FSM 在对账期间误触发状态转移
func (s *MartingaleStrategy) handleOrderUpdate(ctx context.Context, event core.Event) error {
	// ★ P2 加固：对账冻结期间丢弃订单更新
	if s.frozen.Load() {
		return nil
	}

	order, ok := event.Data.(*exchange.OrderUpdate)
	if !ok {
		utils.Logger.Error("无效的订单更新数据",
			zap.String("type", fmt.Sprintf("%T", event.Data)))
		return fmt.Errorf("无效的订单更新数据: 期望 *exchange.OrderUpdate, 得到 %T", event.Data)
	}

	// 只处理配置的交易对订单
	configuredSymbol := s.exchange.GetSymbol()
	if order.Symbol != configuredSymbol {
		utils.Logger.Debug("忽略非目标交易对的订单更新",
			zap.String("order_symbol", order.Symbol),
			zap.String("configured_symbol", configuredSymbol))
		return nil
	}

	utils.Logger.Info("收到订单更新",
		zap.Int64("id", order.OrderID),
		zap.String("status", order.Status),
		zap.String("side", string(order.Side)),
		zap.String("type", string(order.Type)),
	)

	if order.Status == "FILLED" {
		// ★ 运行时修复：syncState 完成前忽略所有历史成交事件。
		// Hyperliquid WS 可能持续推送数秒历史事件且顺序错乱。
		// 仅在 initialSyncDone 后才处理成交，保证 FSM 状态不被历史事件干扰。
		if !s.initialSyncDone.Load() {
			utils.Logger.Info("初始同步未完成，忽略历史成交事件",
				zap.Int64("id", order.OrderID),
				zap.String("side", string(order.Side)))
			return nil
		}

		if order.Side == exchange.OrderSideBuy {
			utils.Logger.Info("买单成交",
				zap.String("type", string(order.Type)),
				zap.Float64("execPrice", order.ExecPrice),
				zap.Float64("qty", order.Quantity),
				zap.Int64("order_id", order.OrderID))

			// 判断是否为入场单（匹配 entryOrderID）
			isEntry := s.entryOrderID != 0 && order.OrderID == s.entryOrderID

			if isEntry {
				// 累加入场单已成交数量
				s.mu.Lock()
				s.entryCumulativeFilled += order.Quantity
				filled := s.entryCumulativeFilled
				submitted := s.entrySubmittedQty
				s.mu.Unlock()

				utils.Logger.Info("入场单成交更新",
					zap.Int64("order_id", order.OrderID),
					zap.Float64("filled_qty", order.Quantity),
					zap.Float64("cumulative", filled),
					zap.Float64("submitted", submitted),
				)

				// 只有完全成交（≥99.9%）才触发网格放置
				if filled >= submitted*0.999 {
					utils.Logger.Info("入场单完全成交，放置网格",
						zap.Float64("cumulative", filled),
						zap.Float64("submitted", submitted))
					s.mu.Lock()
					s.currentState = StateInPosition
					s.entryOrderID = 0
					s.entrySubmittedQty = 0
					s.entryCumulativeFilled = 0
					s.mu.Unlock()
					go s.safePlaceGridOrders()
				}
				return nil
			}

			s.mu.Lock()
			prevState := s.currentState
			s.mu.Unlock()

			s.mu.RLock()
			gridPlaced := s.gridPlaced
			s.mu.RUnlock()

			if prevState == StateIdle || prevState == StatePlacingGrid {
				if !gridPlaced {
					utils.Logger.Info("基础订单成交，放置网格订单", zap.Float64("execPrice", order.ExecPrice))
					s.mu.Lock()
					s.currentState = StateInPosition
					s.entryOrderID = 0
					s.entrySubmittedQty = 0
					s.entryCumulativeFilled = 0
					s.mu.Unlock()
					go s.safePlaceGridOrders()
				} else {
					utils.Logger.Info("基础订单成交但网格已放置，更新 TP", zap.Float64("execPrice", order.ExecPrice))
					s.mu.Lock()
					s.currentState = StateInPosition
					s.entryOrderID = 0
					s.entrySubmittedQty = 0
					s.entryCumulativeFilled = 0
					s.mu.Unlock()
					go s.safeUpdateTP()
				}
			} else {
				// ★ 审计修复：安全订单（加仓单）成交时，始终更新 TP。
				// gridPlaced 仅控制是否重新放置网格订单，不应阻止 TP 更新。
				// 原逻辑在 gridPlaced=false 时跳过 TP，会导致重启后不完整网格的
				// 加仓成交不更新 TP，造成 TP 数量与实际持仓不一致（残余尾仓）。
				utils.Logger.Info("安全订单成交，重新计算 TP", zap.Float64("execPrice", order.ExecPrice))
				go s.safeUpdateTP()
			}
		} else if order.Side == exchange.OrderSideSell {
			utils.Logger.Info("卖单成交 (TP/手动)。重置为 IDLE。",
				zap.String("type", string(order.Type)),
				zap.String("status", order.Status),
			)

			s.mu.Lock()
			s.currentState = StateIdle
			s.currentTPOrderID = 0
			s.gridPlaced = false
			s.lastTPQty = 0
			s.lastTPPrice = 0
			s.position = nil
			s.activeOrders = make(map[int64]*exchange.OpenOrder)
			s.entryOrderID = 0
			s.entrySubmittedQty = 0
			s.entryCumulativeFilled = 0
			utils.Logger.Info("卖单成交：状态重置为 IDLE", zap.Bool("gridPlaced", s.gridPlaced))
			s.mu.Unlock()

			s.exchange.CancelAllOrders()
			utils.Logger.Info("卖单成交后已取消所有挂单")
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// ★ P2 加固：对账冻结/解冻事件处理器
// ---------------------------------------------------------------------------

// handleResyncStart 处理对账开始事件：冻结 FSM，暂停 tick 和订单更新处理
func (s *MartingaleStrategy) handleResyncStart(ctx context.Context, event core.Event) error {
	s.frozen.Store(true)
	utils.Logger.Info("FSM 已冻结：REST 对账进行中，暂停 tick 和订单处理")
	return nil
}

// handleResyncEnd 处理对账结束事件：延迟解冻 FSM，恢复正常处理。
//
// WS 重连/重订阅后会立即推送历史订单状态，直接在 handleResyncEnd 中解冻
// 会导致这些历史事件被正常处理（如历史 SELL 重置 IDLE + 撤销网格）。
// 延迟 2 秒解冻给 WS 时间排空历史事件。
func (s *MartingaleStrategy) handleResyncEnd(ctx context.Context, event core.Event) error {
	// 延迟解冻：WS 重连后 2 秒内推送的多是历史事件，不应处理
	go func() {
		time.Sleep(2 * time.Second)
		s.frozen.Store(false)
		utils.Logger.Info("FSM 已解冻：REST 对账完成，恢复 tick 和订单处理")
	}()
	return nil
}

// handlePositionUpdate 处理持仓更新事件（由 REST 对账发布）。
//
// 当 WSManager 在重连对账后发布实际持仓时，此处理器用真实持仓校准 TP：
//   - 持仓 > 0 且 FSM 处于 IN_POSITION → 触发 safeUpdateTP（仓位变化检测会决定是否实际更新）
//   - 持仓 = 0 但 FSM 非 IDLE → 重置为 IDLE（可能手动平仓或 TP 已成交但事件丢失）
func (s *MartingaleStrategy) handlePositionUpdate(ctx context.Context, event core.Event) error {
	pos, ok := event.Data.(*exchange.Position)
	if !ok {
		utils.Logger.Error("无效的持仓更新数据",
			zap.String("type", fmt.Sprintf("%T", event.Data)))
		return fmt.Errorf("无效的持仓更新数据: 期望 *exchange.Position, 得到 %T", event.Data)
	}

	utils.Logger.Info("收到持仓更新事件",
		zap.Float64("size", pos.Size),
		zap.Float64("entry_price", pos.EntryPrice))

	s.mu.Lock()
	state := s.currentState
	s.position = pos
	s.mu.Unlock()

	if math.Abs(pos.Size) > 0 {
		// 有持仓：若处于 IN_POSITION，触发 TP 校准
		if state == StateInPosition {
			utils.Logger.Info("持仓更新：触发 TP 校准", zap.Float64("size", pos.Size))
			go s.safeUpdateTP()
		}
	} else {
		// 无持仓但 FSM 非 IDLE：重置状态
		if state != StateIdle {
			utils.Logger.Info("持仓更新：持仓为零但状态非 IDLE，重置为 IDLE",
				zap.String("prev_state", string(state)))
			s.mu.Lock()
			s.currentState = StateIdle
			s.gridPlaced = false
			s.currentTPOrderID = 0
			s.lastTPQty = 0
			s.lastTPPrice = 0
			s.position = nil
			s.activeOrders = make(map[int64]*exchange.OpenOrder)
			s.entryOrderID = 0
			s.entrySubmittedQty = 0
			s.entryCumulativeFilled = 0
			s.mu.Unlock()
			s.exchange.CancelAllOrders()
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// ★ P0 加固：带 panic 恢复的安全包装函数
// ---------------------------------------------------------------------------

// safePlaceGridOrders 是 placeGridOrders 的安全包装，带 panic 恢复和自愈。
// ★ 修复：移除 execPrice 参数（placeGridOrders 始终使用 GetPosition().EntryPrice）。
// ★ 修复：添加最大重试次数，防止 panic 恢复导致无限自愈循环。
func (s *MartingaleStrategy) safePlaceGridOrders() {
	const maxRetries = 3
	s.placeGridOrdersWithRetry(0, maxRetries)
}

// placeGridOrdersWithRetry 带重试计数的内部实现。
func (s *MartingaleStrategy) placeGridOrdersWithRetry(attempt, maxRetries int) {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("placeGridOrders panic 恢复",
				zap.Any("recover", r),
				zap.Int("attempt", attempt+1),
				zap.Stack("stack"))
			// 自愈：5 秒后重试（有最大次数限制）
			if attempt+1 < maxRetries {
				go func() {
					time.Sleep(5 * time.Second)
					s.mu.RLock()
					state := s.currentState
					gridPlaced := s.gridPlaced
					s.mu.RUnlock()
					if state == StateInPosition && !gridPlaced {
						s.placeGridOrdersWithRetry(attempt+1, maxRetries)
					}
				}()
			} else {
				utils.Logger.Error("placeGridOrders 已达最大重试次数，放弃",
					zap.Int("max_retries", maxRetries))
			}
		}
	}()
	s.placeGridOrders()
}

// safeUpdateTP 是 updateTP 的安全包装，带 panic 恢复、自愈和并发脏标志。
//
// 并发处理策略：
//   - 使用 TryLock 避免多个 goroutine 阻塞等待
//   - 若 TryLock 失败（已有 updateTP 在执行），标记 tpDirty=true 并返回
//   - 当前 updateTP 完成后检查 tpDirty，若为 true 则重跑，确保 TP 始终与仓位一致
//   - ★ 修复：dirty 循环最多重跑 3 次，防止高频成交场景下 goroutine 永不退出（liveness bug）
func (s *MartingaleStrategy) safeUpdateTP() {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("updateTP panic 恢复",
				zap.Any("recover", r),
				zap.Stack("stack"))
			// 自愈：5 秒后重试
			go func() {
				time.Sleep(5 * time.Second)
				s.mu.RLock()
				state := s.currentState
				s.mu.RUnlock()
				if state == StateInPosition {
					s.safeUpdateTP()
				}
			}()
		}
	}()

	// 尝试获取锁，失败则标记 dirty 让当前执行者完成后重跑
	if !s.tpMu.TryLock() {
		s.tpDirty.Store(true)
		s.mu.Lock()
		s.tpSkipCount++
		skipCount := s.tpSkipCount
		s.mu.Unlock()
		utils.Logger.Warn("updateTP 跳过：已在执行中，标记 dirty",
			zap.Int64("skip_count", skipCount))
		return
	}
	defer s.tpMu.Unlock()

	// 清除 dirty 后执行，执行期间若有新的请求会重新标记 dirty
	s.tpDirty.Store(false)
	s.updateTP()

	// ★ 修复：dirty 循环最多重跑 maxTPDirtyRetries 次，防止高频成交场景下 goroutine 永不退出。
	// 原实现无上限：若 safeUpdateTP 调用频率高于 updateTP 执行速度，持有锁的 goroutine
	// 会因 tpDirty 持续为 true 而永远不退出，导致后续所有 TryLock 失败、tpSkipCount 飙升。
	const maxTPDirtyRetries = 3
	for i := 0; i < maxTPDirtyRetries && s.tpDirty.Load(); i++ {
		s.tpDirty.Store(false)
		utils.Logger.Info("检测到 dirty 标志，重跑 updateTP",
			zap.Int("retry", i+1),
			zap.Int("max_retries", maxTPDirtyRetries))
		s.updateTP()
	}

	// 如果重跑后仍有 dirty（高频场景），留给下一次 safeUpdateTP 调用处理
	if s.tpDirty.Load() {
		utils.Logger.Warn("dirty 标志在重跑后仍存在，留给下次调用处理",
			zap.Int("completed_retries", maxTPDirtyRetries))
	}
}

// ---------------------------------------------------------------------------
// 策略动作
// ---------------------------------------------------------------------------

// enterLong 入场做多（GTC 限价单，挂单成交以节约手续费）。
func (s *MartingaleStrategy) enterLong(currentPrice float64) error {
	utils.Logger.Info("正在入场做多（限价挂单）...")

	minNotional := s.calcMinNotional()
	unitQtyRaw := minNotional / currentPrice
	unitQty := utils.FloorToTickSize(unitQtyRaw, s.stepSize)

	if unitQty < s.minQty {
		unitQty = s.minQty
	}

	baseQty := unitQty * 0.06
	baseQty = utils.FloorToDecimals(baseQty, s.quantityPrecision)

	if baseQty*currentPrice < minNotional {
		baseQty = utils.FloorToDecimals(baseQty+s.stepSize, s.quantityPrecision)
		utils.Logger.Info("金额不足，微调数量",
			zap.Float64("adjusted_qty", baseQty),
			zap.Float64("value", baseQty*currentPrice),
			zap.Float64("min_notional", minNotional))
	}

	// GTC 限价单：1% 溢价确保较快成交，但远低于原 5% IOC 溢价。
	limitPrice := utils.RoundToSigFigs(currentPrice*1.01, 5, s.maxPriceDecimals)

	utils.Logger.Info("计算入场下单量",
		zap.Float64("price", currentPrice),
		zap.Float64("limit_price", limitPrice),
		zap.Float64("unit_qty", unitQty),
		zap.Float64("base_qty", baseQty),
		zap.Float64("value", baseQty*currentPrice),
	)

	resp, err := s.exchange.CreateOrder(exchange.OrderSideBuy, exchange.OrderTypeLimit, baseQty, limitPrice)
	if err != nil {
		utils.Logger.Error("入场限价单下单失败", zap.Error(err))
		return err
	}

	// 跟踪入场单 ID 和期望数量，用于等待完全成交
	s.entryOrderID = resp.OrderID
	s.entrySubmittedQty = baseQty
	s.entryCumulativeFilled = 0
	utils.Logger.Info("入场限价单已提交",
		zap.Int64("order_id", s.entryOrderID),
		zap.Float64("submitted_qty", s.entrySubmittedQty),
		zap.Float64("limit_price", limitPrice),
	)

	return nil
}

// placeGridOrders 放置网格安全订单。
// ★ 修复：始终使用 GetPosition().EntryPrice 作为入场价，不依赖订单事件的 execPrice
// （execPrice 可能因手续费/滑点与链上均价不一致）。
// ★ 修复：检测不完整网格时先取消旧单再重新放置，防止重复挂单。
// ★ 修复：仅在所有订单成功下单后才设置 gridPlaced=true。
func (s *MartingaleStrategy) placeGridOrders() {
	utils.Logger.Info("placeGridOrders 开始")

	// 检查网格是否已放置，防止重复
	s.mu.RLock()
	if s.gridPlaced {
		s.mu.RUnlock()
		utils.Logger.Warn("placeGridOrders 跳过：网格已放置")
		return
	}
	s.mu.RUnlock()

	// ★ 修复：检查是否已有活跃的网格订单，验证完整性
	existingOrders, err := s.exchange.GetOpenOrders()
	if err == nil && len(existingOrders) > 0 {
		gridCount := 0
		for _, o := range existingOrders {
			if o.Side == exchange.OrderSideBuy {
				gridCount++
			}
		}
		if gridCount >= s.cfg.MaxSafetyOrders {
			utils.Logger.Info("placeGridOrders 跳过：网格订单完整",
				zap.Int("existing_grid_count", gridCount),
				zap.Int("expected", s.cfg.MaxSafetyOrders))
			s.mu.Lock()
			s.gridPlaced = true
			s.mu.Unlock()
			return
		}
		// ★ 修复：网格不完整，取消现有买单后重新放置
		if gridCount > 0 {
			utils.Logger.Warn("发现不完整网格，取消现有买单后重新放置",
				zap.Int("existing_grid_count", gridCount),
				zap.Int("expected", s.cfg.MaxSafetyOrders))
			for _, o := range existingOrders {
				if o.Side == exchange.OrderSideBuy {
					if cancelErr := s.exchange.CancelOrder(o.OrderID); cancelErr != nil {
						utils.Logger.Warn("取消不完整网格订单失败",
							zap.Int64("order_id", o.OrderID),
							zap.Error(cancelErr))
					}
				}
			}
			// 短暂等待交易所处理取消
			time.Sleep(200 * time.Millisecond)
		}
	}

	// 防并发
	if !s.gridMu.TryLock() {
		s.mu.Lock()
		s.gridSkipCount++
		skipCount := s.gridSkipCount
		s.mu.Unlock()
		utils.Logger.Warn("placeGridOrders 跳过：已在执行中",
			zap.Int64("skip_count", skipCount))
		return
	}
	defer s.gridMu.Unlock()

	// 再次检查（获取锁后）
	s.mu.RLock()
	if s.gridPlaced {
		s.mu.RUnlock()
		utils.Logger.Warn("placeGridOrders 跳过：网格已放置（获取锁后）")
		return
	}
	s.mu.RUnlock()

	// ★ 修复：始终通过 REST API 获取链上真实持仓均价，不使用订单事件的 execPrice。
	// execPrice 来自 WebSocket 成交事件，可能因手续费、滑点或部分成交与链上均价不一致。
	pos, err := s.exchange.GetPosition()
	if err != nil {
		utils.Logger.Error("获取持仓信息失败", zap.Error(err))
		return
	}
	entryPrice := pos.EntryPrice
	utils.Logger.Info("使用持仓 API 中的入场价", zap.Float64("entryPrice", entryPrice))

	if entryPrice <= 0 {
		utils.Logger.Error("无效的入场价，无法放置网格订单", zap.Float64("entryPrice", entryPrice))
		return
	}

	// 固定百分比网格间距（9 层，相对上一层的下降比例）
	// Level 1: -1.0%  Level 2: -1.0%  Level 3: -1.0%
	// Level 4: -1.1%  Level 5: -2.1%  Level 6: -2.2%
	// Level 7: -4.5%  Level 8: -4.8%  Level 9: -7.7%
	gridPercents := []float64{0.010, 0.010, 0.010, 0.011, 0.021, 0.022, 0.045, 0.048, 0.077}

	minNotional := s.calcMinNotional()
	// ★ 审计修复：数量使用 FloorToTickSize 向下取整
	unitQty := utils.FloorToTickSize(minNotional/entryPrice, s.stepSize)

	utils.Logger.Info("放置网格订单",
		zap.Float64("Entry", entryPrice),
		zap.Float64("UnitQty", unitQty))

	currentPriceLevel := entryPrice

	// ★ 修复：追踪成功下单数量，仅在全部成功时设置 gridPlaced=true
	successCount := 0

	for i := 1; i <= s.cfg.MaxSafetyOrders; i++ {
		// 获取当前层的固定百分比间距（相对上一层价格的下降比例）
		stepPct := 0.01 // 默认 1%
		if i-1 < len(gridPercents) {
			stepPct = gridPercents[i-1]
		} else {
			stepPct = gridPercents[len(gridPercents)-1]
		}

		price := currentPriceLevel * (1 - stepPct)
		currentPriceLevel = price

		// ★ Hyperliquid 5 位有效数字截断
		price = utils.RoundToSigFigs(price, 5, s.maxPriceDecimals)

		utils.Logger.Info("放置安全订单",
			zap.Int("index", i),
			zap.Float64("price", price),
			zap.Float64("pct", stepPct*100),
		)

		volMult := s.getGridMultiplier(i)
		qty := unitQty * float64(volMult)

		if qty*price < minNotional {
			utils.Logger.Info("调整数量以满足最低下单金额",
				zap.Int("index", i),
				zap.Float64("old_qty", qty),
				zap.Float64("price", price),
			)
			qty = minNotional / price
		}

		// ★ 审计修复：数量严格向下取整，防止余额不足和幽灵尾仓
		qty = utils.FloorToTickSize(qty, s.stepSize)
		qty = utils.FloorToDecimals(qty, s.quantityPrecision)

		// ★ 运行时修复：Floor 截断后金额可能略低于 MinNotional，
		// 需要向上微调一个 stepSize 确保满足交易所最低金额要求。
		if qty*price < minNotional {
			qty = utils.FloorToDecimals(qty+s.stepSize, s.quantityPrecision)
			utils.Logger.Info("Floor 截断后金额不足，微调数量",
				zap.Int("index", i),
				zap.Float64("adjusted_qty", qty),
				zap.Float64("value", qty*price),
				zap.Float64("min_notional", minNotional))
		}

		// ★ P1 加固：带重试的下单逻辑（3次重试 + 抖动退避）
		if s.placeOrderWithRetry(exchange.OrderSideBuy, exchange.OrderTypeLimit, qty, price, i) {
			successCount++
		}

		// 避免 API 限流
		time.Sleep(200 * time.Millisecond)
	}

	// ★ 修复：仅在所有订单成功下单后才标记网格已放置
	s.mu.Lock()
	if successCount == s.cfg.MaxSafetyOrders {
		s.gridPlaced = true
		utils.Logger.Info("网格订单放置完成，gridPlaced=true",
			zap.Int("success_count", successCount))
	} else {
		s.gridPlaced = false
		utils.Logger.Warn("网格订单放置不完整，允许重试",
			zap.Int("success_count", successCount),
			zap.Int("expected", s.cfg.MaxSafetyOrders))
	}
	s.mu.Unlock()

	// ★ 修复：在 gridPlaced 状态确定后，再更新 TP（防止 TP 在网格未完成时触发）
	s.safeUpdateTP()
}

// placeOrderWithRetry 带重试的下单逻辑（3次重试 + 抖动指数退避）。
// ★ 修复：返回 bool 表示是否成功，供 placeGridOrders 追踪网格完整性。
func (s *MartingaleStrategy) placeOrderWithRetry(side exchange.OrderSide, orderType exchange.OrderTypeKind, qty, price float64, level int) bool {
	const maxRetries = 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		_, err := s.exchange.CreateOrder(side, orderType, qty, price)
		if err == nil {
			return true // 成功
		}

		if attempt < maxRetries-1 {
			// 抖动指数退避：200ms × 2^attempt + 随机抖动
			backoff := time.Duration(200*(1<<attempt)) * time.Millisecond
			jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
			utils.Logger.Warn("网格订单重试",
				zap.Int("index", level),
				zap.Int("attempt", attempt+1),
				zap.Duration("backoff", backoff+jitter),
				zap.Error(err))
			time.Sleep(backoff + jitter)
		} else {
			utils.Logger.Error("网格订单最终失败",
				zap.Int("index", level),
				zap.Int("max_retries", maxRetries),
				zap.Error(err))
		}
	}
	return false // 全部重试失败
}

// findLiveTP 查询交易所端真实存在的 TP 订单（SELL + Limit）。
//
// 用途：
//   - 入口对账：本地 currentTPOrderID == 0 时，认领交易所端可能遗留的 TP，
//     避免"无本地记录 → 盲目 CreateOrder"产生重复 TP。
//   - Modify 失败对账：Modify 在 HTTP 层失败但交易所端可能已成功（原子 cancel+place），
//     通过查询真实状态判断是否需要降级 Create，避免重复 TP。
//
// 返回值：
//   - 找到 TP：(orderID, quantity, price, nil)
//   - 无 TP：(0, 0, 0, nil)
//   - 查询失败：(0, 0, 0, err)
//
// 异常自愈：若发现多个 TP（历史重复 create 的遗留），保留第一个并异步取消其余，
// 防止重复 TP 累积。
func (s *MartingaleStrategy) findLiveTP() (int64, float64, float64, error) {
	orders, err := s.exchange.GetOpenOrders()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("查询挂单失败: %w", err)
	}

	var tpOrders []exchange.OpenOrder
	for _, o := range orders {
		if o.Side == exchange.OrderSideSell && o.Type == exchange.OrderTypeLimit {
			tpOrders = append(tpOrders, o)
		}
	}

	if len(tpOrders) == 0 {
		return 0, 0, 0, nil
	}

	if len(tpOrders) > 1 {
		utils.Logger.Warn("发现多个 TP 订单（异常状态），保留第一个并取消其余",
			zap.Int("count", len(tpOrders)),
			zap.Int64("keep_id", tpOrders[0].OrderID))
		for i := 1; i < len(tpOrders); i++ {
			extraID := tpOrders[i].OrderID
			go func(id int64) {
				defer func() {
					if r := recover(); r != nil {
						utils.Logger.Error("清理多余 TP goroutine panic", zap.Any("recover", r))
					}
				}()
				if err := s.exchange.CancelOrder(id); err != nil {
					utils.Logger.Warn("清理多余 TP 失败",
						zap.Int64("order_id", id), zap.Error(err))
				}
			}(extraID)
		}
	}

	tp := tpOrders[0]
	return tp.OrderID, tp.Quantity, tp.Price, nil
}

// updateTP 更新止盈订单
//
// 核心逻辑（P1 加固 + 仓位变化检测 + modify 优先 + 对账防重复）：
//   - 入口对账：本地无 TP 记录时查交易所端，认领遗留 TP，避免盲目 create 产生重复
//   - 仓位大小未变化时跳过更新（不获取 ATR，不修改 TP）
//   - 仓位大小变化时重新获取 ATR(30m) 计算止盈位置
//   - 优先使用 ModifyOrder 原子替换，避免取消+重建的空窗期
//   - Modify 失败时对账交易所端真实状态：modify 实际成功则只同步本地状态，
//     真未成功才降级 cancel + create，杜绝"网络失败 + 交易所端成功"导致的重复 TP
//
// 调用约定：调用方（safeUpdateTP）必须已持有 tpMu 锁。
func (s *MartingaleStrategy) updateTP() {
	utils.Logger.Info("updateTP 开始")

	// 1. 获取更新后的持仓
	pos, err := s.exchange.GetPosition()
	if err != nil {
		utils.Logger.Error("获取持仓信息失败（TP 更新）", zap.Error(err))
		return
	}

	// 如果持仓已清零，不需要 TP
	if math.Abs(pos.Size) == 0 {
		s.mu.Lock()
		s.currentTPOrderID = 0
		s.lastTPQty = 0
		s.lastTPPrice = 0
		s.mu.Unlock()
		utils.Logger.Info("持仓已清零，清除 TP 状态")
		return
	}

	// ★ P1 加固：先在锁内读取必要变量，迅速释放锁，再在锁外发起网络请求
	s.mu.RLock()
	isIdle := s.currentState == StateIdle
	oldTPID := s.currentTPOrderID
	prevQty := s.lastTPQty
	s.mu.RUnlock()

	// 安全检查：如果状态为 IDLE，不更新 TP
	if isIdle {
		utils.Logger.Info("updateTP 跳过：状态为 IDLE")
		return
	}

	// ★ 对账防重复（入口）：本地无 TP 记录时，查交易所端是否已有遗留 TP。
	// 场景：重启后 currentTPOrderID 丢失 / 卖单事件清零后交易所端 TP 未同步取消 /
	//       modify 网络失败但交易所端成功后本地状态未更新。
	// 若不认领，后续会盲目 CreateOrder 产生重复 TP（数量价格完全相同）。
	// 仅在 oldTPID == 0 时触发，常规路径不增加 GetOpenOrders 开销。
	if oldTPID == 0 {
		liveID, liveQty, livePrice, reconcileErr := s.findLiveTP()
		if reconcileErr != nil {
			utils.Logger.Warn("入口对账：查询挂单失败，继续以本地状态为准",
				zap.Error(reconcileErr))
		} else if liveID != 0 {
			s.mu.Lock()
			s.currentTPOrderID = liveID
			s.lastTPQty = liveQty
			s.lastTPPrice = livePrice
			s.mu.Unlock()
			oldTPID = liveID
			prevQty = liveQty
			utils.Logger.Info("入口对账：认领交易所端已存在的 TP",
				zap.Int64("tp_id", liveID),
				zap.Float64("qty", liveQty),
				zap.Float64("price", livePrice))
		}
	}

	// ★ 审计修复：TP 数量使用 FloorToDecimals 向下取整，与实际持仓精度对齐。
	// 确保 tp_qty ≤ 实际持仓量，平仓后不会产生反向微型尾仓。
	newQty := utils.FloorToDecimals(math.Abs(pos.Size), s.quantityPrecision)

	// ★ 仓位变化检测：如果仓位未变且已有 TP 订单，跳过更新
	// 此时不获取 ATR、不修改 TP 价格，符合"仓位未变不更新止盈位置"的需求
	// 入口对账已保证 oldTPID 反映交易所端真实状态，无需担心 oldTPID==0 盲区
	if newQty == prevQty && oldTPID != 0 {
		utils.Logger.Debug("updateTP 跳过：仓位未变化",
			zap.Float64("qty", newQty),
			zap.Float64("prev_qty", prevQty),
			zap.Int64("tp_id", oldTPID))
		return
	}

	utils.Logger.Info("仓位变化，更新 TP",
		zap.Float64("prev_qty", prevQty),
		zap.Float64("new_qty", newQty),
		zap.Int64("old_tp_id", oldTPID))

	// ★ 固定百分比止盈：持仓均价 +0.80%
	tpPrice := utils.RoundToSigFigs(pos.EntryPrice*1.008, 5, s.maxPriceDecimals)

	utils.Logger.Info("计算新 TP",
		zap.Float64("entry_price", pos.EntryPrice),
		zap.Float64("tp_price", tpPrice),
		zap.Float64("tp_pct", 0.80),
		zap.Float64("tp_qty", newQty))

	// ★ 优先使用 ModifyOrder 原子替换（避免取消+重建的空窗期）
	if oldTPID != 0 {
		resp, modErr := s.exchange.ModifyOrder(oldTPID, exchange.OrderSideSell, exchange.OrderTypeLimit, newQty, tpPrice)
		if modErr == nil {
			s.mu.Lock()
			if s.currentState == StateIdle {
				s.mu.Unlock()
				utils.Logger.Info("Modify 成功但周期已结束，取消新 TP", zap.Int64("id", resp.OrderID))
				go func() {
					defer func() {
						if r := recover(); r != nil {
							utils.Logger.Error("取消 TP goroutine panic", zap.Any("recover", r))
						}
					}()
					s.exchange.CancelOrder(resp.OrderID)
				}()
				return
			}
			// modify 成功：订单 ID 可能变化，更新本地状态
			if resp.OrderID != 0 {
				s.currentTPOrderID = resp.OrderID
			}
			s.lastTPQty = newQty
			s.lastTPPrice = tpPrice
			s.mu.Unlock()
			utils.Logger.Info("TP 已通过 Modify 更新",
				zap.Int64("tp_id", resp.OrderID),
				zap.Float64("qty", newQty),
				zap.Float64("price", tpPrice))
			return
		}
		// modify 失败（可能订单已成交/已取消/交易所拒绝/HTTP 超时）。
		// ★ 对账防重复：Hyperliquid modify 等价于原子 cancel+place，HTTP 失败时
		// 交易所端可能已成功。盲目降级 cancel+create 会产生重复 TP（数量价格相同）。
		// 通过查询真实状态判断：
		//   - liveID != oldTPID：modify 实际成功（新 TP 已在），只同步本地状态，跳过 create
		//   - liveID == oldTPID：modify 真未生效，旧 TP 仍在，cancel 后 create
		//   - liveID == 0：旧 TP 已不在（被 modify 取消但新单未生成 / 已成交），直接 create
		utils.Logger.Warn("Modify TP 失败，对账交易所端真实状态",
			zap.Int64("old_tp_id", oldTPID),
			zap.Error(modErr))

		liveID, _, _, reconcileErr := s.findLiveTP()
		if reconcileErr != nil {
			utils.Logger.Warn("对账查询失败，保守跳过本次更新，等下次重试",
				zap.Error(reconcileErr))
			return
		}

		if liveID != 0 && liveID != oldTPID {
			// modify 网络失败但交易所端已成功，新 TP 已存在 → 只同步本地状态，不重复 create
			s.mu.Lock()
			if s.currentState == StateIdle {
				s.mu.Unlock()
				utils.Logger.Info("Modify 网络失败但交易所端已成功，周期已结束，取消新 TP",
					zap.Int64("old_tp_id", oldTPID),
					zap.Int64("new_tp_id", liveID))
				go func() {
					defer func() {
						if r := recover(); r != nil {
							utils.Logger.Error("取消 TP goroutine panic", zap.Any("recover", r))
						}
					}()
					s.exchange.CancelOrder(liveID)
				}()
				return
			}
			s.currentTPOrderID = liveID
			s.lastTPQty = newQty
			s.lastTPPrice = tpPrice
			s.mu.Unlock()
			utils.Logger.Info("Modify 网络失败但交易所端已成功，同步本地状态，跳过 create",
				zap.Int64("old_tp_id", oldTPID),
				zap.Int64("new_tp_id", liveID),
				zap.Float64("qty", newQty),
				zap.Float64("price", tpPrice))
			return
		}

		// liveID == oldTPID（旧 TP 仍在）或 liveID == 0（旧 TP 已不在）
		// 走降级 cancel + create，但只在旧 TP 确实还在时才 cancel，避免对已不存在的订单 cancel
		if liveID == oldTPID && oldTPID != 0 {
			utils.Logger.Info("旧 TP 确实仍在，取消后重建", zap.Int64("id", oldTPID))
			if err := s.exchange.CancelOrder(oldTPID); err != nil {
				utils.Logger.Warn("取消旧 TP 失败（可能已成交或已取消）", zap.Error(err))
			}
		} else {
			utils.Logger.Info("旧 TP 已不在交易所端，跳过 cancel 直接 create",
				zap.Int64("old_tp_id", oldTPID),
				zap.Int64("live_tp_id", liveID))
		}
	}

	// 放置新 TP
	resp, err := s.exchange.CreateOrder(exchange.OrderSideSell, exchange.OrderTypeLimit, newQty, tpPrice)
	if err != nil {
		utils.Logger.Error("TP 订单下单失败", zap.Error(err))
		return
	}

	s.mu.Lock()
	if s.currentState == StateIdle {
		s.mu.Unlock()
		utils.Logger.Info("TP 更新期间周期已结束，取消新 TP", zap.Int64("id", resp.OrderID))
		go func() {
			defer func() {
				if r := recover(); r != nil {
					utils.Logger.Error("取消 TP goroutine panic", zap.Any("recover", r))
				}
			}()
			s.exchange.CancelOrder(resp.OrderID)
		}()
		return
	}
	s.currentTPOrderID = resp.OrderID
	s.lastTPQty = newQty
	s.lastTPPrice = tpPrice
	s.mu.Unlock()

	utils.Logger.Info("TP 已通过 Create 更新",
		zap.Int64("tp_id", resp.OrderID),
		zap.Float64("qty", newQty),
		zap.Float64("price", tpPrice))
}

// ---------------------------------------------------------------------------
// 辅助方法
// ---------------------------------------------------------------------------

// cancelEntryOrder 取消当前入场单（如果存在），清理跟踪状态。
// 幂等安全：entryOrderID == 0 时直接返回。
func (s *MartingaleStrategy) cancelEntryOrder() {
	oid := s.entryOrderID
	if oid == 0 {
		return
	}
	utils.Logger.Info("取消未成交的入场限价单", zap.Int64("order_id", oid))
	if err := s.exchange.CancelOrder(oid); err != nil {
		utils.Logger.Warn("取消入场单失败（可能已成交）",
			zap.Int64("order_id", oid),
			zap.Error(err))
	}
	s.entryOrderID = 0
	s.entrySubmittedQty = 0
	s.entryCumulativeFilled = 0
}

// calcMinNotional 动态计算最低下单金额
func (s *MartingaleStrategy) calcMinNotional() float64 {
	balance, err := s.exchange.GetBalance()
	if err != nil {
		utils.Logger.Error("获取余额失败，使用 MinOrderValue", zap.Error(err))
		return MinOrderValue
	}
	notional := balance * s.cfg.BaseRatio
	if notional < MinOrderValue {
		notional = MinOrderValue
	}
	utils.Logger.Info("动态 MinNotional",
		zap.Float64("balance", balance),
		zap.Float64("ratio", s.cfg.BaseRatio),
		zap.Float64("notional", notional))
	return notional
}

// getGridMultiplier 计算网格加仓数量倍数。
//
// 固定数量递增规则：
//
//	层级  倍数    说明
//	 1    0.03   加仓1 = 0.03 × base_ratio
//	 2    0.03   加仓2 = 0.03 × base_ratio
//	 3    0.05   加仓3 = 0.05 × base_ratio
//	 4    0.05   加仓4 = 0.05 × base_ratio
//	 5    0.18   加仓5 = 0.18 × base_ratio
//	 6    0.32   加仓6 = 0.32 × base_ratio
//	 7    0.567  加仓7 = 0.567 × base_ratio
//	 8    0.578  加仓8 = 0.578 × base_ratio
//	 9    1.16   加仓9 = 1.16 × base_ratio
//	...  1.16   后续层使用最后一层倍数
//
// 首仓使用 enterLong 中硬编码的 0.06 系数，不通过此函数。
func (s *MartingaleStrategy) getGridMultiplier(level int) float64 {
	switch level {
	case 1:
		return 0.03
	case 2:
		return 0.03
	case 3:
		return 0.05
	case 4:
		return 0.05
	case 5:
		return 0.18
	case 6:
		return 0.32
	case 7:
		return 0.567
	case 8:
		return 0.578
	default:
		return 1.16
	}
}
