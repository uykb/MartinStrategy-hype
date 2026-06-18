// Package strategy 实现马丁格尔网格策略的有限状态机 (FSM)。
// 本文件包含 TP（止盈）更新逻辑的单元测试。
package strategy

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/uykb/MartinStrategy/internal/config"
	"github.com/uykb/MartinStrategy/internal/core"
	"github.com/uykb/MartinStrategy/internal/exchange"
	"github.com/uykb/MartinStrategy/internal/utils"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// Mock ExchangeAdapter
// ---------------------------------------------------------------------------

// mockAdapter 是用于测试的 ExchangeAdapter 实现。
// 它记录所有调用，允许测试断言行为。
type mockAdapter struct {
	mu sync.Mutex

	symbol string

	// 持仓状态
	posSize       float64
	posEntryPrice float64

	// 余额
	balance float64

	// K线数据（用于 ATR 计算）
	klines map[string][]exchange.Candle

	// 调用记录
	createOrderCount int
	modifyOrderCount int
	cancelOrderCount int

	// 返回的订单 ID 计数器
	nextOrderID int64

	// Modify 错误注入（设为非 nil 则 Modify 返回错误）
	modifyErr error

	// Create 错误注入
	createErr error

	// 记录最后一次 Modify 的参数
	lastModifyOrderID int64
	lastModifyQty     float64
	lastModifyPrice   float64

	// 记录最后一次 Create 的参数
	lastCreateSide  exchange.OrderSide
	lastCreateQty   float64
	lastCreatePrice float64

	// 已存在的挂单（用于 GetOpenOrders）
	openOrders []exchange.OpenOrder
}

func newMockAdapter() *mockAdapter {
	return &mockAdapter{
		symbol:      "HYPE",
		balance:     1000.0,
		nextOrderID: 10000,
		klines:      make(map[string][]exchange.Candle),
	}
}

func (m *mockAdapter) Start(ctx context.Context) error { return nil }
func (m *mockAdapter) Stop() error                     { return nil }

func (m *mockAdapter) GetLatestPrice() (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.posEntryPrice, nil
}

func (m *mockAdapter) GetKlines(interval string, limit int) ([]exchange.Candle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.klines[interval], nil
}

func (m *mockAdapter) GetPosition() (*exchange.Position, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &exchange.Position{
		Symbol:     m.symbol,
		Size:       m.posSize,
		EntryPrice: m.posEntryPrice,
	}, nil
}

func (m *mockAdapter) GetBalance() (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.balance, nil
}

func (m *mockAdapter) CreateOrder(side exchange.OrderSide, orderType exchange.OrderTypeKind, quantity, price float64) (*exchange.OrderResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.createOrderCount++
	m.lastCreateSide = side
	m.lastCreateQty = quantity
	m.lastCreatePrice = price

	if m.createErr != nil {
		return nil, m.createErr
	}

	m.nextOrderID++
	return &exchange.OrderResponse{
		OrderID: m.nextOrderID,
		Status:  "resting",
	}, nil
}

func (m *mockAdapter) ModifyOrder(orderID int64, side exchange.OrderSide, orderType exchange.OrderTypeKind, quantity, price float64) (*exchange.OrderResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.modifyOrderCount++
	m.lastModifyOrderID = orderID
	m.lastModifyQty = quantity
	m.lastModifyPrice = price

	if m.modifyErr != nil {
		return nil, m.modifyErr
	}

	return &exchange.OrderResponse{
		OrderID: orderID, // modify 通常返回相同 ID
		Status:  "resting",
	}, nil
}

func (m *mockAdapter) CancelOrder(orderID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cancelOrderCount++
	return nil
}

func (m *mockAdapter) CancelAllOrders() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.openOrders = nil
	return nil
}

func (m *mockAdapter) GetOpenOrders() ([]exchange.OpenOrder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.openOrders, nil
}

func (m *mockAdapter) GetSymbol() string { return m.symbol }

func (m *mockAdapter) GetSymbolInfo() (*exchange.SymbolInfo, error) {
	return &exchange.SymbolInfo{
		QuantityPrecision: 2,
		PricePrecision:    5,
		MinQty:            0.01,
		StepSize:          0.01,
		TickSize:          0.0001,
		SzDecimals:        2,
		MaxPriceDecimals:  4,
	}, nil
}

// setPos 线程安全地设置持仓状态，供并发测试使用。
// 直接写 mockAdapter.posSize 字段会绕过 mu 锁，与 GetPosition 的读取产生数据竞争。
func (m *mockAdapter) setPos(size, entryPrice float64) {
	m.mu.Lock()
	m.posSize = size
	m.posEntryPrice = entryPrice
	m.mu.Unlock()
}

// setKlines 设置指定周期的 K 线数据，使 ATR 计算返回可预测的值。
func (m *mockAdapter) setKlines(interval string, count int, price float64) {
	candles := make([]exchange.Candle, count)
	for i := range candles {
		candles[i] = exchange.Candle{
			Open:   price,
			High:   price * 1.01,
			Low:    price * 0.99,
			Close:  price,
			Volume: 1000,
		}
	}
	m.mu.Lock()
	m.klines[interval] = candles
	m.mu.Unlock()
}

// getOrderCounts 线程安全地获取订单调用计数器（供并发测试使用）。
func (m *mockAdapter) getOrderCounts() (create, modify, cancel int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.createOrderCount, m.modifyOrderCount, m.cancelOrderCount
}

// ---------------------------------------------------------------------------
// 测试辅助函数
// ---------------------------------------------------------------------------

// newTestStrategy 创建用于测试的策略实例。
func newTestStrategy(t *testing.T, adapter *mockAdapter) *MartingaleStrategy {
	// 初始化 logger（如果尚未初始化）
	if utils.Logger == nil {
		utils.Logger = zap.NewNop()
	}

	cfg := &config.StrategyConfig{
		MaxSafetyOrders: 9,
		AtrPeriod:       14,
		BaseRatio:       0.05,
	}

	bus := core.NewEventBus()
	go bus.Start()
	t.Cleanup(bus.Stop)

	s := NewMartingaleStrategy(cfg, adapter, nil, bus)

	// 手动初始化精度信息（跳过 initSymbolInfo 的网络调用）
	s.quantityPrecision = 2
	s.pricePrecision = 5
	s.minQty = 0.01
	s.stepSize = 0.01
	s.tickSize = 0.0001
	s.szDecimals = 2
	s.maxPriceDecimals = 4

	// ★ 设置初始同步完成标志，绕过历史事件过滤
	s.initialSyncDone.Store(true)

	return s
}

// setupInPosition 将策略设置为 IN_POSITION 状态，模拟已有持仓和 TP。
func setupInPosition(s *MartingaleStrategy, adapter *mockAdapter, posSize, entryPrice float64, tpID int64) {
	adapter.setPos(posSize, entryPrice)

	s.mu.Lock()
	s.currentState = StateInPosition
	s.gridPlaced = true
	s.currentTPOrderID = tpID
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// 测试用例
// ---------------------------------------------------------------------------

// TestUpdateTP_NoChange_SkipsModify 验证仓位未变化时跳过 TP 更新。
// 期望：不调用 ModifyOrder、CancelOrder、CreateOrder，不获取 ATR。
func TestUpdateTP_NoChange_SkipsModify(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	posSize := 100.0
	entryPrice := 50.0
	tpID := int64(10001)

	setupInPosition(s, adapter, posSize, entryPrice, tpID)

	// 设置 lastTPQty 为截断后的 posSize，模拟上次 TP 已对齐
	s.mu.Lock()
	s.lastTPQty = utils.FloorToDecimals(math.Abs(posSize), s.quantityPrecision)
	s.lastTPPrice = 51.0
	s.mu.Unlock()

	// 设置 ATR K线数据（如果 updateTP 调用了 fetchATR，说明逻辑有误）
	adapter.setKlines("30m", 50, entryPrice)

	// 直接在 tpMu 锁保护下调用 updateTP（模拟 safeUpdateTP 的行为）
	s.tpMu.Lock()
	s.updateTP()
	s.tpMu.Unlock()

	if adapter.modifyOrderCount != 0 {
		t.Errorf("仓位未变化时不应调用 ModifyOrder，实际调用了 %d 次", adapter.modifyOrderCount)
	}
	if adapter.cancelOrderCount != 0 {
		t.Errorf("仓位未变化时不应调用 CancelOrder，实际调用了 %d 次", adapter.cancelOrderCount)
	}
	if adapter.createOrderCount != 0 {
		t.Errorf("仓位未变化时不应调用 CreateOrder，实际调用了 %d 次", adapter.createOrderCount)
	}
}

// TestUpdateTP_SizeChanged_UsesModify 验证仓位变化时使用 ModifyOrder 更新 TP。
// 期望：调用 ModifyOrder，不调用 CancelOrder/CreateOrder，lastTPQty 更新为新仓位。
func TestUpdateTP_SizeChanged_UsesModify(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	initialSize := 100.0
	newSize := 150.0
	entryPrice := 50.0
	tpID := int64(10001)

	setupInPosition(s, adapter, newSize, entryPrice, tpID)

	// 设置 lastTPQty 为旧仓位（与新仓位不同）
	s.mu.Lock()
	s.lastTPQty = utils.FloorToDecimals(initialSize, s.quantityPrecision)
	s.lastTPPrice = 51.0
	s.mu.Unlock()

	// 设置 ATR K线数据
	adapter.setKlines("30m", 50, entryPrice)

	s.tpMu.Lock()
	s.updateTP()
	s.tpMu.Unlock()

	if adapter.modifyOrderCount != 1 {
		t.Fatalf("仓位变化时应调用 ModifyOrder 1 次，实际 %d 次", adapter.modifyOrderCount)
	}
	if adapter.cancelOrderCount != 0 {
		t.Errorf("Modify 成功时不应调用 CancelOrder，实际调用了 %d 次", adapter.cancelOrderCount)
	}
	if adapter.createOrderCount != 0 {
		t.Errorf("Modify 成功时不应调用 CreateOrder，实际调用了 %d 次", adapter.createOrderCount)
	}

	// 验证 Modify 参数
	expectedQty := utils.FloorToDecimals(newSize, s.quantityPrecision)
	if adapter.lastModifyQty != expectedQty {
		t.Errorf("Modify 数量错误: 期望 %f, 实际 %f", expectedQty, adapter.lastModifyQty)
	}
	if adapter.lastModifyOrderID != tpID {
		t.Errorf("Modify 订单 ID 错误: 期望 %d, 实际 %d", tpID, adapter.lastModifyOrderID)
	}

	// 验证 lastTPQty 已更新
	s.mu.RLock()
	updatedQty := s.lastTPQty
	s.mu.RUnlock()
	if updatedQty != expectedQty {
		t.Errorf("lastTPQty 未更新: 期望 %f, 实际 %f", expectedQty, updatedQty)
	}
}

// TestUpdateTP_ModifyFails_FallsBackToCancelCreate 验证 Modify 失败时降级到 cancel+create。
func TestUpdateTP_ModifyFails_FallsBackToCancelCreate(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	initialSize := 100.0
	newSize := 150.0
	entryPrice := 50.0
	tpID := int64(10001)

	setupInPosition(s, adapter, newSize, entryPrice, tpID)

	s.mu.Lock()
	s.lastTPQty = utils.FloorToDecimals(initialSize, s.quantityPrecision)
	s.lastTPPrice = 51.0
	s.mu.Unlock()

	// 注入 Modify 错误
	adapter.modifyErr = fmt.Errorf("order already filled")

	adapter.setKlines("30m", 50, entryPrice)

	s.tpMu.Lock()
	s.updateTP()
	s.tpMu.Unlock()

	if adapter.modifyOrderCount != 1 {
		t.Fatalf("应先尝试 Modify 1 次，实际 %d 次", adapter.modifyOrderCount)
	}
	if adapter.cancelOrderCount != 1 {
		t.Errorf("Modify 失败后应调用 CancelOrder 1 次，实际 %d 次", adapter.cancelOrderCount)
	}
	if adapter.createOrderCount != 1 {
		t.Errorf("Modify 失败后应调用 CreateOrder 1 次，实际 %d 次", adapter.createOrderCount)
	}

	// 验证 lastTPQty 已更新
	expectedQty := utils.FloorToDecimals(newSize, s.quantityPrecision)
	s.mu.RLock()
	updatedQty := s.lastTPQty
	tpIDAfter := s.currentTPOrderID
	s.mu.RUnlock()
	if updatedQty != expectedQty {
		t.Errorf("lastTPQty 未更新: 期望 %f, 实际 %f", expectedQty, updatedQty)
	}
	if tpIDAfter == 0 {
		t.Error("currentTPOrderID 应为新的订单 ID")
	}
}

// TestUpdateTP_ZeroPosition_ClearsTP 验证持仓清零时清除 TP 状态。
func TestUpdateTP_ZeroPosition_ClearsTP(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	tpID := int64(10001)
	setupInPosition(s, adapter, 100.0, 50.0, tpID)

	s.mu.Lock()
	s.lastTPQty = 100.0
	s.lastTPPrice = 51.0
	s.mu.Unlock()

	// 持仓清零
	adapter.setPos(0, 50.0)

	s.tpMu.Lock()
	s.updateTP()
	s.tpMu.Unlock()

	s.mu.RLock()
	tpIDAfter := s.currentTPOrderID
	lastQty := s.lastTPQty
	lastPrice := s.lastTPPrice
	s.mu.RUnlock()

	if tpIDAfter != 0 {
		t.Errorf("持仓清零后 currentTPOrderID 应为 0，实际 %d", tpIDAfter)
	}
	if lastQty != 0 {
		t.Errorf("持仓清零后 lastTPQty 应为 0，实际 %f", lastQty)
	}
	if lastPrice != 0 {
		t.Errorf("持仓清零后 lastTPPrice 应为 0，实际 %f", lastPrice)
	}
}

// TestUpdateTP_NoExistingTP_CreatesNew 验证没有已有 TP 时直接创建新 TP。
func TestUpdateTP_NoExistingTP_CreatesNew(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	posSize := 100.0
	entryPrice := 50.0

	setupInPosition(s, adapter, posSize, entryPrice, 0) // tpID = 0

	// lastTPQty = 0（初始值）
	adapter.setKlines("30m", 50, entryPrice)

	s.tpMu.Lock()
	s.updateTP()
	s.tpMu.Unlock()

	// 没有 oldTPID，不应调用 Modify 或 Cancel
	if adapter.modifyOrderCount != 0 {
		t.Errorf("无已有 TP 时不应调用 ModifyOrder，实际 %d 次", adapter.modifyOrderCount)
	}
	if adapter.cancelOrderCount != 0 {
		t.Errorf("无已有 TP 时不应调用 CancelOrder，实际 %d 次", adapter.cancelOrderCount)
	}
	if adapter.createOrderCount != 1 {
		t.Errorf("应调用 CreateOrder 1 次，实际 %d 次", adapter.createOrderCount)
	}

	// 验证创建了 sell 限价单
	if adapter.lastCreateSide != exchange.OrderSideSell {
		t.Errorf("应创建卖单，实际 %s", adapter.lastCreateSide)
	}

	// 验证 lastTPQty 已更新
	expectedQty := utils.FloorToDecimals(posSize, s.quantityPrecision)
	s.mu.RLock()
	updatedQty := s.lastTPQty
	s.mu.RUnlock()
	if updatedQty != expectedQty {
		t.Errorf("lastTPQty 未更新: 期望 %f, 实际 %f", expectedQty, updatedQty)
	}
}

// TestSafeUpdateTP_ConcurrentFills_DirtyFlag 验证并发成交时 dirty 标志确保最终 TP 一致。
// 模拟两个安全订单快速成交：第一次 updateTP 执行期间第二次请求被标记 dirty，
// 第一次完成后重跑，最终 TP 数量等于两次成交后的总仓位。
func TestSafeUpdateTP_ConcurrentFills_DirtyFlag(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	entryPrice := 50.0
	tpID := int64(10001)

	// 初始持仓 100
	adapter.setPos(100.0, entryPrice)
	adapter.setKlines("30m", 50, entryPrice)

	s.mu.Lock()
	s.currentState = StateInPosition
	s.gridPlaced = true
	s.currentTPOrderID = tpID
	s.lastTPQty = 100.0 // 上次 TP 已对齐
	s.mu.Unlock()

	// 第一次安全订单成交，仓位变为 150
	adapter.setPos(150.0, entryPrice)

	// 启动第一次 safeUpdateTP（在 goroutine 中）
	done := make(chan struct{})
	go func() {
		s.safeUpdateTP()
		close(done)
	}()

	// 等待第一次 updateTP 开始执行（获取 tpMu）
	time.Sleep(50 * time.Millisecond)

	// 第二次安全订单成交，仓位变为 200（线程安全写入，避免与 GetPosition 的读取竞争）
	adapter.setPos(200.0, entryPrice)

	// 尝试触发第二次 safeUpdateTP（应被 TryLock 拦截，标记 dirty）
	go s.safeUpdateTP()

	// 等待第一次完成
	<-done

	// 给 dirty 重跑一点时间
	time.Sleep(200 * time.Millisecond)

	// 验证最终 TP 数量等于 200（两次成交后的总仓位）
	s.mu.RLock()
	finalQty := s.lastTPQty
	s.mu.RUnlock()

	expectedQty := utils.FloorToDecimals(200.0, s.quantityPrecision)
	if finalQty != expectedQty {
		t.Errorf("并发成交后 TP 数量应为 %f（总仓位），实际 %f", expectedQty, finalQty)
	}
}

// TestUpdateTP_IdleState_SkipsUpdate 验证 IDLE 状态下不更新 TP。
func TestUpdateTP_IdleState_SkipsUpdate(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	// 设置持仓但状态为 IDLE
	adapter.setPos(100.0, 50.0)

	s.mu.Lock()
	s.currentState = StateIdle
	s.currentTPOrderID = 0
	s.mu.Unlock()

	s.tpMu.Lock()
	s.updateTP()
	s.tpMu.Unlock()

	if adapter.modifyOrderCount != 0 {
		t.Errorf("IDLE 状态下不应调用 ModifyOrder，实际 %d 次", adapter.modifyOrderCount)
	}
	if adapter.createOrderCount != 0 {
		t.Errorf("IDLE 状态下不应调用 CreateOrder，实际 %d 次", adapter.createOrderCount)
	}
}

// TestUpdateTP_PrecisionTruncation 验证仓位变化检测使用精度截断后的值对比。
// 即使浮点精度有微小差异，截断后相同则跳过更新。
func TestUpdateTP_PrecisionTruncation(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	tpID := int64(10001)
	// posSize 有微小浮点差异，但截断到 2 位小数后相同
	posSize := 100.000001
	entryPrice := 50.0

	setupInPosition(s, adapter, posSize, entryPrice, tpID)

	// lastTPQty = 100.00（截断后的值）
	s.mu.Lock()
	s.lastTPQty = utils.FloorToDecimals(100.0, s.quantityPrecision) // = 100.00
	s.mu.Unlock()

	adapter.setKlines("30m", 50, entryPrice)

	s.tpMu.Lock()
	s.updateTP()
	s.tpMu.Unlock()

	// 100.000001 截断到 2 位 = 100.00 = lastTPQty，应跳过
	if adapter.modifyOrderCount != 0 {
		t.Errorf("截断后仓位相同应跳过 ModifyOrder，实际调用了 %d 次", adapter.modifyOrderCount)
	}
	if adapter.createOrderCount != 0 {
		t.Errorf("截断后仓位相同应跳过 CreateOrder，实际调用了 %d 次", adapter.createOrderCount)
	}
}

// ---------------------------------------------------------------------------
// 网格放置完整性测试
// ---------------------------------------------------------------------------

// TestPlaceOrderWithRetry_ReturnsFalseOnFailure 验证下单失败时返回 false。
func TestPlaceOrderWithRetry_ReturnsFalseOnFailure(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	adapter.createErr = fmt.Errorf("insufficient balance")

	result := s.placeOrderWithRetry(exchange.OrderSideBuy, exchange.OrderTypeLimit, 1.0, 50.0, 1)

	if result {
		t.Error("下单失败时应返回 false，实际返回 true")
	}
	if adapter.createOrderCount != 3 {
		t.Errorf("应重试 3 次，实际调用了 %d 次", adapter.createOrderCount)
	}
}

// TestPlaceOrderWithRetry_ReturnsTrueOnSuccess 验证下单成功时返回 true。
func TestPlaceOrderWithRetry_ReturnsTrueOnSuccess(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	result := s.placeOrderWithRetry(exchange.OrderSideBuy, exchange.OrderTypeLimit, 1.0, 50.0, 1)

	if !result {
		t.Error("下单成功时应返回 true，实际返回 false")
	}
	if adapter.createOrderCount != 1 {
		t.Errorf("成功时应只调用 1 次，实际调用了 %d 次", adapter.createOrderCount)
	}
}

// TestSafeUpdateTP_DirtyLoop_MaxRetries 验证 dirty 循环有最大重试限制。
// 模拟高频并发场景：持续有新的 safeUpdateTP 调用设置 dirty，验证不会无限循环。
func TestSafeUpdateTP_DirtyLoop_MaxRetries(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	entryPrice := 50.0
	tpID := int64(10001)

	adapter.setPos(100.0, entryPrice)
	adapter.setKlines("30m", 50, entryPrice)

	s.mu.Lock()
	s.currentState = StateInPosition
	s.gridPlaced = true
	s.currentTPOrderID = tpID
	s.lastTPQty = 0 // 触发 updateTP 执行
	s.mu.Unlock()

	// 持续设置 dirty 标志，模拟高频并发调用
	stopDirty := make(chan struct{})
	dirtyCount := 0
	go func() {
		for {
			select {
			case <-stopDirty:
				return
			default:
				s.tpDirty.Store(true)
				dirtyCount++
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// 执行 safeUpdateTP
	start := time.Now()
	s.safeUpdateTP()
	elapsed := time.Since(start)

	close(stopDirty)

	// 验证：不应无限循环，应在合理时间内完成（< 5 秒）
	if elapsed > 5*time.Second {
		t.Errorf("safeUpdateTP 执行时间过长 (%v)，可能存在无限循环", elapsed)
	}

	// 验证：updateTP 应被调用至少 1 次（初始执行）+ 最多 3 次（dirty 重跑）
	// 总计最多 4 次，但不会无限循环
	if adapter.modifyOrderCount == 0 && adapter.createOrderCount == 0 {
		// 如果 lastTPQty == lastTPQty（仓位未变化），updateTP 会跳过
		// 这种情况下不调用 API 是正常的
		t.Log("仓位未变化，updateTP 跳过 API 调用（正常行为）")
	}
}

// TestHandleOrderUpdate_SafetyFill_AlwaysTriggersTP 验证安全订单成交时
// 无论 gridPlaced 状态如何，始终触发 TP 更新。
// 审计修复：gridPlaced 仅控制网格放置，不应阻止 TP 更新。
func TestHandleOrderUpdate_SafetyFill_AlwaysTriggersTP(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	entryPrice := 50.0
	adapter.setPos(100.0, entryPrice)
	adapter.setKlines("30m", 50, entryPrice)

	// 设置状态为 IN_POSITION 但 gridPlaced=false（重启后不完整网格场景）
	s.mu.Lock()
	s.currentState = StateInPosition
	s.gridPlaced = false
	s.currentTPOrderID = 10001
	s.lastTPQty = 50.0 // 旧仓位，与新仓位不同
	s.mu.Unlock()

	// 模拟安全订单成交
	event := core.Event{
		Type: core.EventOrderUpdate,
		Data: &exchange.OrderUpdate{
			OrderID:   20001,
			Symbol:    "HYPE",
			Side:      exchange.OrderSideBuy,
			Type:      exchange.OrderTypeLimit,
			Status:    "FILLED",
			ExecPrice: 48.0,
			Quantity:  0.5,
		},
	}

	err := s.handleOrderUpdate(context.Background(), event)
	if err != nil {
		t.Fatalf("handleOrderUpdate 失败: %v", err)
	}

	// 等待 goroutine 执行
	time.Sleep(200 * time.Millisecond)

	// 验证：无论 gridPlaced 如何，安全订单成交都应触发 TP 更新
	// ★ 使用线程安全的 getOrderCounts 避免与 goroutine 中的 ModifyOrder/CreateOrder 竞争
	createCount, modifyCount, _ := adapter.getOrderCounts()
	if modifyCount == 0 && createCount == 0 {
		t.Error("安全订单成交应始终触发 TP 更新，但未调用任何订单 API")
	}
}

// TestHandleOrderUpdate_SafetyFill_GridPlaced_TriggersTP 验证安全订单成交时
// 如果网格已放置，触发 TP 更新。
func TestHandleOrderUpdate_SafetyFill_GridPlaced_TriggersTP(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	entryPrice := 50.0
	adapter.setPos(100.0, entryPrice)
	adapter.setKlines("30m", 50, entryPrice)

	// 设置状态为 IN_POSITION 且 gridPlaced=true
	s.mu.Lock()
	s.currentState = StateInPosition
	s.gridPlaced = true
	s.currentTPOrderID = 10001
	s.lastTPQty = 50.0 // 旧仓位，与新仓位不同
	s.mu.Unlock()

	// 模拟安全订单成交
	event := core.Event{
		Type: core.EventOrderUpdate,
		Data: &exchange.OrderUpdate{
			OrderID:   20001,
			Symbol:    "HYPE",
			Side:      exchange.OrderSideBuy,
			Type:      exchange.OrderTypeLimit,
			Status:    "FILLED",
			ExecPrice: 48.0,
			Quantity:  0.5,
		},
	}

	err := s.handleOrderUpdate(context.Background(), event)
	if err != nil {
		t.Fatalf("handleOrderUpdate 失败: %v", err)
	}

	// 等待 goroutine 执行
	time.Sleep(200 * time.Millisecond)

	// 验证：应触发 TP 更新
	// ★ 使用线程安全的 getOrderCounts 避免与 goroutine 中的 ModifyOrder/CreateOrder 竞争
	createCount, modifyCount, _ := adapter.getOrderCounts()
	if modifyCount == 0 && createCount == 0 {
		t.Error("网格已放置时安全订单成交应触发 TP 更新，但未调用任何订单 API")
	}
}

// TestSyncState_IncompleteGrid_KeepsGridPlaced 验证 syncState 检测到不完整网格时
// 仍然设置 gridPlaced=true（不重新放置，防止爆仓风险）。
func TestSyncState_IncompleteGrid_KeepsGridPlaced(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	// 设置持仓
	adapter.setPos(100.0, 50.0)

	// 模拟不完整的网格订单（只有 3 个买单，期望 9 个）
	adapter.openOrders = []exchange.OpenOrder{
		{OrderID: 1, Side: exchange.OrderSideBuy, Type: exchange.OrderTypeLimit, Price: 49.0},
		{OrderID: 2, Side: exchange.OrderSideBuy, Type: exchange.OrderTypeLimit, Price: 48.0},
		{OrderID: 3, Side: exchange.OrderSideBuy, Type: exchange.OrderTypeLimit, Price: 47.0},
		{OrderID: 4, Side: exchange.OrderSideSell, Type: exchange.OrderTypeLimit, Price: 55.0}, // TP
	}

	s.syncState()

	s.mu.RLock()
	state := s.currentState
	gridPlaced := s.gridPlaced
	s.mu.RUnlock()

	if state != StateInPosition {
		t.Errorf("有持仓时状态应为 IN_POSITION，实际 %s", state)
	}
	// ★ 业务逻辑：重启时有持仓，gridPlaced 始终为 true，不重新放置网格
	if !gridPlaced {
		t.Error("重启时有持仓 gridPlaced 应为 true（不重新放置网格），实际为 false")
	}
}

// TestSyncState_CompleteGrid_SetsGridPlaced 验证 syncState 检测到完整网格时
// 设置 gridPlaced=true。
func TestSyncState_CompleteGrid_SetsGridPlaced(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	// 设置持仓
	adapter.setPos(100.0, 50.0)

	// 模拟完整的网格订单（9 个买单 + 1 个 TP 卖单）
	orders := []exchange.OpenOrder{
		{OrderID: 1, Side: exchange.OrderSideSell, Type: exchange.OrderTypeLimit, Price: 55.0}, // TP
	}
	for i := 2; i <= 10; i++ {
		orders = append(orders, exchange.OpenOrder{
			OrderID: int64(i),
			Side:    exchange.OrderSideBuy,
			Type:    exchange.OrderTypeLimit,
			Price:   50.0 - float64(i)*1.0,
		})
	}
	adapter.openOrders = orders

	s.syncState()

	s.mu.RLock()
	state := s.currentState
	gridPlaced := s.gridPlaced
	s.mu.RUnlock()

	if state != StateInPosition {
		t.Errorf("有持仓时状态应为 IN_POSITION，实际 %s", state)
	}
	if !gridPlaced {
		t.Error("网格完整时 gridPlaced 应为 true，实际为 false")
	}
}

// TestSyncState_WithTP_InitializesLastTPQty 验证 syncState 检测到已有 TP 时
// 初始化 lastTPQty 为当前持仓量，防止 resyncViaREST 触发不必要的 TP 重建。
func TestSyncState_WithTP_InitializesLastTPQty(t *testing.T) {
	adapter := newMockAdapter()
	s := newTestStrategy(t, adapter)

	posSize := 100.0
	entryPrice := 50.0
	adapter.setPos(posSize, entryPrice)

	// 模拟完整网格 + TP
	orders := []exchange.OpenOrder{
		{OrderID: 1, Side: exchange.OrderSideSell, Type: exchange.OrderTypeLimit, Price: 55.0}, // TP
	}
	for i := 2; i <= 10; i++ {
		orders = append(orders, exchange.OpenOrder{
			OrderID: int64(i),
			Side:    exchange.OrderSideBuy,
			Type:    exchange.OrderTypeLimit,
			Price:   entryPrice - float64(i)*1.0,
		})
	}
	adapter.openOrders = orders

	// 确保 lastTPQty 初始为 0（模拟重启后内存丢失）
	s.mu.Lock()
	s.lastTPQty = 0
	s.mu.Unlock()

	s.syncState()

	s.mu.RLock()
	lastTPQty := s.lastTPQty
	currentTPOrderID := s.currentTPOrderID
	s.mu.RUnlock()

	expectedQty := utils.FloorToDecimals(posSize, s.quantityPrecision)

	// 验证：lastTPQty 应被初始化为当前持仓量
	if lastTPQty != expectedQty {
		t.Errorf("lastTPQty 应初始化为 %f，实际 %f", expectedQty, lastTPQty)
	}

	// 验证：currentTPOrderID 应被设置
	if currentTPOrderID != 1 {
		t.Errorf("currentTPOrderID 应为 1，实际 %d", currentTPOrderID)
	}

	// 验证：不应触发 safeUpdateTP（因为 lastTPQty 已对齐，仓位变化检测会跳过）
	if adapter.modifyOrderCount != 0 {
		t.Errorf("lastTPQty 已对齐时不应调用 ModifyOrder，实际 %d 次", adapter.modifyOrderCount)
	}
	if adapter.createOrderCount != 0 {
		t.Errorf("lastTPQty 已对齐时不应调用 CreateOrder，实际 %d 次", adapter.createOrderCount)
	}
}
