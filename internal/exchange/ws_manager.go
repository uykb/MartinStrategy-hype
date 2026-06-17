// Package exchange 提供 WebSocket 连接管理器，实现三层稳定性防线：
//
//   第一层：主动心跳 —— 每 30 秒发送 ping，超时判定假死
//   第二层：断线重连 + 指数退避 —— 网络中断时自动重连
//   第三层：REST 对账 (Resync) —— 重连后强制查询持仓与挂单，校准 FSM
//
// 同时管理公有流（l2Book / trades → EventTick）与私有流
// （userFills / orderUpdates → EventOrderUpdate）的双通道订阅。
package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	hyperliquid "github.com/sonirico/go-hyperliquid"
	"github.com/uykb/MartinStrategy/internal/core"
	"github.com/uykb/MartinStrategy/internal/utils"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// WebSocket 消息类型定义
// ---------------------------------------------------------------------------

// wsRequest 表示发往 Hyperliquid WebSocket 的请求
type wsRequest struct {
	Method      string      `json:"method"`                // "subscribe" | "unsubscribe" | "ping"
	Subscription *wsSubscription `json:"subscription,omitempty"` // 订阅参数
}

// wsSubscription 表示订阅参数
type wsSubscription struct {
	Type string `json:"type"` // "l2Book" | "trades" | "userFills" | "orderUpdates" | "allMids"
	Coin string `json:"coin,omitempty"` // 交易对（公有流需要）
	User string `json:"user,omitempty"` // 用户地址（私有流需要）
}

// wsEnvelope 表示 WebSocket 推送消息的统一信封。
// ★ P3 优化：单次反序列化覆盖 pong 响应和频道消息，消除双重 JSON 解析。
// Hyperliquid WS 消息格式：
//   - 心跳响应：{"method":"pong"}
//   - 频道数据：{"channel":"l2Book","data":{...}}
//   - 订阅确认：{"channel":"subscriptionResponse","data":{...}}
type wsEnvelope struct {
	Method  string          `json:"method,omitempty"`  // "pong" 心跳响应
	Channel string          `json:"channel,omitempty"` // "l2Book" | "trades" | "userFills" | "orderUpdates"
	Data    json.RawMessage `json:"data,omitempty"`    // 频道负载数据（延迟解析）
}

// wsL2BookData 表示 l2Book 频道的数据
type wsL2BookData struct {
	Coin   string       `json:"coin"`
	Levels [][]wsLevel  `json:"levels"` // [0]=bids, [1]=asks
	Time   int64        `json:"time"`
}

// wsLevel 表示 L2 盘口的一个价位
type wsLevel struct {
	Px  float64 `json:"px,string"`
	Sz  float64 `json:"sz,string"`
	N   int     `json:"n"`
}

// wsTradeData 表示 trades 频道的数据
type wsTradeData struct {
	Coin string  `json:"coin"`
	Side string  `json:"side"` // "B"=buy, "S"=sell
	Px   float64 `json:"px,string"`
	Sz   float64 `json:"sz,string"`
	Time int64   `json:"time"`
}

// wsUserFillData 表示 userFills 频道的成交数据
type wsUserFillData struct {
	Fills []wsFill `json:"fills"`
}

// wsFill 表示一笔成交回报
type wsFill struct {
	Coin      string  `json:"coin"`
	Side      string  `json:"side"` // "B"=buy, "S"=sell
	Px        float64 `json:"px,string"`
	Sz        float64 `json:"sz,string"`
	Time      int64   `json:"time"`
	Hash      string  `json:"hash"`
	Oid       int64   `json:"oid,omitempty"` // 订单 ID
	Crossed   bool    `json:"crossed"`
	Directed  bool    `json:"directed"`
}

// wsOrderUpdateData 表示 orderUpdates 频道的订单更新数据
type wsOrderUpdateData struct {
	Orders []wsOrderUpdate `json:"orders"`
}

// wsOrderUpdate 表示一个订单状态更新
type wsOrderUpdate struct {
	Order struct {
		Coin    string `json:"coin"`
		Oid     int64  `json:"oid"`
		Side    string `json:"side"` // "B"=buy, "S"=sell
		OrigSz  string `json:"origSz"`
		Sz      string `json:"sz"`
		LimitPx string `json:"limitPx"`
		Status  string `json:"status"` // "open", "filled", "canceled" 等
	} `json:"order"`
	Status string `json:"status"`
}

// ---------------------------------------------------------------------------
// WebSocketManager 配置常量
// ---------------------------------------------------------------------------

const (
	// 心跳间隔：每 30 秒发送一次 ping
	wsPingInterval = 30 * time.Second

	// pong 超时：发送 ping 后 10 秒未收到 pong 判定假死
	wsPongTimeout = 10 * time.Second

	// 最大重连次数
	wsMaxReconnectRetries = 10

	// 初始退避时间
	wsInitialBackoff = 2 * time.Second

	// 最大退避时间
	wsMaxBackoff = 60 * time.Second

	// 写超时
	wsWriteTimeout = 5 * time.Second

	// 读超时（包含 pong 等待时间）
	wsReadTimeout = wsPingInterval + wsPongTimeout + 5*time.Second
)

// ---------------------------------------------------------------------------
// WebSocketManager 核心结构
// ---------------------------------------------------------------------------

// WSManager 管理 Hyperliquid WebSocket 连接的生命周期，
// 包括双通道订阅、心跳、重连与 REST 对账。
type WSManager struct {
	cfg    *HyperliquidConfig // 交易所配置
	bus    *core.EventBus     // 事件总线
	adapter *HyperliquidAdapter // 反向引用，用于 REST 对账

	// WebSocket 连接
	connMu sync.Mutex
	conn   *websocket.Conn

	// 生命周期控制
	ctx    context.Context
	cancel context.CancelFunc

	// 心跳
	pongCh chan struct{} // 收到 pong 时发信号

	// 重连状态
	reconnectMu sync.Mutex
	reconnecting bool

	// ★ P2 加固：WS 活跃状态标志（atomic，供 REST 降级和健康检查查询）
	wsActive atomic.Bool

	// 订阅参数（重连后需要重新订阅）
	subscriptions []wsSubscription

	// 事件 channel（带缓冲，防止高频行情阻塞网络 I/O）
	priceEventCh  chan *PriceUpdate     // 价格更新缓冲 channel（带时间戳防滑点）
	orderEventCh  chan *OrderUpdate     // 订单更新缓冲 channel

	// WaitGroup 用于优雅关闭
	wg sync.WaitGroup
}

// NewWSManager 创建 WebSocket 管理器实例
func NewWSManager(cfg *HyperliquidConfig, bus *core.EventBus, adapter *HyperliquidAdapter) *WSManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &WSManager{
		cfg:       cfg,
		bus:       bus,
		adapter:   adapter,
		ctx:       ctx,
		cancel:    cancel,
		pongCh:    make(chan struct{}, 1),
		priceEventCh:  make(chan *PriceUpdate, 500),  // 500 缓冲，防止高频行情丢失
		orderEventCh:  make(chan *OrderUpdate, 200), // 200 缓冲，防止成交回报丢失
	}
}

// ---------------------------------------------------------------------------
// 公开方法
// ---------------------------------------------------------------------------

// IsWSActive 返回 WebSocket 连接是否活跃。
// ★ P2 加固：供 REST 降级和健康检查查询，避免 WS 正常时 REST 重复推送。
func (w *WSManager) IsWSActive() bool {
	return w.wsActive.Load()
}

// Start 启动 WebSocket 管理器：连接、订阅、启动心跳与事件分发
func (w *WSManager) Start() error {
	// 1. 建立 WebSocket 连接
	if err := w.connect(); err != nil {
		return fmt.Errorf("WSManager.Start: 连接失败: %w", err)
	}

	// 2. 注册订阅列表
	w.registerSubscriptions()

	// 3. 发送订阅请求
	if err := w.sendSubscriptions(); err != nil {
		return fmt.Errorf("WSManager.Start: 订阅失败: %w", err)
	}

	// 4. 启动读协程
	w.wg.Add(1)
	go w.readLoop()

	// 5. 启动心跳协程
	w.wg.Add(1)
	go w.heartbeatLoop()

	// 6. 启动事件分发协程（将缓冲 channel 中的事件桥接到 EventBus）
	w.wg.Add(1)
	go w.dispatchPriceEvents()

	w.wg.Add(1)
	go w.dispatchOrderEvents()

	utils.Logger.Info("WSManager 启动成功",
		zap.String("ws_url", w.cfg.WSURL),
		zap.String("symbol", w.cfg.Symbol))

	return nil
}

// Stop 优雅关闭 WebSocket 管理器
func (w *WSManager) Stop() {
	utils.Logger.Info("WSManager 正在关闭...")

	// ★ P2 加固：标记 WS 不活跃
	w.wsActive.Store(false)

	// 取消上下文，通知所有协程退出
	w.cancel()

	// 关闭 WebSocket 连接
	w.connMu.Lock()
	if w.conn != nil {
		// 设置短超时后关闭，避免阻塞
		w.conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(time.Second))
		w.conn.Close()
		w.conn = nil
	}
	w.connMu.Unlock()

	// 等待所有协程退出
	w.wg.Wait()

	utils.Logger.Info("WSManager 已关闭")
}

// ---------------------------------------------------------------------------
// 连接管理
// ---------------------------------------------------------------------------

// connect 建立 WebSocket 连接
func (w *WSManager) connect() error {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(w.cfg.WSURL, nil)
	if err != nil {
		return fmt.Errorf("WebSocket 拨号失败: %w", err)
	}

	// 设置读超时
	conn.SetReadDeadline(time.Now().Add(wsReadTimeout))

	w.conn = conn

	// ★ P2 加固：标记 WS 活跃
	w.wsActive.Store(true)

	utils.Logger.Info("WebSocket 连接建立成功", zap.String("url", w.cfg.WSURL))
	return nil
}

// registerSubscriptions 注册需要订阅的频道列表
func (w *WSManager) registerSubscriptions() {
	w.subscriptions = []wsSubscription{
		// 公有流：l2Book 获取实时价格
		{Type: "l2Book", Coin: w.cfg.Symbol},
		// 私有流：userFills 获取成交回报
		{Type: "userFills", User: w.cfg.AccountAddress},
		// 私有流：orderUpdates 获取订单状态变更
		{Type: "orderUpdates", User: w.cfg.AccountAddress},
	}
}

// sendSubscriptions 发送所有订阅请求
func (w *WSManager) sendSubscriptions() error {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	if w.conn == nil {
		return fmt.Errorf("WebSocket 未连接")
	}

	for _, sub := range w.subscriptions {
		req := wsRequest{
			Method:      "subscribe",
			Subscription: &sub,
		}
		data, err := json.Marshal(req)
		if err != nil {
			return fmt.Errorf("序列化订阅请求失败: %w", err)
		}

		if err := w.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			return fmt.Errorf("发送订阅请求失败 (%s): %w", sub.Type, err)
		}

		utils.Logger.Info("已发送订阅请求",
			zap.String("type", sub.Type),
			zap.String("coin", sub.Coin),
			zap.String("user", sub.User))
	}

	return nil
}

// ---------------------------------------------------------------------------
// 第一层防线：主动心跳
// ---------------------------------------------------------------------------

// heartbeatLoop 定期发送 ping，检测连接存活
func (w *WSManager) heartbeatLoop() {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("heartbeatLoop panic 恢复", zap.Any("recover", r))
		}
	}()

	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return

		case <-ticker.C:
			if err := w.sendPing(); err != nil {
				utils.Logger.Warn("发送 ping 失败，触发重连", zap.Error(err))
				go w.triggerReconnect()
				return
			}

			// 等待 pong 响应
			select {
			case <-w.pongCh:
				utils.Logger.Debug("收到 pong，连接正常")
			case <-time.After(wsPongTimeout):
				utils.Logger.Warn("pong 超时，判定连接假死，触发重连")
				go w.triggerReconnect()
				return
			}
		}
	}
}

// sendPing 发送心跳 ping 消息
func (w *WSManager) sendPing() error {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	if w.conn == nil {
		return fmt.Errorf("连接不存在")
	}

	ping := wsRequest{Method: "ping"}
	data, err := json.Marshal(ping)
	if err != nil {
		return fmt.Errorf("序列化 ping 失败: %w", err)
	}

	if err := w.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("发送 ping 失败: %w", err)
	}

	utils.Logger.Debug("已发送 ping")
	return nil
}

// ---------------------------------------------------------------------------
// 第二层防线：断线重连 + 指数退避
// ---------------------------------------------------------------------------

// triggerReconnect 触发重连流程（带互斥保护，防止并发重连）
func (w *WSManager) triggerReconnect() {
	w.reconnectMu.Lock()
	if w.reconnecting {
		w.reconnectMu.Unlock()
		return
	}
	w.reconnecting = true
	w.reconnectMu.Unlock()

	defer func() {
		w.reconnectMu.Lock()
		w.reconnecting = false
		w.reconnectMu.Unlock()
	}()

	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("triggerReconnect panic 恢复", zap.Any("recover", r))
		}
	}()

	w.reconnectWithBackoff()
}

// reconnectWithBackoff 执行指数退避重连
func (w *WSManager) reconnectWithBackoff() {
	backoff := wsInitialBackoff

	for attempt := 1; attempt <= wsMaxReconnectRetries; attempt++ {
		// 检查是否已关闭
		select {
		case <-w.ctx.Done():
			utils.Logger.Info("重连取消：WSManager 已停止")
			return
		default:
		}

		utils.Logger.Info("尝试重连 WebSocket...",
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff),
			zap.Int("max_retries", wsMaxReconnectRetries))

		// 等待退避时间
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(backoff):
		}

		// 关闭旧连接
		w.connMu.Lock()
		if w.conn != nil {
			w.conn.Close()
			w.conn = nil
		}
		w.connMu.Unlock()

		// ★ P2 加固：标记 WS 不活跃（正在重连）
		w.wsActive.Store(false)

		// 尝试重新连接
		if err := w.connect(); err != nil {
			utils.Logger.Error("重连失败",
				zap.Int("attempt", attempt),
				zap.Error(err))

			// 指数退避，上限 60 秒
			backoff *= 2
			if backoff > wsMaxBackoff {
				backoff = wsMaxBackoff
			}
			continue
		}

		// 重连成功，重新订阅
		if err := w.sendSubscriptions(); err != nil {
			utils.Logger.Error("重新订阅失败", zap.Error(err))
			continue
		}

		utils.Logger.Info("WebSocket 重连成功", zap.Int("attempt", attempt))

		// 第三层防线：REST 对账
		w.resyncViaREST()

		// 重启读协程
		w.wg.Add(1)
		go w.readLoop()

		// 重启心跳协程
		w.wg.Add(1)
		go w.heartbeatLoop()

		return
	}

	utils.Logger.Error("WebSocket 重连失败：已达最大重试次数",
		zap.Int("max_retries", wsMaxReconnectRetries))
}

// ---------------------------------------------------------------------------
// 第三层防线：REST 对账 (Resync)
// ---------------------------------------------------------------------------

// resyncViaREST 在 WebSocket 重连后，通过 REST API 强制查询持仓与挂单，
// 校准 FSM 状态机，防止断线期间漏掉成交事件。
// ★ P2 加固：对账前后发布冻结/解冻事件，让 FSM 在对账期间暂停处理。
func (w *WSManager) resyncViaREST() {
	utils.Logger.Info("开始 REST 对账 (Resync)...")

	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("resyncViaREST panic 恢复", zap.Any("recover", r))
		}
	}()

	// ★ P2 加固：发布对账开始事件，冻结 FSM
	w.bus.Publish(core.EventResyncStart, nil)

	// 确保对账结束后发布解冻事件
	defer func() {
		w.bus.Publish(core.EventResyncEnd, nil)
		utils.Logger.Info("REST 对账完成，已发布解冻事件")
	}()

	// 1. 查询真实持仓
	pos, err := w.adapter.GetPosition()
	if err != nil {
		utils.Logger.Error("REST 对账：查询持仓失败", zap.Error(err))
		return
	}

	if pos != nil && pos.Size != 0 {
		utils.Logger.Info("REST 对账：检测到持仓",
			zap.Float64("size", pos.Size),
			zap.Float64("entry_price", pos.EntryPrice))

		// 发布持仓更新事件，让 FSM 校准
		w.bus.Publish(core.EventPositionUpdate, pos)
	} else {
		utils.Logger.Info("REST 对账：无持仓")
		// 发布空持仓事件，确保 FSM 知道没有持仓
		w.bus.Publish(core.EventPositionUpdate, &Position{Symbol: w.cfg.Symbol, Size: 0})
	}

	// 2. 查询挂单列表
	orders, err := w.adapter.GetOpenOrders()
	if err != nil {
		utils.Logger.Error("REST 对账：查询挂单失败", zap.Error(err))
		return
	}

	utils.Logger.Info("REST 对账：挂单列表",
		zap.Int("count", len(orders)))

	// 3. 查询最近成交记录（补漏）
	// 通过 REST 查询最近成交，防止断线期间漏掉成交
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fills, err := w.adapter.infoClient.UserFills(ctx, w.adapter.userFillsParams())
	if err != nil {
		utils.Logger.Warn("REST 对账：查询最近成交失败", zap.Error(err))
	} else {
		for _, fill := range fills {
			// 将断线期间可能漏掉的成交转换为 OrderUpdate 事件
			update := fillToOrderUpdate(fill, w.cfg.Symbol)
			if update != nil {
				utils.Logger.Info("REST 对账：补发漏掉的成交事件",
					zap.Int64("oid", update.OrderID),
					zap.String("side", string(update.Side)),
					zap.Float64("price", update.ExecPrice))
				w.bus.Publish(core.EventOrderUpdate, update)
			}
		}
	}
}

// fillToOrderUpdate 将 Hyperliquid Fill 转换为通用 OrderUpdate
func fillToOrderUpdate(fill hyperliquid.Fill, symbol string) *OrderUpdate {
	// 过滤非目标交易对
	if fill.Coin != symbol {
		return nil
	}

	update := &OrderUpdate{
		OrderID:   fill.Oid,
		Symbol:    fill.Coin,
		ExecPrice: 0,
		Quantity:  0,
		Status:    "FILLED",
	}

	// 解析价格和数量
	if px, err := strconv.ParseFloat(fill.Price, 64); err == nil {
		update.ExecPrice = px
	}
	if sz, err := strconv.ParseFloat(fill.Size, 64); err == nil {
		update.Quantity = sz
	}

	// 转换方向
	if fill.Side == "B" {
		update.Side = OrderSideBuy
	} else {
		update.Side = OrderSideSell
	}

	return update
}

// ---------------------------------------------------------------------------
// 读循环：接收并分发 WebSocket 消息
// ---------------------------------------------------------------------------

// readLoop 持续读取 WebSocket 消息并分发到对应处理器
func (w *WSManager) readLoop() {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("readLoop panic 恢复", zap.Any("recover", r))
			// panic 后触发重连
			go w.triggerReconnect()
		}
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		w.connMu.Lock()
		conn := w.conn
		w.connMu.Unlock()

		if conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 重置读超时（每次成功读取后重置）
		conn.SetReadDeadline(time.Now().Add(wsReadTimeout))

		_, message, err := conn.ReadMessage()
		if err != nil {
			// 判断是否为正常关闭
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				utils.Logger.Info("WebSocket 正常关闭")
				return
			}

			utils.Logger.Warn("WebSocket 读取错误，触发重连", zap.Error(err))
			go w.triggerReconnect()
			return
		}

		// 处理消息
		w.handleMessage(message)
	}
}

// handleMessage 解析并分发 WebSocket 消息。
// ★ P3 优化：单次反序列化，通过 wsEnvelope.Method / wsEnvelope.Channel 快速分流，
// 消除原双重 JSON 解析（先 map 检查 pong，再 struct 解析频道）。
// 在高频行情下（l2Book 每秒数十帧），减少 50% 的堆内存分配。
func (w *WSManager) handleMessage(message []byte) {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("handleMessage panic 恢复",
				zap.Any("recover", r),
				zap.String("message", string(message)))
		}
	}()

	// ★ 单次反序列化：统一解析为 wsEnvelope
	var env wsEnvelope
	if err := json.Unmarshal(message, &env); err != nil {
		utils.Logger.Debug("无法解析 WS 消息", zap.Error(err))
		return
	}

	// 快速路径：心跳 pong 响应（Method == "pong"，Channel 为空）
	if env.Method == "pong" {
		select {
		case w.pongCh <- struct{}{}:
		default:
		}
		return
	}

	// 频道消息分发（Channel 非空，Method 为空）
	switch env.Channel {
	case "l2Book":
		w.handleL2Book(env.Data)
	case "trades":
		w.handleTrades(env.Data)
	case "userFills":
		w.handleUserFills(env.Data)
	case "orderUpdates":
		w.handleOrderUpdates(env.Data)
	default:
		// 订阅确认或其他未知消息，静默忽略
		if env.Channel != "" {
			utils.Logger.Debug("未处理的频道", zap.String("channel", env.Channel))
		}
	}
}

// ---------------------------------------------------------------------------
// 频道处理器
// ---------------------------------------------------------------------------

// handleL2Book 处理 L2 盘口数据，提取最优买价作为最新价格
func (w *WSManager) handleL2Book(data json.RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("handleL2Book panic 恢复", zap.Any("recover", r))
		}
	}()

	var book wsL2BookData
	if err := json.Unmarshal(data, &book); err != nil {
		utils.Logger.Error("解析 l2Book 数据失败", zap.Error(err))
		return
	}

	// 过滤非目标交易对
	if book.Coin != w.cfg.Symbol {
		return
	}

	// 提取最优买价（bids[0].Px）作为最新价格
	if len(book.Levels) > 0 && len(book.Levels[0]) > 0 {
		bestBid := book.Levels[0][0].Px
		update := &PriceUpdate{
			Price:     bestBid,
			Timestamp: book.Time, // 使用 WS 推送的服务器时间（毫秒）
		}
		// 非阻塞写入价格缓冲 channel
		select {
		case w.priceEventCh <- update:
		default:
			// 通道满，丢弃本次更新（l2Book 更新频率高，下一帧很快到来）
		}
	}
}

// handleTrades 处理成交流数据，提取最新成交价
func (w *WSManager) handleTrades(data json.RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("handleTrades panic 恢复", zap.Any("recover", r))
		}
	}()

	var trades []wsTradeData
	if err := json.Unmarshal(data, &trades); err != nil {
		utils.Logger.Error("解析 trades 数据失败", zap.Error(err))
		return
	}

	// 取最后一笔成交价作为最新价格
	if len(trades) > 0 {
		lastTrade := trades[len(trades)-1]
		if lastTrade.Coin == w.cfg.Symbol {
			update := &PriceUpdate{
				Price:     lastTrade.Px,
				Timestamp: lastTrade.Time * 1000, // WS trades 时间为秒级，转为毫秒
			}
			select {
			case w.priceEventCh <- update:
			default:
			}
		}
	}
}

// handleUserFills 处理用户成交回报，转换为 OrderUpdate 事件
func (w *WSManager) handleUserFills(data json.RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("handleUserFills panic 恢复", zap.Any("recover", r))
		}
	}()

	var fillData wsUserFillData
	if err := json.Unmarshal(data, &fillData); err != nil {
		utils.Logger.Error("解析 userFills 数据失败", zap.Error(err))
		return
	}

	for _, fill := range fillData.Fills {
		// 过滤非目标交易对
		if fill.Coin != w.cfg.Symbol {
			continue
		}

		update := &OrderUpdate{
			OrderID:   fill.Oid,
			Symbol:    fill.Coin,
			ExecPrice: fill.Px,
			Quantity:  fill.Sz,
			Status:    "FILLED",
		}

		// 转换方向
		if fill.Side == "B" {
			update.Side = OrderSideBuy
		} else {
			update.Side = OrderSideSell
		}

		// 非阻塞写入订单事件缓冲 channel
		select {
		case w.orderEventCh <- update:
		default:
			utils.Logger.Warn("订单事件 channel 已满，丢弃事件",
				zap.Int64("oid", update.OrderID))
		}
	}
}

// handleOrderUpdates 处理订单状态更新
func (w *WSManager) handleOrderUpdates(data json.RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("handleOrderUpdates panic 恢复", zap.Any("recover", r))
		}
	}()

	// orderUpdates 数据格式为直接数组 [{...}]，不是 {"orders":[...]}。
	// 先尝试数组解析，失败再尝试包装对象解析（兼容两种格式）。
	var orders []wsOrderUpdate
	if err := json.Unmarshal(data, &orders); err != nil {
		var orderData wsOrderUpdateData
		if err := json.Unmarshal(data, &orderData); err != nil {
			utils.Logger.Error("解析 orderUpdates 数据失败", zap.Error(err))
			return
		}
		orders = orderData.Orders
	}

	for _, ou := range orders {
		// 过滤非目标交易对
		if ou.Order.Coin != w.cfg.Symbol {
			continue
		}

		update := &OrderUpdate{
			OrderID: ou.Order.Oid,
			Symbol:  ou.Order.Coin,
			Status:  ou.Status,
		}

		// 转换方向
		if ou.Order.Side == "B" {
			update.Side = OrderSideBuy
		} else {
			update.Side = OrderSideSell
		}

		// 解析价格和数量
		if ou.Order.LimitPx != "" {
			fmt.Sscanf(ou.Order.LimitPx, "%f", &update.ExecPrice)
		}
		if ou.Order.Sz != "" {
			fmt.Sscanf(ou.Order.Sz, "%f", &update.Quantity)
		}

		// 非阻塞写入
		select {
		case w.orderEventCh <- update:
		default:
			utils.Logger.Warn("订单事件 channel 已满，丢弃事件",
				zap.Int64("oid", update.OrderID))
		}
	}
}

// ---------------------------------------------------------------------------
// 事件分发：将缓冲 channel 中的事件桥接到 EventBus
// ---------------------------------------------------------------------------

// dispatchPriceEvents 从价格缓冲 channel 读取，发布为 EventTick
func (w *WSManager) dispatchPriceEvents() {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("dispatchPriceEvents panic 恢复", zap.Any("recover", r))
		}
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		case update := <-w.priceEventCh:
			w.bus.Publish(core.EventTick, update)
		}
	}
}

// dispatchOrderEvents 从订单事件缓冲 channel 读取，发布为 EventOrderUpdate
func (w *WSManager) dispatchOrderEvents() {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("dispatchOrderEvents panic 恢复", zap.Any("recover", r))
		}
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		case update := <-w.orderEventCh:
			w.bus.Publish(core.EventOrderUpdate, update)
		}
	}
}
