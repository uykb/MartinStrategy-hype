// Package exchange 定义了交易所通用适配器接口与领域模型，
// 使策略层（FSM）与底层交易所实现完全解耦。
// 任何新交易所只需实现 ExchangeAdapter 接口即可无缝接入。
package exchange

import (
	"context"
	"time"
)

// ---------------------------------------------------------------------------
// 订单方向与类型常量
// ---------------------------------------------------------------------------

// OrderSide 表示订单方向
type OrderSide string

const (
	OrderSideBuy  OrderSide = "BUY"
	OrderSideSell OrderSide = "SELL"
)

// OrderTypeKind 表示订单类型
type OrderTypeKind string

const (
	OrderTypeMarket OrderTypeKind = "MARKET"
	OrderTypeLimit  OrderTypeKind = "LIMIT"
)

// ---------------------------------------------------------------------------
// 领域模型 —— 与具体交易所无关的通用数据结构
// ---------------------------------------------------------------------------

// Position 表示一个交易对的持仓信息
type Position struct {
	Symbol        string  // 交易对名称（如 "BTC" 或 "HYPE"）
	Size          float64 // 持仓数量：正数=多仓，负数=空仓
	EntryPrice    float64 // 开仓均价
	UnrealizedPnl float64 // 未实现盈亏
	Leverage      int     // 杠杆倍数
	LiquidationPx float64 // 强平价格（0 表示无数据）
}

// OpenOrder 表示一个挂单（未成交订单）
type OpenOrder struct {
	OrderID  int64        // 交易所返回的订单 ID
	Side     OrderSide    // 买 / 卖
	Type     OrderTypeKind // 限价 / 市价
	Price    float64      // 挂单价格
	Quantity float64      // 挂单数量
	Symbol   string       // 交易对
}

// OrderResponse 表示下单后的交易所响应
type OrderResponse struct {
	OrderID int64  // 交易所分配的订单 ID
	Status  string // "resting" | "filled" | "error"
}

// OrderUpdate 表示订单状态变更事件（由 WebSocket 推送）
// 该结构体会被封装为 EventOrderUpdate 注入 FSM
type OrderUpdate struct {
	OrderID   int64        // 订单 ID
	Symbol    string       // 交易对
	Side      OrderSide    // 买 / 卖
	Type      OrderTypeKind // 限价 / 市价
	Status    string       // "FILLED" | "CANCELED" | "PARTIALLY_FILLED" 等
	ExecPrice float64      // 成交均价
	Quantity  float64      // 成交数量
}

// PriceUpdate 表示带时间戳的价格更新事件。
// 用于防止过期行情触发 FSM 状态转移（防滑点机制）。
// Timestamp 来源于 WebSocket 推送的服务器时间（毫秒），
// 在 REST 降级模式下使用本地时间。
type PriceUpdate struct {
	Price     float64 // 最新价格
	Timestamp int64    // 毫秒时间戳（来自 WS 服务器时间或本地时间）
}

// IsStale 判断价格更新是否已过期。
// maxLatency 为最大允许延迟（如 2 秒）。
func (p *PriceUpdate) IsStale(maxLatency time.Duration) bool {
	eventTime := time.UnixMilli(p.Timestamp)
	return time.Since(eventTime) > maxLatency
}

// Candle 表示一根 K 线数据
type Candle struct {
	OpenTime int64   // 开盘时间（毫秒时间戳）
	Open     float64 // 开盘价
	High     float64 // 最高价
	Low      float64 // 最低价
	Close    float64 // 收盘价
	Volume   float64 // 成交量
}

// SymbolInfo 表示交易对的精度与限制信息
type SymbolInfo struct {
	QuantityPrecision int     // 数量小数位数
	PricePrecision    int     // 价格小数位数
	MinQty            float64 // 最小下单数量
	StepSize          float64 // 数量步长（如 0.01）
	TickSize          float64 // 价格步长（如 0.01）
	SzDecimals        int     // Hyperliquid 专用：size 的小数位数
	MaxPriceDecimals  int     // Hyperliquid 专用：价格最大小数位 = 6 - szDecimals
}

// ---------------------------------------------------------------------------
// ExchangeAdapter —— 交易所通用适配器接口
// ---------------------------------------------------------------------------
// FSM 状态机仅依赖此接口，不直接引用任何交易所 SDK 类型。
// 新交易所只需实现此接口即可无缝接入马丁格尔策略。

type ExchangeAdapter interface {
	// ---- 生命周期 ----

	// Start 启动适配器（WebSocket 连接、心跳、事件桥接等）
	Start(ctx context.Context) error

	// Stop 优雅关闭适配器（断开 WebSocket、取消挂单等）
	Stop() error

	// ---- 行情 ----

	// GetLatestPrice 通过 REST 获取最新成交价
	GetLatestPrice() (float64, error)

	// GetKlines 获取指定周期的 K 线数据
	GetKlines(interval string, limit int) ([]Candle, error)

	// ---- 账户 ----

	// GetPosition 获取当前交易对的持仓信息
	GetPosition() (*Position, error)

	// GetBalance 获取账户可用余额（Hyperliquid 为 USDC）
	GetBalance() (float64, error)

	// ---- 订单 ----

	// CreateOrder 创建订单（市价 / 限价）
	CreateOrder(side OrderSide, orderType OrderTypeKind, quantity, price float64) (*OrderResponse, error)

	// CancelOrder 取消指定订单
	CancelOrder(orderID int64) error

	// CancelAllOrders 取消当前交易对的所有挂单
	CancelAllOrders() error

	// GetOpenOrders 获取当前交易对的所有未成交订单
	GetOpenOrders() ([]OpenOrder, error)

	// ---- 交易对信息 ----

	// GetSymbol 返回当前配置的交易对名称
	GetSymbol() string

	// GetSymbolInfo 返回交易对的精度与限制信息
	GetSymbolInfo() (*SymbolInfo, error)
}
