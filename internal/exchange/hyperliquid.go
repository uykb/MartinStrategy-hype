// Package exchange 实现 Hyperliquid 交易所适配器。
//
// HyperliquidAdapter 实现了 ExchangeAdapter 接口，将 Hyperliquid 的
// REST API / WebSocket / EIP-712 签名机制封装为统一的领域模型。
//
// 关键设计：
//   - 资产适配：全仓保证金资产由 USDT 改为 USDC
//   - 5 位有效数字截断：所有下单价格经过 RoundToSigFigs 处理
//   - Agent 钱包签名：仅使用 Agent 私钥进行 L1 签名，主钱包私钥安全
//   - WebSocket 为主、REST 为辅的混合架构
package exchange

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	hyperliquid "github.com/sonirico/go-hyperliquid"
	"github.com/uykb/MartinStrategy/internal/config"
	"github.com/uykb/MartinStrategy/internal/core"
	"github.com/uykb/MartinStrategy/internal/utils"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// Hyperliquid 专属配置
// ---------------------------------------------------------------------------

// HyperliquidConfig 封装 Hyperliquid 交易所所需的全部配置参数
type HyperliquidConfig struct {
	// API 端点
	APIURL string // REST API 地址（主网: https://api.hyperliquid.xyz）
	WSURL  string // WebSocket 地址（主网: wss://api.hyperliquid.xyz/ws）

	// 鉴权参数
	PrivateKey     string // Agent 钱包私钥（十六进制，不含 0x 前缀）
	AccountAddress string // 主钱包地址（十六进制，含 0x 前缀）

	// 交易参数
	Symbol     string // 交易对名称（如 "HYPE"，注意 Hyperliquid 不带 USDT 后缀）
	UseTestnet bool   // 是否使用测试网

	// WebSocket 心跳与重连参数
	PingInterval   time.Duration // 心跳间隔（默认 30s）
	MaxReconnect   int           // 最大重连次数（默认 10）
	InitialBackoff time.Duration // 初始退避时间（默认 2s）
}

// NewHyperliquidConfig 从通用 ExchangeConfig 创建 Hyperliquid 专属配置
func NewHyperliquidConfig(cfg *config.ExchangeConfig) *HyperliquidConfig {
	hlCfg := &HyperliquidConfig{
		PrivateKey:     cfg.ApiKey,    // 复用 ApiKey 字段存储 Agent 私钥
		AccountAddress: cfg.ApiSecret, // 复用 ApiSecret 字段存储主钱包地址
		Symbol:         cfg.Symbol,
		UseTestnet:     cfg.UseTestnet,
		PingInterval:   30 * time.Second,
		MaxReconnect:   10,
		InitialBackoff: 2 * time.Second,
	}

	// 根据网络环境设置端点
	if cfg.UseTestnet {
		hlCfg.APIURL = hyperliquid.TestnetAPIURL
		hlCfg.WSURL = "wss://api.hyperliquid-testnet.xyz/ws"
	} else {
		hlCfg.APIURL = hyperliquid.MainnetAPIURL
		hlCfg.WSURL = "wss://api.hyperliquid.xyz/ws"
	}

	return hlCfg
}

// ---------------------------------------------------------------------------
// HyperliquidAdapter 核心结构
// ---------------------------------------------------------------------------

// HyperliquidAdapter 实现 ExchangeAdapter 接口，
// 封装 Hyperliquid 的 REST API 调用与 WebSocket 事件桥接。
type HyperliquidAdapter struct {
	cfg *HyperliquidConfig
	bus *core.EventBus

	// SDK 客户端
	exchangeClient *hyperliquid.Exchange // 交易客户端（下单、撤单）
	infoClient     *hyperliquid.Info     // 查询客户端（行情、持仓、挂单）

	// WebSocket 管理器
	wsManager *WSManager

	// 交易对元数据缓存
	symbolInfo   *SymbolInfo
	symbolInfoMu sync.RWMutex

	// 上下文控制
	ctx    context.Context
	cancel context.CancelFunc
}

// NewHyperliquidAdapter 创建 Hyperliquid 适配器实例
func NewHyperliquidAdapter(cfg *config.ExchangeConfig, bus *core.EventBus) (*HyperliquidAdapter, error) {
	hlCfg := NewHyperliquidConfig(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	// 诊断日志：打印密钥长度（不打印内容，保证安全）
	utils.Logger.Info("Hyperliquid 适配器初始化",
		zap.String("symbol", hlCfg.Symbol),
		zap.Bool("testnet", hlCfg.UseTestnet),
		zap.Int("api_key_length", len(hlCfg.PrivateKey)),
		zap.Int("api_secret_length", len(hlCfg.AccountAddress)))

	// 解析 Agent 私钥为 *ecdsa.PrivateKey（兼容 0x 前缀）
	pkHex := strings.TrimPrefix(hlCfg.PrivateKey, "0x")
	if pkHex == "" {
		cancel()
		return nil, fmt.Errorf("Agent 私钥为空（环境变量 MARTIN_EXCHANGE_API_KEY 未设置或为空），需要 64 位十六进制字符串，不含 0x 前缀")
	}
	if len(pkHex) != 64 {
		cancel()
		return nil, fmt.Errorf("Agent 私钥长度无效: 需要 64 个十六进制字符（256 bits），实际 %d 个字符（含0x前缀=%d）。请检查 MARTIN_EXCHANGE_API_KEY 环境变量", len(pkHex), len(hlCfg.PrivateKey))
	}
	privateKey, err := crypto.HexToECDSA(pkHex)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("解析 Agent 私钥失败: %w（请确认 MARTIN_EXCHANGE_API_KEY 为有效的 64 位十六进制私钥）", err)
	}

	// 初始化交易客户端（使用 Agent 私钥进行 L1 签名）
	exchangeClient := hyperliquid.NewExchange(
		ctx,
		privateKey,
		hlCfg.APIURL,
		nil,                   // meta: 自动获取
		"",                    // vault 地址：空表示主账户
		hlCfg.AccountAddress,  // 主钱包地址
		nil,                   // spotMeta: 自动获取
		nil,                   // perpDexs: 自动获取
	)

	// 初始化查询客户端
	infoClient := hyperliquid.NewInfo(
		ctx,
		hlCfg.APIURL,
		true, // skipWS: 我们使用自己的 WSManager
		nil,  // meta: 自动获取
		nil,  // spotMeta: 自动获取
		nil,  // perpDexs: 自动获取
	)

	adapter := &HyperliquidAdapter{
		cfg:            hlCfg,
		bus:            bus,
		exchangeClient: exchangeClient,
		infoClient:     infoClient,
		ctx:            ctx,
		cancel:         cancel,
	}

	// 初始化 WebSocket 管理器
	adapter.wsManager = NewWSManager(hlCfg, bus, adapter)

	return adapter, nil
}

// ---------------------------------------------------------------------------
// ExchangeAdapter 接口实现
// ---------------------------------------------------------------------------

// Start 启动适配器：初始化交易对信息 + 启动 WebSocket
func (h *HyperliquidAdapter) Start(ctx context.Context) error {
	// 1. 初始化交易对精度信息（从 Hyperliquid meta 获取）
	if err := h.initSymbolInfo(); err != nil {
		return fmt.Errorf("初始化交易对信息失败: %w", err)
	}

	utils.Logger.Info("Hyperliquid 适配器启动",
		zap.String("symbol", h.cfg.Symbol),
		zap.String("api_url", h.cfg.APIURL),
		zap.String("ws_url", h.cfg.WSURL))

	// 2. 启动 WebSocket 管理器
	if err := h.wsManager.Start(); err != nil {
		return fmt.Errorf("启动 WebSocket 管理器失败: %w", err)
	}

	// 3. 启动 REST 轮询降级（当 WebSocket 不可用时，每 10 秒轮询一次价格）
	go h.restPriceFallback()

	return nil
}

// Stop 优雅关闭适配器
func (h *HyperliquidAdapter) Stop() error {
	utils.Logger.Info("Hyperliquid 适配器正在关闭...")

	// 停止 WebSocket 管理器
	h.wsManager.Stop()

	// 取消上下文
	h.cancel()

	utils.Logger.Info("Hyperliquid 适配器已关闭")
	return nil
}

// GetLatestPrice 通过 REST API 获取最新价格
func (h *HyperliquidAdapter) GetLatestPrice() (float64, error) {
	mids, err := h.infoClient.AllMids(h.ctx)
	if err != nil {
		return 0, fmt.Errorf("获取最新价格失败: %w", err)
	}

	priceStr, ok := mids[h.cfg.Symbol]
	if !ok {
		return 0, fmt.Errorf("交易对 %s 不存在于价格列表中", h.cfg.Symbol)
	}

	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		return 0, fmt.Errorf("解析价格失败 (%s): %w", priceStr, err)
	}

	return price, nil
}

// GetKlines 获取 K 线数据
func (h *HyperliquidAdapter) GetKlines(interval string, limit int) ([]Candle, error) {
	// 计算时间范围
	endTime := time.Now().UnixMilli()
	// 根据周期估算起始时间
	duration := intervalToDuration(interval)
	startTime := endTime - int64(limit)*duration.Milliseconds()

	candles, err := h.infoClient.CandlesSnapshot(
		h.ctx,
		h.cfg.Symbol,
		interval,
		startTime,
		endTime,
	)
	if err != nil {
		return nil, fmt.Errorf("获取 K 线数据失败 (%s): %w", interval, err)
	}

	// 转换为通用 Candle 类型
	result := make([]Candle, 0, len(candles))
	for _, c := range candles {
		open, _ := strconv.ParseFloat(c.Open, 64)
		high, _ := strconv.ParseFloat(c.High, 64)
		low, _ := strconv.ParseFloat(c.Low, 64)
		close_, _ := strconv.ParseFloat(c.Close, 64)
		vol, _ := strconv.ParseFloat(c.Volume, 64)

		result = append(result, Candle{
			OpenTime: c.TimeOpen,
			Open:     open,
			High:     high,
			Low:      low,
			Close:    close_,
			Volume:   vol,
		})
	}

	return result, nil
}

// GetPosition 获取当前交易对的持仓信息
func (h *HyperliquidAdapter) GetPosition() (*Position, error) {
	userState, err := h.infoClient.UserState(h.ctx, h.cfg.AccountAddress)
	if err != nil {
		return nil, fmt.Errorf("获取用户状态失败: %w", err)
	}

	// 在所有持仓中查找目标交易对
	for _, ap := range userState.AssetPositions {
		if ap.Position.Coin == h.cfg.Symbol {
			pos := ap.Position

			// 解析持仓数量（szi: 正数=多仓，负数=空仓）
			size, _ := strconv.ParseFloat(pos.Szi, 64)

			// 解析开仓均价
			var entryPrice float64
			if pos.EntryPx != nil {
				entryPrice, _ = strconv.ParseFloat(*pos.EntryPx, 64)
			}

			// 解析未实现盈亏
			unrealizedPnl, _ := strconv.ParseFloat(pos.UnrealizedPnl, 64)

			// 解析强平价格
			var liqPx float64
			if pos.LiquidationPx != nil {
				liqPx, _ = strconv.ParseFloat(*pos.LiquidationPx, 64)
			}

			// 解析杠杆倍数
			leverage := pos.Leverage.Value

			return &Position{
				Symbol:        pos.Coin,
				Size:          size,
				EntryPrice:    entryPrice,
				UnrealizedPnl: unrealizedPnl,
				Leverage:      leverage,
				LiquidationPx: liqPx,
			}, nil
		}
	}

	// 无持仓：返回零值 Position
	return &Position{
		Symbol: h.cfg.Symbol,
		Size:   0,
	}, nil
}

// GetBalance 获取账户 USDC 余额
func (h *HyperliquidAdapter) GetBalance() (float64, error) {
	userState, err := h.infoClient.UserState(h.ctx, h.cfg.AccountAddress)
	if err != nil {
		return 0, fmt.Errorf("获取用户状态失败: %w", err)
	}

	// Hyperliquid 使用 USDC 作为保证金
	// AccountValue 包含未实现盈亏的总账户价值
	accountValue, _ := strconv.ParseFloat(userState.MarginSummary.AccountValue, 64)

	return accountValue, nil
}

// CreateOrder 创建订单
func (h *HyperliquidAdapter) CreateOrder(
	side OrderSide,
	orderType OrderTypeKind,
	quantity, price float64,
) (*OrderResponse, error) {

	// 获取交易对精度信息
	info := h.getSymbolInfo()

	// 价格截断：Hyperliquid 严格要求 5 位有效数字
	if info != nil && orderType == OrderTypeLimit {
		price = utils.RoundToSigFigs(price, 5, info.MaxPriceDecimals)
	}

	// 数量精度截断
	if info != nil {
		quantity = utils.ToFixed(quantity, info.SzDecimals)
	}

	// 构建 SDK 订单请求
	req := hyperliquid.CreateOrderRequest{
		Coin:       h.cfg.Symbol,
		IsBuy:      side == OrderSideBuy,
		Size:       quantity,
		Price:      price,
		ReduceOnly: false,
	}

	// 设置订单类型
	switch orderType {
	case OrderTypeLimit:
		req.OrderType = hyperliquid.OrderType{
			Limit: &hyperliquid.LimitOrderType{
				Tif: hyperliquid.TifGtc, // Good Till Cancel
			},
		}
	case OrderTypeMarket:
		// Hyperliquid 没有原生市价单，使用 IOC 限价单模拟
		// 以极端价格挂单，确保立即成交
		req.OrderType = hyperliquid.OrderType{
			Limit: &hyperliquid.LimitOrderType{
				Tif: hyperliquid.TifIoc, // Immediate Or Cancel
			},
		}
		// 市价买入：价格设为极高值确保成交
		if side == OrderSideBuy {
			req.Price = price * 1.05 // 高于当前价 5%
		} else {
			req.Price = price * 0.95 // 低于当前价 5%
		}
		// 再次截断市价单价格
		if info != nil {
			req.Price = utils.RoundToSigFigs(req.Price, 5, info.MaxPriceDecimals)
		}
	}

	// 发送订单（第二个参数为 BuilderInfo，传 nil 表示不使用 builder）
	status, err := h.exchangeClient.Order(h.ctx, req, nil)
	if err != nil {
		return nil, fmt.Errorf("下单失败: %w", err)
	}

	// 解析响应（OrderStatus 直接返回，不是嵌套在 APIResponse 中）
	orderResp := &OrderResponse{Status: "unknown"}

	if status.Resting != nil {
		orderResp.OrderID = status.Resting.Oid
		orderResp.Status = "resting"
	} else if status.Filled != nil {
		orderResp.OrderID = int64(status.Filled.Oid)
		orderResp.Status = "filled"
	} else if status.Error != nil {
		orderResp.Status = "error"
		return orderResp, fmt.Errorf("下单被交易所拒绝: %s", *status.Error)
	}

	utils.Logger.Info("订单已提交",
		zap.String("side", string(side)),
		zap.String("type", string(orderType)),
		zap.Float64("price", price),
		zap.Float64("quantity", quantity),
		zap.Int64("order_id", orderResp.OrderID),
		zap.String("status", orderResp.Status))

	return orderResp, nil
}

// CancelOrder 取消指定订单
func (h *HyperliquidAdapter) CancelOrder(orderID int64) error {
	_, err := h.exchangeClient.Cancel(h.ctx, h.cfg.Symbol, orderID)
	if err != nil {
		return fmt.Errorf("取消订单失败 (oid=%d): %w", orderID, err)
	}

	utils.Logger.Info("订单已取消", zap.Int64("order_id", orderID))
	return nil
}

// CancelAllOrders 取消当前交易对的所有挂单
func (h *HyperliquidAdapter) CancelAllOrders() error {
	// 先获取所有挂单
	orders, err := h.GetOpenOrders()
	if err != nil {
		return fmt.Errorf("获取挂单列表失败: %w", err)
	}

	if len(orders) == 0 {
		utils.Logger.Info("没有需要取消的挂单")
		return nil
	}

	// 逐个取消（Hyperliquid SDK 支持 BulkCancel，但逐个取消更安全）
	var lastErr error
	cancelledCount := 0
	for _, o := range orders {
		if err := h.CancelOrder(o.OrderID); err != nil {
			utils.Logger.Error("取消订单失败",
				zap.Int64("order_id", o.OrderID),
				zap.Error(err))
			lastErr = err
		} else {
			cancelledCount++
		}
		// 避免 API 限流
		time.Sleep(100 * time.Millisecond)
	}

	utils.Logger.Info("批量取消订单完成",
		zap.Int("total", len(orders)),
		zap.Int("cancelled", cancelledCount))

	return lastErr
}

// GetOpenOrders 获取当前交易对的所有未成交订单
func (h *HyperliquidAdapter) GetOpenOrders() ([]OpenOrder, error) {
	orders, err := h.infoClient.OpenOrders(h.ctx, h.cfg.AccountAddress)
	if err != nil {
		return nil, fmt.Errorf("获取挂单列表失败: %w", err)
	}

	result := make([]OpenOrder, 0)
	for _, o := range orders {
		// 过滤非目标交易对
		if o.Coin != h.cfg.Symbol {
			continue
		}

		// SDK 的 OpenOrder.LimitPx 和 Size 已经是 float64 类型
		side := OrderSideBuy
		if o.Side == "S" {
			side = OrderSideSell
		}

		// Hyperliquid OpenOrder 没有独立的 OrderType 字段，
		// 所有通过 OpenOrders 返回的都是限价挂单
		orderType := OrderTypeLimit

		result = append(result, OpenOrder{
			OrderID:  o.Oid,
			Side:     side,
			Type:     orderType,
			Price:    o.LimitPx, // float64，无需解析
			Quantity: o.Size,    // float64，无需解析
			Symbol:   o.Coin,
		})
	}

	return result, nil
}

// GetSymbol 返回当前配置的交易对名称
func (h *HyperliquidAdapter) GetSymbol() string {
	return h.cfg.Symbol
}

// IsWSActive 返回 WebSocket 连接是否活跃（供健康检查查询）
func (h *HyperliquidAdapter) IsWSActive() bool {
	return h.wsManager.IsWSActive()
}

// GetSymbolInfo 返回交易对的精度与限制信息
func (h *HyperliquidAdapter) GetSymbolInfo() (*SymbolInfo, error) {
	h.symbolInfoMu.RLock()
	info := h.symbolInfo
	h.symbolInfoMu.RUnlock()

	if info != nil {
		return info, nil
	}

	// 缓存未命中，重新获取
	if err := h.initSymbolInfo(); err != nil {
		return nil, err
	}

	h.symbolInfoMu.RLock()
	info = h.symbolInfo
	h.symbolInfoMu.RUnlock()

	return info, nil
}

// ---------------------------------------------------------------------------
// 内部方法
// ---------------------------------------------------------------------------

// initSymbolInfo 从 Hyperliquid meta API 获取交易对精度信息
func (h *HyperliquidAdapter) initSymbolInfo() error {
	meta, err := h.infoClient.Meta(h.ctx)
	if err != nil {
		return fmt.Errorf("获取交易所元数据失败: %w", err)
	}

	// 在 universe 中查找目标交易对
	found := false
	info := &SymbolInfo{}

	for _, asset := range meta.Universe {
		if asset.Name == h.cfg.Symbol {
			found = true
			info.SzDecimals = asset.SzDecimals

			// 计算 Hyperliquid 专有精度参数
			info.MaxPriceDecimals = utils.CalcMaxPriceDecimals(asset.SzDecimals, false)

			// 从 Hyperliquid 的 szDecimals 推算精度
			info.PricePrecision = info.MaxPriceDecimals
			info.QuantityPrecision = asset.SzDecimals

			// 步长和最小数量：Hyperliquid 的最小下单量由 szDecimals 决定
			info.StepSize = math.Pow(10, float64(-asset.SzDecimals))
			info.MinQty = info.StepSize

			// TickSize：根据价格精度推算
			info.TickSize = math.Pow(10, float64(-info.MaxPriceDecimals))

			break
		}
	}

	if !found {
		return fmt.Errorf("交易对 %s 不存在于 Hyperliquid 元数据中", h.cfg.Symbol)
	}

	// 缓存
	h.symbolInfoMu.Lock()
	h.symbolInfo = info
	h.symbolInfoMu.Unlock()

	utils.Logger.Info("交易对信息初始化完成",
		zap.String("symbol", h.cfg.Symbol),
		zap.Int("sz_decimals", info.SzDecimals),
		zap.Int("max_price_decimals", info.MaxPriceDecimals),
		zap.Int("price_precision", info.PricePrecision),
		zap.Int("qty_precision", info.QuantityPrecision),
		zap.Float64("step_size", info.StepSize),
		zap.Float64("tick_size", info.TickSize),
		zap.Float64("min_qty", info.MinQty))

	return nil
}

// getSymbolInfo 获取缓存的交易对信息
func (h *HyperliquidAdapter) getSymbolInfo() *SymbolInfo {
	h.symbolInfoMu.RLock()
	defer h.symbolInfoMu.RUnlock()
	return h.symbolInfo
}

// restPriceFallback REST 轮询降级：当 WebSocket 不可用时，定期查询价格
// ★ P2 加固：检测 WS 活跃状态，WS 正常时跳过 REST 推送，避免重复行情
func (h *HyperliquidAdapter) restPriceFallback() {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("restPriceFallback panic 恢复", zap.Any("recover", r))
		}
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			// ★ P2 加固：WS 活跃时跳过 REST 推送，避免重复行情干扰 FSM
			if h.wsManager.IsWSActive() {
				continue
			}

			price, err := h.GetLatestPrice()
			if err != nil {
				utils.Logger.Error("REST 轮询获取价格失败", zap.Error(err))
				continue
			}
			// REST 降级作为补充，携带本地时间戳
			h.bus.Publish(core.EventTick, &PriceUpdate{
				Price:     price,
				Timestamp: time.Now().UnixMilli(),
			})
			utils.Logger.Debug("REST 降级推送价格", zap.Float64("price", price))
		}
	}
}

// userFillsParams 构建 SDK 的 UserFillsParams 请求参数
func (h *HyperliquidAdapter) userFillsParams() hyperliquid.UserFillsParams {
	return hyperliquid.UserFillsParams{
		Address: h.cfg.AccountAddress,
	}
}

// ---------------------------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------------------------

// intervalToDuration 将 K 线周期字符串转换为时间间隔
func intervalToDuration(interval string) time.Duration {
	switch interval {
	case "1m":
		return time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return time.Hour
	case "2h":
		return 2 * time.Hour
	case "4h":
		return 4 * time.Hour
	case "8h":
		return 8 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "1d":
		return 24 * time.Hour
	case "3d":
		return 3 * 24 * time.Hour
	case "1w":
		return 7 * 24 * time.Hour
	case "1M":
		return 30 * 24 * time.Hour
	default:
		return time.Hour // 默认 1 小时
	}
}
