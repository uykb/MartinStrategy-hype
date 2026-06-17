// Package main 是马丁格尔策略交易机器人的入口。
//
// 重构说明：
//   - 交易所适配器由 BinanceClient 替换为 HyperliquidAdapter
//   - 价格获取由定时轮询升级为 WebSocket 为主、REST 为辅
//   - 事件总线架构保持不变，FSM 状态机完全透明
//   - 带缓冲的 Channel 架构确保高频行情推送不阻塞网络 I/O
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/uykb/MartinStrategy/internal/config"
	"github.com/uykb/MartinStrategy/internal/core"
	"github.com/uykb/MartinStrategy/internal/exchange"
	"github.com/uykb/MartinStrategy/internal/health"
	"github.com/uykb/MartinStrategy/internal/storage"
	"github.com/uykb/MartinStrategy/internal/strategy"
	"github.com/uykb/MartinStrategy/internal/utils"
	"go.uber.org/zap"
)

func main() {
	// ---------------------------------------------------------------
	// 1. 加载配置
	// ---------------------------------------------------------------
	// 配置文件 config.yaml 中的字段已适配 Hyperliquid：
	//   - api_key:     Agent 钱包私钥
	//   - api_secret:  主钱包地址
	//   - symbol:      交易对名称（如 "HYPE"，不带 USDT 后缀）
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		panic(err)
	}

	// ---------------------------------------------------------------
	// 2. 初始化日志
	// ---------------------------------------------------------------
	if err := utils.InitLogger(cfg.Log.Level); err != nil {
		panic(err)
	}
	defer utils.Logger.Sync()
	utils.Logger.Info("马丁格尔策略机器人启动",
		zap.String("symbol", cfg.Exchange.Symbol),
		zap.Bool("testnet", cfg.Exchange.UseTestnet))

	// ---------------------------------------------------------------
	// 3. 初始化存储（SQLite + Redis）
	// ---------------------------------------------------------------
	db, err := storage.InitStorage(
		cfg.Storage.SqlitePath,
		cfg.Storage.RedisAddr,
		cfg.Storage.RedisPass,
		cfg.Storage.RedisDB,
	)
	if err != nil {
		utils.Logger.Fatal("存储初始化失败", zap.Error(err))
	}

	// ---------------------------------------------------------------
	// 4. 创建事件总线
	// ---------------------------------------------------------------
	// EventBus 是整个系统的消息中枢：
	//   - WebSocket 公有流 → EventTick → FSM handleTick
	//   - WebSocket 私有流 → EventOrderUpdate → FSM handleOrderUpdate
	//   - REST 对账 → EventPositionUpdate → FSM 状态校准
	bus := core.NewEventBus()
	bus.Start()
	defer bus.Stop()

	// ---------------------------------------------------------------
	// 5. 创建 Hyperliquid 适配器
	// ---------------------------------------------------------------
	// ★ 核心重构：由 BinanceClient 替换为 HyperliquidAdapter
	// HyperliquidAdapter 内部管理：
	//   - REST API 客户端（下单、查询持仓/余额/K线）
	//   - WebSocket 管理器（双通道订阅 + 三层稳定性防线）
	//   - 5 位有效数字价格截断
	//   - Agent 钱包 EIP-712 签名
	ex, err := exchange.NewHyperliquidAdapter(&cfg.Exchange, bus)
	if err != nil {
		utils.Logger.Fatal("Hyperliquid 适配器创建失败", zap.Error(err))
	}

	// 启动适配器（WebSocket 连接 + 心跳 + 事件桥接）
	if err := ex.Start(context.Background()); err != nil {
		utils.Logger.Fatal("Hyperliquid 适配器启动失败", zap.Error(err))
	}

	// ---------------------------------------------------------------
	// 6. 创建并启动策略
	// ---------------------------------------------------------------
	// ★ 策略仅依赖 ExchangeAdapter 接口，对底层交易所完全透明
	// FSM 状态转移逻辑未做任何修改：
	//   IDLE → PLACING_GRID → IN_POSITION → IDLE
	strat := strategy.NewMartingaleStrategy(&cfg.Strategy, ex, db, bus)
	go strat.Start()

	// ---------------------------------------------------------------
	// 6.5 启动健康检查 HTTP 服务器
	// ---------------------------------------------------------------
	// ★ P2 加固：提供 /healthz（liveness）和 /readyz（readiness）端点
	// readiness 探针检查 WS 连接状态 + FSM 是否冻结
	healthAddr := ":8080" // 默认健康检查端口
	if cfg.Health != nil && cfg.Health.Addr != "" {
		healthAddr = cfg.Health.Addr
	}
	healthSrv := health.NewHealthServer(healthAddr, ex, strat)
	healthSrv.Start()
	defer healthSrv.Stop()

	// ---------------------------------------------------------------
	// 7. 等待关闭信号
	// ---------------------------------------------------------------
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	utils.Logger.Info("正在关闭...")

	// ---------------------------------------------------------------
	// 8. 优雅关闭
	// ---------------------------------------------------------------
	// 停止策略（取消 context，停止监控 goroutine）
	strat.Stop()

	// 停止适配器（断开 WebSocket、取消心跳）
	if err := ex.Stop(); err != nil {
		utils.Logger.Error("适配器关闭失败", zap.Error(err))
	}

	// 取消所有挂单（安全退出）
	if err := ex.CancelAllOrders(); err != nil {
		utils.Logger.Error("关闭时取消挂单失败", zap.Error(err))
	} else {
		utils.Logger.Info("所有挂单已取消")
	}

	utils.Logger.Info("关闭完成")
}
