# MartinStrategy-Hype

基于 Go 原生高并发架构的马丁格尔永续合约交易机器人，采用 **事件驱动 + 有限状态机 (ED-FSM)** 架构，专为 **Hyperliquid** 永续合约设计，经生产级安全审计加固，支持 7×24 无人值守运行。

## 项目简介

MartinStrategy-Hype 是一个面向 Hyperliquid 永续合约市场的自动化马丁格尔网格策略机器人。核心设计理念：

- **Go 原生高并发**：零 CGO 依赖，纯 Go 实现，WebSocket 为主 + REST 降级的混合架构
- **ED-FSM 架构**：事件驱动 + 有限状态机，FSM 状态转移严格线性同步执行，杜绝竞态条件
- **三层稳定性防线**：主动心跳 → 断线重连 + 指数退避 → REST 对账冻结恢复
- **Hyperliquid 深度适配**：USDC 本位、5 位有效数字价格截断、Agent 钱包 EIP-712 签名、`szDecimals` 精度体系

## 架构总览

```
                         Hyperliquid Exchange
                        ┌─────────────────────┐
                        │  REST API  │  WS API │
                        └─────┬──────┴───┬────┘
                              │           │
                    ┌─────────▼───┐  ┌─────▼──────────┐
                    │ REST Client │  │   WSManager     │
                    │ (降级通道)   │  │                  │
                    │             │  │ ┌──────────────┐ │
                    │ GetPosition │  │ │ 3-Layer      │ │
                    │ GetBalance  │  │ │ Stability    │ │
                    │ GetKlines   │  │ │ ──────────── │ │
                    │ CreateOrder │  │ │ ① Heartbeat  │ │
                    │ CancelOrder │  │ │ ② Reconnect  │ │
                    │ OpenOrders  │  │ │ ③ REST Resync│ │
                    └──────┬──────┘  │ └──────────────┘ │
                           │         │                    │
                           │         │ ┌──────────────┐ │
              IsWSActive() │         │ │ Dual Channel  │ │
              ┌────────────┤         │ │ ──────────── │ │
              │ WS活跃时跳过│         │ │ priceEventCh │ │
              │ REST推送    │         │ │   (500 buf)  │ │
              └────────────┘         │ │ orderEventCh │ │
                                     │ │   (200 buf)  │ │
                                     │ └──────┬───────┘ │
                                     └────────┼─────────┘
                                              │
                              ┌───────────────┼───────────────┐
                              │               │               │
                     EventTick      EventOrderUpdate   EventResyncStart/End
                              │               │               │
                              ▼               ▼               ▼
                    ┌─────────────────────────────────────────────┐
                    │              EventBus (1000 buf)            │
                    │                                             │
                    │  ★ 严格顺序同步执行（杜绝 FSM 竞态）         │
                    │  ★ defer recover() + 5s 自愈重启            │
                    │  ★ atomic 限速日志（每 100 次丢弃记录 1 次） │
                    └──────────────────┬──────────────────────────┘
                                       │
                              Subscribe │
                                       ▼
                    ┌─────────────────────────────────────────────┐
                    │         MartingaleStrategy (FSM)             │
                    │                                             │
                    │   IDLE ──▶ PLACING_GRID ──▶ IN_POSITION ──┐ │
                    │      ▲                                  ◀─┘ │
                    │      └──────── TP 成交 / 手动平仓 ─────────┘│
                    │                                             │
                    │  ★ frozen atomic.Bool — 对账冻结期间暂停    │
                    │  ★ PriceUpdate.IsStale(2s) — 过期行情丢弃  │
                    │  ★ placeOrderWithRetry — 3次抖动退避重试    │
                    │  ★ ctx/cancel — 优雅停止                    │
                    └──────────────────┬──────────────────────────┘
                                       │
                              ┌────────┴────────┐
                              │   Storage Layer  │
                              │  SQLite + Redis  │
                              └─────────────────┘

                    ┌─────────────────────────────────────────────┐
                    │         Health Server (:8080)                │
                    │                                             │
                    │  GET /healthz  → liveness  (进程存活=200)    │
                    │  GET /readyz   → readiness (WS活跃+未冻结=200)│
                    └─────────────────────────────────────────────┘
```

## 生产级优化特性

### 1. FSM 事件绝对线性同步执行（对应审计 1.1）

EventBus handler 由并发 `go func()` 改为**严格顺序同步执行**。每个 handler 独立包裹 `defer recover()`，单个 handler panic 不影响其他 handler 和主循环。主循环自身也有 `defer recover()` + 5 秒自愈重启。

```go
// EventBus.process — 顺序同步执行，确保 FSM 状态转移的确定性
for _, handler := range handlers {
    func() {
        defer func() {
            if r := recover(); r != nil { /* zap 日志 */ }
        }()
        handler(eb.ctx, event)
    }()
}
```

### 2. 锁粒度精细化，大锁内剥离网络 I/O（对应审计 1.2 / 4.3）

- **`handleTick`**：状态检查和变更在 `mu.Lock()` 内完成，`enterLong()` 网络调用在锁外执行
- **`updateTP`**：先在 `RLock` 内读取 `isIdle` + `oldTPID`，释放锁后再调用 `fetchATR()` 网络请求
- **`placeGridOrders`**：`gridMu.TryLock()` 防并发，双重 `gridPlaced` 检查

### 3. 价格事件内置服务器毫秒级时间戳，超过 2 秒自动丢弃（对应审计 3.1）

```go
type PriceUpdate struct {
    Price     float64 // 最新价格
    Timestamp int64    // 毫秒时间戳（WS 服务器时间 / REST 本地时间）
}

func (p *PriceUpdate) IsStale(maxLatency time.Duration) bool {
    return time.Since(time.UnixMilli(p.Timestamp)) > maxLatency
}

// handleTick 中：
if priceUpdate.IsStale(2 * time.Second) {
    return nil // 丢弃过期行情
}
```

时间戳来源：l2Book 使用 `book.Time`（WS 服务器时间 ms），trades 使用 `trade.Time * 1000`，REST 降级使用 `time.Now().UnixMilli()`。

### 4. 每个 `go func()` 完整覆盖 `defer recover()`，Panic 后 5 秒自愈（对应审计 4.1 / 4.2）

所有常驻 goroutine 均有 panic 恢复 + 自愈机制：

| Goroutine | 自愈行为 |
|-----------|---------|
| `EventBus.Start()` | 主循环 panic → 5s 后重启事件循环 |
| `monitorPositionStatus()` | panic → 5s 后重启监控 |
| `waitForFillAndPlaceGrid()` | panic → 5s 后检查状态，若仍在 PLACING_GRID 则重试 |
| `safePlaceGridOrders()` | panic → 5s 后检查状态，若仍在 IN_POSITION 且未放置网格则重试 |
| `safeUpdateTP()` | panic → 5s 后检查状态，若仍在 IN_POSITION 则重试 |
| `WSManager.readLoop()` | panic → 触发重连 |
| `WSManager.heartbeatLoop()` | panic → 日志记录 |
| `WSManager.dispatchPriceEvents()` | panic → 日志记录 |
| `WSManager.dispatchOrderEvents()` | panic → 日志记录 |

### 5. 补仓网格下单支持最大 3 次带随机抖动的指数退避重试（对应审计 4.4）

```go
func (s *MartingaleStrategy) placeOrderWithRetry(
    side OrderSide, orderType OrderTypeKind, qty, price float64, level int,
) {
    const maxRetries = 3
    for attempt := 0; attempt < maxRetries; attempt++ {
        _, err := s.exchange.CreateOrder(side, orderType, qty, price)
        if err == nil { return }
        if attempt < maxRetries-1 {
            backoff := time.Duration(200*(1<<attempt)) * time.Millisecond
            jitter  := time.Duration(rand.Int63n(int64(backoff) / 2))
            time.Sleep(backoff + jitter)
        }
    }
}
```

退避序列：`200ms + jitter` → `400ms + jitter` → `800ms + jitter`（Go 1.25+ 自动种子 `math/rand`）。

### 6. 双向对账模式：WS 重连 / REST 降级时冻结 FSM（对应审计 1.5 / 4.5）

```
WS 断线 → triggerReconnect() → reconnectWithBackoff()
                                    │
                                    ▼
                              resyncViaREST()
                                    │
                          ┌─────────┴─────────┐
                          │                   │
                  Publish(EventResyncStart)   │
                  frozen.Store(true)          │
                          │                   │
                          ▼                   │
                    FSM 冻结：                │
                    handleTick → return nil   │
                    handleOrderUpdate → nil   │
                          │                  │
                          ▼                  │
                  REST 查询持仓 + 挂单 + 成交 │
                          │                  │
                          ▼                  │
                  Publish(EventResyncEnd)    │
                  frozen.Store(false)        │
                          │                  │
                          ▼                  │
                    FSM 解冻，恢复正常处理    │
```

REST 降级轮询同样检测 `IsWSActive()`：WS 活跃时跳过推送，避免重复行情。

## 交易所机制适配

### USDC 本位

Hyperliquid 永续合约使用 **USDC** 作为保证金资产（非 USDT）。`GetBalance()` 返回 USDC 账户价值，`calcMinNotional()` 基于 USDC 余额动态计算头仓。

### 5 位有效数字价格截断

Hyperliquid 严格要求下单价格最多 5 位有效数字 + 最多 `(6 - szDecimals)` 位小数。违反此规则将收到 `Invalid Price` 拒单。

```go
// 所有下单价格经过 RoundToSigFigs 处理
price = utils.RoundToSigFigs(price, 5, maxPriceDecimals)

// 示例：
//   102.3456 → 102.35  (5 sig figs, 2 max decimals)
//   0.00123456 → 0.0012346  (5 sig figs, 6 max decimals)
//   100000 → 100000  (整数价格始终合法)
```

### szDecimals 精度体系

```go
type SymbolInfo struct {
    SzDecimals       int     // Hyperliquid 专用：size 小数位数
    MaxPriceDecimals int     // = 6 - szDecimals（perps）
    StepSize         float64 // = 10^(-szDecimals)
    MinQty           float64 // = StepSize
    TickSize         float64 // = 10^(-maxPriceDecimals)
}
```

从 Hyperliquid `meta` API 自动获取并缓存，所有下单数量和价格均经过精度截断。

### 市价单模拟

Hyperliquid 无原生市价单，使用 IOC 限价单 + 5% 价格偏移模拟：

```go
case OrderTypeMarket:
    req.OrderType = hyperliquid.OrderType{
        Limit: &hyperliquid.LimitOrderType{Tif: hyperliquid.TifIoc},
    }
    if side == OrderSideBuy {
        req.Price = price * 1.05  // 高于当前价 5%
    } else {
        req.Price = price * 0.95  // 低于当前价 5%
    }
```

### Agent 钱包签名

仅使用 Agent 私钥进行 L1 签名，主钱包私钥安全隔离：

```go
privateKey, _ := crypto.HexToECDSA(cfg.PrivateKey)  // Agent 私钥
exchangeClient := hyperliquid.NewExchange(ctx, privateKey, apiURL, nil, "", accountAddress, nil, nil)
```

## 快速开始

### 前置条件

- Go 1.25+（项目使用 Go 1.25.4 编译）
- Hyperliquid 主网或测试网账户
- Agent 钱包私钥 + 主钱包地址

### 配置文件

创建 `config.yaml`：

```yaml
# Hyperliquid 交易所配置
exchange:
  # Agent 钱包私钥（十六进制，不含 0x 前缀）
  # ⚠️ 生产环境务必通过环境变量设置，不要明文写入配置文件
  api_key: ""
  # 主钱包地址（十六进制，含 0x 前缀）
  api_secret: ""
  # 交易对名称（注意：不带 USDT 后缀，如 "HYPE" 而非 "HYPEUSDT"）
  symbol: "HYPE"
  # 是否使用 Hyperliquid 测试网
  use_testnet: false

# 策略配置
strategy:
  # 最大网格层数（Fibonacci 序列：1,1,2,3,5,8,13,21）
  max_safety_orders: 8
  # ATR 计算周期
  atr_period: 14
  # 头仓金额 = 账户 USDC 余额 × base_ratio（不低于 10 USDC）
  base_ratio: 0.1

# 存储配置
storage:
  sqlite_path: "bot.db"
  redis_addr: "localhost:6379"
  redis_pass: ""
  redis_db: 0

# 日志配置
log:
  level: "info"

# 健康检查配置
# /healthz → liveness 探针（进程存活=200）
# /readyz  → readiness 探针（WS 活跃 + FSM 未冻结=200）
health:
  addr: ":8080"
```

### 环境变量覆盖

所有配置项均可通过 `MARTIN_` 前缀的环境变量覆盖：

```bash
export MARTIN_EXCHANGE_API_KEY="your_agent_private_key_hex"
export MARTIN_EXCHANGE_API_SECRET="0xyour_main_wallet_address"
export MARTIN_EXCHANGE_SYMBOL="HYPE"
export MARTIN_EXCHANGE_USE_TESTNET="true"
export MARTIN_STRATEGY_MAX_SAFETY_ORDERS="8"
export MARTIN_STRATEGY_BASE_RATIO="0.1"
export MARTIN_LOG_LEVEL="debug"
export MARTIN_HEALTH_ADDR=":9090"
```

### 编译与运行

```bash
# 安装依赖（中国大陆需设置代理）
export GOPROXY=https://goproxy.cn,direct
go mod tidy

# 方式一：直接运行
go run cmd/bot/main.go

# 方式二：编译后运行
go build -o bot cmd/bot/main.go
./bot

# 方式三：Docker Compose
docker-compose up -d
docker-compose logs -f
```

### 健康检查

```bash
# Liveness — 进程存活即返回 200
curl http://localhost:8080/healthz

# Readiness — WS 连接活跃 + FSM 未冻结才返回 200
curl http://localhost:8080/readyz
```

Kubernetes 探针配置示例：

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30

readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 15
  periodSeconds: 10
```

## 策略详解

### 状态机流转

```
┌──────────┐     Tick 事件      ┌───────────────┐
│  IDLE    │ ──────────────────▶│ PLACING_GRID  │
│ (空仓等待) │                    │  (等待底仓成交)  │
└──────────┘                    └───────┬───────┘
     ▲                                  │
     │                                  │ 检测到持仓
     │                                  ▼
     │                          ┌───────────────┐
     │     TP 成交 (SELL)       │ IN_POSITION   │
     └─────────────────────────│   (持仓中)     │
                               │               │
                               │  安全单成交    │
                               ▼               │
                         更新止盈单 (TP)        │
                               ▲               │
                               └───────────────┘
```

| 状态 | 说明 | 触发进入 | 触发离开 |
|------|------|----------|----------|
| `IDLE` | 空仓等待，可接收新 Tick | 启动 / TP 成交 / 手动平仓 | 收到 Tick 事件 |
| `PLACING_GRID` | 底仓已下，等待成交 | 市价买入底仓后 | 检测到持仓 |
| `IN_POSITION` | 持有仓位，网格单活跃 | 底仓成交后 | 止盈成交 / 手动平仓 |

### 网格间距设计

| 层级 | 间距计算 | 时间框架 | 说明 |
|------|----------|----------|------|
| 1 | ATR(30m) | 30 分钟 | 首层保护 |
| 2 | ATR(1h) | 1 小时 | 第二层保护 |
| 3 | ATR(2h) | 2 小时 | 中短期保护 |
| 4 | ATR(4h) | 4 小时 | 中期保护 |
| 5 | ATR(8h) | 8 小时 | 中长期保护 |
| 6 | ATR(12h) | 12 小时 | 长期保护 |
| 7 | ATR(1d) | 日线 | 长期保护 |
| 8 | ATR(1w) | 周线 | 最深层保护 |

> 间距为**相对上一层**的距离。ATR 获取失败时回退至入场价 × 1%。

### Fibonacci 加仓数量

| 层级 | Fibonacci 倍数 | 数量（unit=1） | 累计倍数 |
|------|----------------|----------------|----------|
| 1 | 1 | 1 | 1 |
| 2 | 1 | 1 | 2 |
| 3 | 2 | 2 | 4 |
| 4 | 3 | 3 | 7 |
| 5 | 5 | 5 | 12 |
| 6 | 8 | 8 | 20 |
| 7 | 13 | 13 | 33 |
| 8 | 21 | 21 | 54 |

### 止盈策略

- **计算基准**：当前持仓均价（EntryPrice）
- **止盈价格**：`avgPrice + ATR(30m)`
- **止盈数量**：全仓平出
- **更新时机**：每次安全单成交后重新计算并替换 TP 订单

## 目录结构

```
.
├── cmd/
│   └── bot/
│       └── main.go              # 程序入口，适配器装配与生命周期管理
├── internal/
│   ├── config/
│   │   └── config.go            # Viper 配置加载（YAML + 环境变量覆盖）
│   ├── core/
│   │   └── event_bus.go         # 事件总线（顺序同步执行 + panic 自愈）
│   ├── exchange/
│   │   ├── adapter.go           # ExchangeAdapter 接口 + 领域模型
│   │   ├── hyperliquid.go       # Hyperliquid 适配器（REST + Agent 签名）
│   │   └── ws_manager.go        # WSManager（三层稳定性 + 双 Channel + 单次反序列化）
│   ├── health/
│   │   └── health.go            # HTTP 健康检查（/healthz + /readyz）
│   ├── strategy/
│   │   └── strategy.go          # 马丁格尔 FSM（对账冻结 + panic 自愈 + 抖动退避重试）
│   ├── storage/
│   │   └── storage.go           # SQLite + Redis 分布式锁
│   └── utils/
│       ├── indicators.go        # ATR 技术指标
│       ├── logger.go            # Zap 结构化日志
│       └── price_rounder.go    # 5 位有效数字截断 + szDecimals 精度体系
├── config.yaml                  # 默认配置文件
├── Dockerfile                   # 多阶段 Docker 构建（纯 Go，零 CGO）
├── docker-compose.yml           # Docker Compose 编排
├── go.mod                       # Go 模块依赖
└── AGENTS.md                    # 开发者 / Agent 指南
```

## 核心模块说明

### EventBus (`internal/core/event_bus.go`)

| 特性 | 说明 |
|------|------|
| 事件类型 | `TICK`, `ORDER_UPDATE`, `POSITION_UPDATE`, `LOG`, `START`, `STOP`, `RESYNC_START`, `RESYNC_END` |
| 队列容量 | 1000（满时丢弃 + atomic 限速日志） |
| 处理方式 | **严格顺序同步执行**（杜绝 FSM 竞态） |
| 崩溃防护 | 主循环 `defer recover()` + 5s 自愈重启 |
| handler 防护 | 每个 handler 独立 `defer recover()` |

### WSManager (`internal/exchange/ws_manager.go`)

| 特性 | 说明 |
|------|------|
| 三层稳定性 | ① 心跳 30s + pong 超时 10s ② 断线重连 + 指数退避（最多 10 次） ③ REST 对账冻结 |
| 双 Channel | `priceEventCh`（500 buf）+ `orderEventCh`（200 buf） |
| 消息解析 | 单次 `json.Unmarshal` → `wsEnvelope`，消除双重解析 |
| WS 活跃检测 | `IsWSActive()` atomic.Bool，供 REST 降级和健康检查查询 |
| 对账冻结 | `resyncViaREST()` 发布 `EventResyncStart/End`，冻结 FSM |

### HyperliquidAdapter (`internal/exchange/hyperliquid.go`)

| 特性 | 说明 |
|------|------|
| REST 降级 | WS 不活跃时每 10s 轮询价格，活跃时跳过 |
| 市价单模拟 | IOC 限价单 + 5% 价格偏移 |
| 价格截断 | `RoundToSigFigs(price, 5, maxPriceDecimals)` |
| 数量截断 | `ToFixed(qty, szDecimals)` |
| Agent 签名 | `crypto.HexToECDSA` → `NewExchange(ctx, privateKey, ...)` |

### MartingaleStrategy (`internal/strategy/strategy.go`)

| 特性 | 说明 |
|------|------|
| 动态头仓 | `余额 × base_ratio`，不低于 10 USDC |
| 网格层级 | 最多 8 层 Fibonacci 序列 |
| 仓位监控 | 每 5s 检查，检测手动平仓自动重置 |
| 对账冻结 | `frozen atomic.Bool`，对账期间丢弃 tick 和 orderUpdate |
| 过期行情 | `PriceUpdate.IsStale(2s)` 丢弃超过 2 秒的陈旧价格 |
| 下单重试 | `placeOrderWithRetry` 3 次 + 抖动指数退避 |
| 优雅停止 | `ctx/cancel` + `Stop()` 方法 |

### Health Server (`internal/health/health.go`)

| 端点 | 用途 | 200 条件 |
|------|------|----------|
| `GET /healthz` | Liveness 探针 | 进程存活 |
| `GET /readyz` | Readiness 探针 | WS 活跃 + FSM 未冻结 |

## 技术栈

| 组件 | 技术 | 版本 |
|------|------|------|
| 语言 | Go | 1.25.4 |
| 交易所 SDK | go-hyperliquid | v0.37.0 |
| WebSocket | gorilla/websocket | v1.5.3 |
| 签名 | ethereum/go-ethereum | v1.17.3 |
| 存储 | SQLite (glebarez/sqlite) | v1.11.0 |
| 缓存/锁 | Redis (go-redis) | v9.18.0 |
| 配置 | Viper | v1.21.0 |
| 日志 | Zap | v1.27.1 |
| ORM | GORM | v1.31.1 |
| 技术指标 | go-talib | - |

## 风险提示

- 马丁格尔策略在单边下跌行情中风险极高，可能导致重大亏损
- 建议设置止损或限制最大持仓层数
- 请确保 Agent 钱包私钥安全，生产环境务必通过环境变量设置
- **强烈建议先在测试网（`use_testnet: true`）验证策略**
- 本软件仅供学习研究，不构成投资建议，使用风险自负

## License

MIT License