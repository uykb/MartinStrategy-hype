# MartinStrategy-Hype

基于 Go 原生高并发架构的马丁格尔永续合约交易机器人，采用 **事件驱动 + 有限状态机 (ED-FSM)** 架构，专为 **Hyperliquid** 永续合约设计，经生产级安全审计加固，支持 7×24 无人值守运行。

## 项目简介

MartinStrategy-Hype 是一个面向 Hyperliquid 永续合约市场的自动化马丁格尔网格策略机器人。核心设计理念：

- **Go 原生高并发**：零 CGO 依赖，纯 Go 实现，WebSocket 为主 + REST 降级的混合架构
- **ED-FSM 架构**：事件驱动 + 有限状态机，FSM 状态转移严格线性同步执行，杜绝竞态条件
- **三层稳定性防线**：主动心跳 → 断线重连 + 指数退避 → REST 对账冻结恢复
- **Hyperliquid 深度适配**：USDC 本位、5 位有效数字价格截断、Agent 钱包 EIP-712 签名、`szDecimals` 精度体系、统一账户余额查询

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
                     │  ★ initialSyncDone — 启动时过滤历史成交     │
                     │  ★ PriceUpdate.IsStale(2s) — 过期行情丢弃  │
                     │  ★ placeOrderWithRetry — 3次抖动退避重试    │
                     │  ★ FloorToDecimals — 数量严格向下取整       │
                     └──────────────────┬──────────────────────────┘
                                        │
                               ┌────────┴────────┐
                               │   Storage Layer  │
                               │  SQLite + Redis  │
                               └─────────────────┘
```

## 快速开始

### 前置条件

- Go 1.25+
- Hyperliquid 主网账户（统一账户或标准账户均可）
- Agent 钱包私钥 + 主钱包地址

### 配置

通过环境变量配置（推荐生产环境使用），或使用 `config.yaml` 配置文件。

**环境变量方式（推荐）：**

```bash
export MARTIN_EXCHANGE_API_KEY="你的Agent钱包私钥_64位十六进制_不含0x"
export MARTIN_EXCHANGE_API_SECRET="0x你的主钱包地址"
export MARTIN_EXCHANGE_SYMBOL="HYPE"
export MARTIN_EXCHANGE_USE_TESTNET="false"
```

**配置文件方式：**

创建 `config.yaml`：

```yaml
exchange:
  api_key: ""              # Agent 钱包私钥（64位十六进制，不含0x前缀）
  api_secret: ""           # 主钱包地址（含0x前缀）
  symbol: "HYPE"           # 交易对（不带USDT后缀）
  use_testnet: false       # 是否使用测试网

strategy:
  max_safety_orders: 9     # 最大网格层数
  atr_period: 14           # ATR 计算周期
  base_ratio: 0.05         # 头仓比例（余额 × 0.05）

storage:
  sqlite_path: "data/bot.db"
  redis_addr: ""           # 留空则不启用 Redis

log:
  level: "info"

health:
  addr: ":8080"
```

### 编译与运行

```bash
# 安装依赖
go mod tidy

# 编译二进制
go build -o bot cmd/bot/main.go

# 运行（通过环境变量传入密钥）
export MARTIN_EXCHANGE_API_KEY="your_agent_private_key"
export MARTIN_EXCHANGE_API_SECRET="0xyour_main_wallet_address"
./bot
```

### 健康检查

```bash
# 进程存活
curl http://localhost:8080/healthz

# 就绪状态（WS活跃 + FSM未冻结）
curl http://localhost:8080/readyz
```

## 策略详解

### 核心交易逻辑

1. **初始开仓**：无持仓时收到 Tick → 以 IOC 限价单（+5% 偏移模拟市价）快速开出首仓
2. **网格部署**：首仓成交后，以持仓均价为基准，按 9 级 ATR 间距向下放置限价加仓单
3. **动态止盈**：止盈单（TP）数量始终等于链上总持仓量，止盈价 = 持仓均价 + ATR(30m)
4. **加仓同步**：每次加仓单成交 → 重新获取链上持仓 → 更新 TP 数量和价格
5. **循环复位**：TP 成交（仓位归零）→ 取消所有挂单 → 回归 IDLE → 等待下一轮

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

9 层网格使用不同时间框架的 ATR 作为间距，从短周期到长周期逐层加深保护：

| 层级 | 间距计算 | 时间框架 | 说明 |
|------|----------|----------|------|
| 1 | ATR(30m) | 30 分钟 | 首层保护（最贴近当前波动） |
| 2 | ATR(1h) | 1 小时 | 第二层保护 |
| 3 | ATR(2h) | 2 小时 | 中短期保护 |
| 4 | ATR(4h) | 4 小时 | 中期保护 |
| 5 | ATR(8h) | 8 小时 | 中期保护 |
| 6 | ATR(12h) | 12 小时 | 中长期保护 |
| 7 | ATR(1d) | 日线 | 长期保护 |
| 8 | ATR(3d) | 3 日 | 长期保护 |
| 9 | ATR(1w) | 周线 | 最深层保护 |

> 间距为**相对上一层**的距离。ATR 获取失败时回退至入场价 × 1%。

### 数量递增规则

首仓 = `base_ratio`，前两次加仓使用半仓，从第三次起斐波那契递增：

| 层级 | 倍数 | 说明 | 数量（假设 unitQty=1） |
|------|------|------|------------------------|
| 1 | 1.0 | 首仓 | 1 |
| 2 | 0.5 | 第一次加仓（半仓） | 0.5 |
| 3 | 0.5 | 第二次加仓（半仓） | 0.5 |
| 4 | 1.0 | 第三次加仓 | 1 |
| 5 | 1.0 | 第四次加仓 | 1 |
| 6 | 2.0 | 第五次加仓（1+1） | 2 |
| 7 | 3.0 | 第六次加仓（1+2） | 3 |
| 8 | 5.0 | 第七次加仓（2+3） | 5 |
| 9 | 8.0 | 第八次加仓（3+5） | 8 |

> `base_ratio` 默认 0.05，头仓金额 = 账户 USDC 余额 × 0.05（不低于 10 USDC）

### 止盈策略

- **止盈数量**：始终等于链上真实持仓总量（`GetPosition().Size`），确保一次性全平
- **止盈价格**：`持仓均价 + ATR(30m)`
- **更新时机**：每次安全单（加仓单）成交后，重新获取链上持仓并更新 TP
- **原子替换**：优先使用 `ModifyOrder` 原子替换，失败时降级为 cancel + create
- **数量精度**：使用 `FloorToDecimals` 向下取整，杜绝残余尾仓

### 重启保护

重启时有持仓的冷启动行为：

1. **不动现有网格**：链上已有的限价网格委托单保持原样，不取消、不重新放置
2. **读取链上真实持仓**：通过 REST API 获取真实持仓均价和总数量
3. **恢复止盈单**：如果 TP 订单缺失则重新挂出，如果已存在则保持
4. **历史成交过滤**：启动后 3 秒内忽略 WS 推送的历史成交事件，防止误触发

> **安全原则**：重启时绝不重新放置网格。如果已有 5 层加仓成交，重新放置 9 层会导致总共 14 层，杠杆过高造成爆仓风险。

## 数量精度体系

所有 HYPE 代币数量计算严格执行**向下取整（Floor）**，杜绝余额不足拒单和幽灵尾仓：

| 函数 | 用途 | 示例 |
|------|------|------|
| `FloorToDecimals(qty, 2)` | 精度截断到 szDecimals | 0.666 → 0.66 |
| `FloorToTickSize(qty, 0.01)` | 步长对齐 | 0.1666 → 0.16 |

Floor 截断后若订单金额低于 $10 最低限制，自动向上微调一个 stepSize。

## 交易所机制适配

### 统一账户余额查询

Hyperliquid 统一账户的 USDC 存放在现货账户中，`GetBalance()` 同时查询永续合约账户和现货账户，取最大值：

```
perp_balance:  0.00    (永续合约账户)
spot_usdc:  1185.21    (现货账户) ← 统一账户在这里
used:       1185.21    (取最大值)
```

### 5 位有效数字价格截断

Hyperliquid 严格要求下单价格最多 5 位有效数字 + 最多 `(6 - szDecimals)` 位小数：

```
102.3456 → 102.35       (5 sig figs, 2 max decimals)
0.00123456 → 0.0012346  (5 sig figs, 6 max decimals)
100000 → 100000         (整数价格始终合法)
```

### 市价单模拟

Hyperliquid 无原生市价单，使用 IOC 限价单 + 5% 价格偏移模拟：

```go
// 市价买入：价格设为极高值确保成交
req.Price = price * 1.05  // 高于当前价 5%
```

### Agent 钱包签名

仅使用 Agent 私钥进行 L1 签名，主钱包私钥安全隔离。

## 生产级稳定性特性

### 1. FSM 事件绝对线性同步执行

EventBus handler 严格顺序同步执行，每个 handler 独立包裹 `defer recover()`，单个 handler panic 不影响其他 handler 和主循环。

### 2. 三层 WebSocket 稳定性防线

| 层级 | 机制 | 参数 |
|------|------|------|
| 第一层 | 主动心跳 | 30s ping 间隔，10s pong 超时 |
| 第二层 | 断线重连 + 指数退避 | 最多 10 次，2s 初始，60s 上限 |
| 第三层 | REST 对账冻结 | 重连后冻结 FSM，查询持仓校准 TP，延迟 2s 解冻 |

### 3. 启动与重连历史成交过滤

- **启动时**：`initialSyncDone` 标志位，`syncState` 完成后 3 秒才允许处理成交事件
- **重连时**：`frozen` 标志位延迟 2 秒解冻，给 WS 时间排空历史事件推送
- **REST 对账**：不再补发历史成交，仅通过持仓更新校准 TP

### 4. 过期行情丢弃

`PriceUpdate.IsStale(2s)` 丢弃超过 2 秒的陈旧价格，防止滑点。

### 5. 下单重试

`placeOrderWithRetry` 支持 3 次带随机抖动的指数退避重试：`200ms → 400ms → 800ms`。

### 6. 全 goroutine panic 自愈

所有常驻 goroutine 均有 `defer recover()` + 5 秒自愈重启机制。

## 目录结构

```
.
├── cmd/
│   └── bot/
│       └── main.go              # 程序入口
├── internal/
│   ├── config/
│   │   └── config.go            # Viper 配置加载（YAML + 环境变量）
│   ├── core/
│   │   └── event_bus.go         # 事件总线（顺序同步执行 + panic 自愈）
│   ├── exchange/
│   │   ├── adapter.go           # ExchangeAdapter 接口 + 领域模型
│   │   ├── hyperliquid.go       # Hyperliquid 适配器（REST + 统一账户余额）
│   │   └── ws_manager.go        # WSManager（三层稳定性 + 双 Channel）
│   ├── health/
│   │   └── health.go            # HTTP 健康检查
│   ├── strategy/
│   │   ├── strategy.go          # 马丁格尔 FSM
│   │   └── strategy_test.go     # 单元测试（16 个）
│   ├── storage/
│   │   └── storage.go           # SQLite + Redis
│   └── utils/
│       ├── indicators.go        # ATR + FloorToDecimals + FloorToTickSize
│       ├── logger.go            # Zap 结构化日志
│       └── price_rounder.go     # 5 位有效数字截断
├── config.yaml                  # 默认配置文件
├── go.mod                       # Go 模块依赖
└── AGENTS.md                    # 开发者指南
```

## 技术栈

| 组件 | 技术 |
|------|------|
| 语言 | Go 1.25+ |
| 交易所 SDK | go-hyperliquid |
| WebSocket | gorilla/websocket |
| 签名 | ethereum/go-ethereum |
| 存储 | SQLite (GORM) |
| 缓存/锁 | Redis (go-redis) |
| 配置 | Viper |
| 日志 | Zap |
| 技术指标 | go-talib |

## 风险提示

- 马丁格尔策略在单边下跌行情中风险极高，可能导致重大亏损
- 建议设置止损或限制最大持仓层数
- 请确保 Agent 钱包私钥安全，生产环境务必通过环境变量设置
- **强烈建议先在测试网（`use_testnet: true`）验证策略**
- 本软件仅供学习研究，不构成投资建议，使用风险自负

## License

MIT License
