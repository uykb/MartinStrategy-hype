# MartinStrategy-Hype

A production-hardened, event-driven Martingale perpetual futures trading bot for **Hyperliquid**, built in pure Go with zero CGO dependencies.

## Overview

MartinStrategy-Hype implements a Martingale grid strategy using an **Event-Driven Finite State Machine (ED-FSM)** architecture optimized for 24/7 unattended operation. It trades Hyperliquid perpetual contracts via WebSocket market data with REST API fallback.

**Key design principles:**

- **Go-native concurrency** — zero CGO, pure Go; WebSocket-primary with REST degradation
- **ED-FSM architecture** — strictly sequential FSM transitions eliminate race conditions
- **Three-layer connection stability** — active heartbeat → exponential-backoff reconnect → REST resync with FSM freeze
- **Deep Hyperliquid integration** — USDC settlement, 5-significant-figure price truncation, Agent wallet EIP-712 signing, unified account balance queries

## Architecture

```
                          Hyperliquid Exchange
                         ┌─────────────────────┐
                         │  REST API  │  WS API │
                         └─────┬──────┴───┬────┘
                               │           │
                     ┌─────────▼───┐  ┌─────▼──────────┐
                     │ REST Client │  │   WSManager     │
                     │ (degraded)  │  │                  │
                     │             │  │ ┌──────────────┐ │
                     │ GetPosition │  │ │ 3-Layer      │ │
                     │ GetBalance  │  │ │ Stability    │ │
                     │ CreateOrder │  │ │ ──────────── │ │
                     │ CancelOrder │  │ │ ① Heartbeat  │ │
                     │ OpenOrders  │  │ │ ② Reconnect  │ │
                     └──────┬──────┘  │ │ ③ REST Resync│ │
                            │         │ └──────────────┘ │
                            │         │                    │
               IsWSActive() │         │ ┌──────────────┐ │
               ┌────────────┤         │ │ Dual Channel  │ │
               │ Skip REST  │         │ │ ──────────── │ │
               │ when WS up │         │ │ priceEventCh │ │
               └────────────┘         │ │ orderEventCh │ │
                                      │ └──────┬───────┘ │
                                      └────────┼─────────┘
                                               │
                               ┌───────────────┼───────────────┐
                               │               │               │
                      EventTick      EventOrderUpdate   EventResyncStart/End
                               │               │               │
                               ▼               ▼               ▼
                     ┌─────────────────────────────────────────────┐
                     │            EventBus (1000 buffer)           │
                     │                                             │
                     │  ★ Strict sequential execution (no races)   │
                     │  ★ defer recover() + 5s self-healing        │
                     └──────────────────┬──────────────────────────┘
                                        │
                                        ▼
                     ┌─────────────────────────────────────────────┐
                     │          MartingaleStrategy (FSM)            │
                     │                                             │
                     │   IDLE ──▶ PLACING_GRID ──▶ IN_POSITION ──┐ │
                     │      ▲                                  ◀─┘ │
                     │      └─── TP fill / manual close ──────────┘│
                     │                                             │
                     │  ★ frozen — pause during REST resync        │
                     │  ★ initialSyncDone — filter historical fills│
                     │  ★ IsStale(2s) — discard stale prices       │
                     │  ★ 3-retry jittered backoff on orders       │
                     │  ★ FloorToDecimals — floor-quantity safety  │
                     └─────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- **Go 1.25+**
- A Hyperliquid account (unified or standard)
- Agent wallet private key + main wallet address

### Configuration

Use environment variables (recommended for production) or `config.yaml`.

**Via environment variables:**

```bash
export MARTIN_EXCHANGE_API_KEY="your_agent_private_key_64_hex_no_0x"
export MARTIN_EXCHANGE_API_SECRET="0xyour_main_wallet_address"
export MARTIN_EXCHANGE_SYMBOL="HYPE"
export MARTIN_EXCHANGE_USE_TESTNET="true"   # testnet first!
export MARTIN_LOG_LEVEL="info"
```

**Via `config.yaml`:**

```yaml
exchange:
  api_key: ""              # Agent wallet private key (64 hex, no 0x)
  api_secret: ""           # Main wallet address (with 0x prefix)
  symbol: "HYPE"           # Trading pair (no USDT suffix)
  use_testnet: false

strategy:
  max_safety_orders: 9
  base_ratio: 0.05         # % of balance per base order

storage:
  sqlite_path: "data/bot.db"

log:
  level: "info"

health:
  addr: ":8080"
```

### Build & Run

```bash
# Install dependencies
go mod tidy

# Build binary
go build -o bot cmd/bot/main.go

# Run (pass credentials via env vars)
export MARTIN_EXCHANGE_API_KEY="your_agent_private_key"
export MARTIN_EXCHANGE_API_SECRET="0xyour_main_wallet_address"
./bot
```

### Health Checks

```bash
curl http://localhost:8080/healthz   # liveness (process alive = 200)
curl http://localhost:8080/readyz    # readiness (WS active + FSM not frozen = 200)
```

## Strategy

### Core Trading Logic

1. **Entry** — When IDLE and a tick arrives, place an IOC limit buy at +5% of market price to simulate a market order for instant fill.
2. **Grid deployment** — After the base order fills, place 9 limit buy orders below the entry price using fixed percentage steps.
3. **Dynamic take-profit** — A single TP sell order always covers the full on-chain position size. TP price = entry price × 1.008 (+0.80%).
4. **Safety-order sync** — Every time a grid order fills, the bot re-fetches the on-chain position and updates both TP quantity and price.
5. **Cycle reset** — When the TP fills (position → 0), all remaining grid orders are cancelled and the bot returns to IDLE.

### FSM States

```
┌──────────┐       tick event      ┌───────────────┐
│   IDLE   │ ────────────────────▶ │ PLACING_GRID  │
│ (no pos) │                       │ (positioning) │
└──────────┘                       └───────┬───────┘
     ▲                                     │
     │                                     │ position detected
     │                                     ▼
     │                             ┌───────────────┐
     │       TP fill (SELL)        │ IN_POSITION   │
     └──────────────────────────── │  (in trade)   │
                                   │               │
                                   │ safety fills  │
                                   ▼               │
                              update TP            │
                                   ▲               │
                                   └───────────────┘
```

| State | Description | Entered by | Exited by |
|-------|-------------|------------|-----------|
| `IDLE` | Waiting, no position | Startup / TP fill / manual close | Tick event |
| `PLACING_GRID` | Base order submitted, awaiting fill | Market buy executed | Position detected |
| `IN_POSITION` | Holding position, grid active | Base order filled | TP fill / manual close |

### Grid Spacing

9 levels with fixed percentage steps relative to the **previous level's price**:

| Level | Step Down | Cumulative from Entry |
|-------|-----------|----------------------|
| 1 | −1.0% | −1.0% |
| 2 | −1.0% | −2.0% |
| 3 | −1.0% | −3.0% |
| 4 | −1.1% | −4.0% |
| 5 | −2.1% | −6.0% |
| 6 | −2.2% | −8.1% |
| 7 | −4.5% | −12.3% |
| 8 | −4.8% | −16.5% |
| 9 | −7.7% | −23.0% |

### Position Sizing

Base order = `balance × 6%`, minimum $10 USDC. Safety order sizing is configured explicitly to match specific asset characteristics:

| Level | Asset Allocation % |
|-------|--------------------|
| 1 (base) | 6% |
| 2 (Grid 1) | 3% |
| 3 (Grid 2) | 3% |
| 4 (Grid 3) | 5% |
| 5 (Grid 4) | 5% |
| 6 (Grid 5) | 18% |
| 7 (Grid 6) | 32% |
| 8 (Grid 7) | 56.7% |
| 9 (Grid 8) | 57.8% |
| 10 (Grid 9+) | 116% |

### Take-Profit

- **Size**: `FloorToDecimals(|position.Size|, szDecimals)` — always equals the full on-chain position.
- **Price**: `entryPrice × 1.008` (+0.80%), truncated to 5 significant figures.
- **Update**: triggered every time a safety order fills.
- **Replacement**: prefers `ModifyOrder` (atomic) and falls back to cancel + create on failure.

### Cold-Start with Existing Position

When the bot restarts and detects an on-chain position:

1. **Do not touch existing grid orders** — on-chain orders are the source of truth.
2. **Read real on-chain position** via REST API (entry price and size).
3. **Restore TP** if missing; if present, initialize tracking state to prevent unnecessary re-creation.

> **Safety principle**: Never re-place grid orders on restart. If 5 safety layers have already filled, re-placing 9 orders would result in 14 layers total — extreme leverage and liquidation risk.

## Quantity Precision (Floor Truncation)

All token quantity calculations strictly use **floor truncation** to prevent insufficient-funds rejections and ghost residual positions:

| Function | Purpose | Example |
|----------|---------|---------|
| `FloorToDecimals(qty, 2)` | Truncate to `szDecimals` | 0.666 → 0.66 |
| `FloorToTickSize(qty, 0.01)` | Align to tick size | 0.1666 → 0.16 |

When floor truncation drops the order value below $10, the bot automatically bumps it by one `stepSize`.

## Exchange Integration

### Unified Account Balance

Hyperliquid unified accounts hold USDC in the spot account. `GetBalance()` queries both perp and spot balances and returns the maximum:

```
perp_balance:     0.00
spot_usdc:    1,185.21
used:         1,185.21   (takes the max)
```

### 5-Significant-Figure Price Truncation

Hyperliquid requires all order prices to have at most 5 significant figures and at most `(6 − szDecimals)` decimal places:

```
102.3456 → 102.35       (5 sig figs, 2 max decimals)
0.00123456 → 0.0012346  (5 sig figs, 6 max decimals)
100000 → 100000         (integers always valid)
```

### Market Order Simulation

Hyperliquid has no native market orders. The bot uses IOC limit orders with a 5% price offset to guarantee immediate execution:

```go
// Market buy: set price far above current to ensure fill
req.Price = price * 1.05  // 5% above best bid
```

### Agent Wallet Signing

Only the Agent private key is used for L1 signing; the main wallet private key remains secure.

## Production-Stability Features

### 1. Strictly Sequential FSM Execution

EventBus handlers execute synchronously in strict order. Each handler is wrapped in `defer recover()`. A single handler's panic never affects other handlers or the main loop.

### 2. Three-Layer WebSocket Stability

| Layer | Mechanism | Parameters |
|-------|-----------|------------|
| 1 | Active heartbeat | 30s ping interval, 10s pong timeout |
| 2 | Reconnect + exponential backoff | Max 10 attempts, 2s initial, 60s cap |
| 3 | REST resync with FSM freeze | Freeze FSM → fetch position → calibrate TP → delayed unfreeze (2s) |

### 3. Historical Fill Filtering

- **On startup**: `initialSyncDone` flag prevents processing FILL events for 3 seconds after state sync.
- **On reconnect**: `frozen` flag remains set for 2 seconds after resync, giving the WebSocket time to drain replayed events.
- **REST resync**: No longer replays historical fills; only publishes position updates for TP calibration.

### 4. Stale Price Discard

`PriceUpdate.IsStale(2s)` drops any tick older than 2 seconds to prevent slippage.

### 5. Order Retries

`placeOrderWithRetry` uses 3 attempts with jittered exponential backoff: `200ms → 400ms → 800ms`.

### 6. Goroutine Panic Self-Healing

Every long-running goroutine has `defer recover()` + 5-second self-healing restart.

## Directory Structure

```
.
├── cmd/bot/main.go                    # Entry point
├── internal/
│   ├── config/config.go               # Viper config (YAML + env vars)
│   ├── core/event_bus.go              # Sequential event bus with self-healing
│   ├── exchange/
│   │   ├── adapter.go                 # ExchangeAdapter interface + domain models
│   │   ├── hyperliquid.go             # Hyperliquid adapter (REST + unified balance)
│   │   └── ws_manager.go              # WSManager (3-layer stability, dual channel)
│   ├── health/health.go               # HTTP health endpoints
│   ├── strategy/strategy.go           # Martingale FSM
│   ├── storage/storage.go             # SQLite + optional Redis
│   └── utils/
│       ├── indicators.go              # FloorToDecimals, FloorToTickSize
│       ├── logger.go                  # Zap structured logging
│       └── price_rounder.go           # 5 sig-fig price truncation
├── go.mod
├── config.yaml
└── AGENTS.md
```

## Tech Stack

| Component | Library |
|-----------|---------|
| Language | Go 1.25+ |
| Exchange SDK | go-hyperliquid |
| WebSocket | gorilla/websocket |
| Signing | ethereum/go-ethereum |
| Storage | SQLite (GORM) |
| Locking | Redis (go-redis) — optional |
| Config | Viper |
| Logging | Zap |

## Risk Warning

- Martingale strategies carry extreme risk in sustained downtrends and may lead to significant losses.
- Use a stop-loss or limit the maximum number of grid levels.
- Keep your Agent wallet private key secure; always use environment variables in production.
- **Strongly recommended: test on testnet first** (`use_testnet: true`).
- This software is for educational and research purposes only. It does not constitute investment advice. Use at your own risk.

## License

MIT License
