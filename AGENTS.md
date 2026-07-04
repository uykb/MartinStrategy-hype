# AGENTS.md

## Build / Test / Lint Commands

```bash
# Build binary
go build -o bot ./cmd/bot/

# Run all tests
go test ./...

# Run single test (example pattern)
go test -run TestFunctionName ./internal/utils/...
go test -v -run TestRoundToSigFigs ./internal/utils/

# Code checks
go vet ./...
go fmt ./...

# Dependencies
go mod tidy
go mod download

# Run locally
go run cmd/bot/main.go

# Docker
docker-compose build
docker-compose up -d
```

## Code Style Guidelines

### Imports
- Group imports: stdlib, blank line, third-party, blank line, project packages
- Use `goimports` style (project uses module `github.com/uykb/MartinStrategy`)
- Example:
```go
import (
    "context"
    "fmt"
    "sync"

    "github.com/sonirico/go-hyperliquid"
    "go.uber.org/zap"

    "github.com/uykb/MartinStrategy/internal/config"
    "github.com/uykb/MartinStrategy/internal/core"
)
```

### Formatting
- Standard Go formatting (`gofmt`)
- Line length: aim for ~100 chars, no hard limit
- Comments for exported types/functions start with the name

### Types
- Use custom type definitions for states/enums: `type State string`, `type EventType string`
- Prefer explicit types over primitives for domain concepts
- Struct tags use `mapstructure` for config, `json`/`gorm` for storage models

### Naming Conventions
- **Exported**: PascalCase (e.g., `EventBus`, `NewHyperliquidAdapter`)
- **Unexported**: camelCase (e.g., `currentState`, `handleTick`)
- **Constants**: PascalCase for exported, camelCase for unexported (e.g., `StateIdle`, `minNotional`)
- **Interfaces**: `-er` suffix (e.g., `EventHandler`)
- **Acronyms**: Keep uppercase (e.g., `TP`, `API`)
- Event type constants: `Event` prefix (e.g., `EventTick`, `EventOrderUpdate`)

### Error Handling
- Wrap errors with context: `fmt.Errorf("failed to get exchange info: %w", err)`
- Return errors to callers; only log at appropriate levels
- Fatal only in `main.go` or initialization failures
- Use Zap for structured logging with fields:
```go
utils.Logger.Error("Failed to do something", zap.Error(err), zap.String("symbol", symbol))
```

### Concurrency Patterns
- Always use `sync.Mutex` or `sync.RWMutex` for shared state
- Use `TryLock()` pattern for re-entrant prevention:
```go
if !s.gridMu.TryLock() {
    s.gridSkipCount++
    return
}
defer s.gridMu.Unlock()
```
- Keep network calls OUTSIDE of locks to prevent blocking
- Rollback state on failure:
```go
s.mu.Lock()
s.currentState = StatePlacingGrid
s.mu.Unlock()

if err := doNetworkCall(); err != nil {
    s.mu.Lock()
    s.currentState = StateIdle  // Rollback
    s.mu.Unlock()
}
```
- All long-running goroutines must have `defer recover()` + 5s self-healing restart
- Use `atomic.Bool` for cross-goroutine flags (frozen, tpDirty, initialSyncDone)
- Use context cancellation for goroutine lifecycle control

### Configuration
- Environment variables use `MARTIN_` prefix (e.g., `MARTIN_EXCHANGE_API_KEY`)
- Struct field tags use snake_case: `mapstructure:"api_key"`
- YAML config file uses snake_case keys

### Comments
- All exported items must have a comment starting with the name
- Comments in Chinese are acceptable (existing code has some)
- Doc comments should explain purpose, not implementation details

## Architecture Quick Reference

| Package | Purpose |
|---------|---------|
| `internal/config` | Viper-based config loading from YAML/env (prefix `MARTIN_`) |
| `internal/core` | Event bus with Pub/Sub pattern, buffer 1000 |
| `internal/exchange` | Hyperliquid WebSocket + REST adapter (ExchangeAdapter interface) |
| `internal/strategy` | Martingale FSM (states: IDLE → PLACING_GRID → IN_POSITION) |
| `internal/storage` | GORM + SQLite, Redis for locking |
| `internal/utils` | Price rounding (5 sig figs, Floor truncation), Zap logger |

## Key Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `MinOrderValue` | 10.0 | Minimum USDC order value (动态头仓下限) |
| `MaxTickStaleness` | 2s | Max allowed price age; stale ticks are dropped (防滑点) |
| Price polling interval | 10s | REST fallback when WS is down |
| Position monitor interval | 5s | Monitor position zero → auto reset to IDLE |
| Grid order rate limit | 200ms | Delay between each grid order placement |
| Grid order retries | 3 | Per-level retry with jitter exponential backoff |
| TP dirty retries | 3 | Max retries for dirty TP loop (liveness guard) |
| WS ping interval | 30s | Heartbeat ping |
| WS pong timeout | 10s | Heartbeat pong wait |
| WS reconnect | 10 retries | Exponential backoff (2s initial, 60s max) |
| WS initial sync delay | 3s | Wait after syncState before processing fills |
| Resync thaw delay | 2s | Delay after ResyncEnd before unfreezing FSM |
| Entry order timeout | 30s | Max wait for limit order fill before cancelling and resetting |
| Entry order timeout | 30s | Max wait for market order fill before resetting |

## Key Config Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `base_ratio` | 0.05 | 头仓金额 = 账户 USDC 余额 × base_ratio（动态计算，每次实时查询） |
| `max_safety_orders` | 9 | 最大网格层数 |
| `atr_period` | 14 | **未使用**（保留但策略中无 ATR 计算） |

## Grid Strategy Details

### Grid Price Distances (Fixed Percentage)

网格间距是**固定百分比**，相对于**前一层**价格（非入口价）：

```
gridPercents := []float64{0.010, 0.010, 0.010, 0.011, 0.021, 0.022, 0.045, 0.048, 0.077}
```

| Level | Drop % | Cumulative Drop | Description |
|-------|--------|----------------|-------------|
| 1 | -1.0% | -1.0% | 首层保护 |
| 2 | -1.0% | -2.0% | 第二层保护 |
| 3 | -1.0% | -3.0% | 第三层保护 |
| 4 | -1.1% | -4.0% | 第四层保护 |
| 5 | -2.1% | -6.0% | 第五层保护 |
| 6 | -2.2% | -8.1% | 第六层保护 |
| 7 | -4.5% | -12.3% | 第七层保护 |
| 8 | -4.8% | -16.5% | 第八层保护 |
| 9 | -7.7% | -22.9% | 最深层保护 |

- Each level price = `previousLevelPrice × (1 - stepPct)`
- All prices truncated to **5 significant figures** via `RoundToSigFigs(price, 5, maxPriceDecimals)`
- Beyond level 9, fallback to last array element (0.077 = 7.7%)
- 无 ATR 依赖：无需 fallback，纯硬编码百分比

### Position Sizing

首仓和加仓使用固定倍数，乘以 `unitQty`（`balance × base_ratio / price`）得到实际下单数量。

| 仓位 | 倍数 | 有效余额占比 |
|------|------|------------|
| 首仓 | 0.06 | balance × base_ratio × 0.06 |
| 加仓1 | 0.03 | balance × base_ratio × 0.03 |
| 加仓2 | 0.03 | balance × base_ratio × 0.03 |
| 加仓3 | 0.05 | balance × base_ratio × 0.05 |
| 加仓4 | 0.05 | balance × base_ratio × 0.05 |
| 加仓5 | 0.18 | balance × base_ratio × 0.18 |
| 加仓6 | 0.32 | balance × base_ratio × 0.32 |
| 加仓7 | 0.567 | balance × base_ratio × 0.567 |
| 加仓8 | 0.578 | balance × base_ratio × 0.578 |
| 加仓9 | 1.16 | balance × base_ratio × 1.16 |

首仓在 `enterLong()` 中硬编码为 `0.06`，不通过 `getGridMultiplier`。加仓层通过 `getGridMultiplier(level)` 查表，level > 9 向下取最后一层 `1.16`。

### Quantity Flooring Rules

All order quantities use **strict floor truncation** to prevent insufficient balance:

```go
qty = FloorToTickSize(qty, stepSize)       // Floor to tickSize multiple
qty = FloorToDecimals(qty, quantityPrecision) // Floor to decimal precision
```

Both functions add a small epsilon (`+0.00000001`) before floor to counter IEEE 754 floating point tail errors (e.g., `2.53` stored as `2.529999...`).

If floor-truncated value falls below `MinNotional`, quantity is bumped by one `stepSize`:

```go
if qty * price < minNotional {
    qty = FloorToDecimals(qty + stepSize, quantityPrecision)
}
```

### Take Profit (TP)

- **TP price**: `RoundToSigFigs(entryPrice * 1.008, 5, maxPriceDecimals)` — fixed **+0.80%** markup
- **TP quantity**: `FloorToDecimals(abs(pos.Size), quantityPrecision)` — full position close, floor truncated to prevent residual phantom position
- **Updated after**: each safety order fill, grid placement, position update event, or restart

#### TP Update Flow (strategy.go:1100-1322)

1. If position size is zero → clear TP state, return
2. **Entry reconciliation**: if `currentTPOrderID == 0`, call `findLiveTP()` to claim any existing TP on exchange (prevents duplicate create on restart)
3. **Change detection**: if `newQty == prevQty && oldTPID != 0` → skip (no ATR fetch, no TP modify)
4. **Modify priority**: if `oldTPID != 0`, try `ModifyOrder()` first (atomic cancel+place, no protection gap)
5. **Modify failure reconciliation**: if modify fails, query exchange real state via `findLiveTP()`:
   - `liveID != oldTPID` → modify actually succeeded server-side, just sync local state
   - `liveID == oldTPID` → modify truly failed, cancel old + create new
   - `liveID == 0` → old TP gone, skip cancel and create directly
6. **Create fallback**: if no existing TP, create new limit sell order

#### Concurrency Safety (safeUpdateTP)

- Uses `tpMu.TryLock()` pattern — if another update is in flight, sets `tpDirty` flag and returns
- Current holder checks `tpDirty` after completion, re-runs up to `maxTPDirtyRetries` (3) times
- Prevents goroutine pile-up in high-frequency fill scenarios

## Dynamic Notional Calculation

```go
func (s *MartingaleStrategy) calcMinNotional() float64 {
    balance, err := s.exchange.GetBalance()  // REST API 查询 USDC 余额
    if err != nil {
        return MinOrderValue  // 降级到 10.0 USDC
    }
    notional := balance * s.cfg.BaseRatio  // 余额 × 比例
    if notional < MinOrderValue {
        notional = MinOrderValue  // 不低于 Hyperliquid 最低限制
    }
    return notional
}
```

- 每次 `enterLong()` 和 `placeGridOrders()` 各独立调用一次，不缓存（每次实时查询余额）
- `enterLong` 中 `unitQty = calcMinNotional() / currentPrice` → `baseQty = unitQty * 1.0`
- `placeGridOrders` 中 `unitQty = calcMinNotional() / entryPrice`，循环内按 `getGridMultiplier(i)` 缩放

## Entry & Grid Placement Flow

```
IDLE → PriceUpdate Tick (fresh, ≤2s old)
  → state = PLACING_GRID
  → enterLong(price):
      1. calcMinNotional() → unitQty → baseQty (×0.06)
      2. limitPrice = RoundToSigFigs(price × 1.01, 5, maxPriceDecimals)  // 1% 溢价
      3. CreateOrder(LIMIT BUY GTC, baseQty, limitPrice)                 // 挂单成交
      4. Save entryOrderID + entrySubmittedQty for fill tracking
  → waitForFillAndPlaceGrid() [goroutine, polls 2s interval, 30s timeout]
       → On timeout / state change: cancelEntryOrder() + reset to IDLE
  → handleOrderUpdate FILLED (matches entryOrderID):
      1. Accumulate entryCumulativeFilled += order.Quantity
      2. When cumulativeFilled ≥ submittedQty × 0.999 → fully filled
      3. Set state = StateInPosition, clear entry tracking
      4. → safePlaceGridOrders() → placeGridOrders():
           1. Check grid completeness via GetOpenOrders()
              - If grid intact (gridCount ≥ MaxSafetyOrders), gridPlaced=true, return
              - If incomplete, cancel existing buy orders, re-place
           2. calcMinNotional() → unitQty
           3. For level 1..MaxSafetyOrders:
              price = prevLevelPrice × (1 - stepPct), RoundToSigFigs
              qty = unitQty × getGridMultiplier(i), FloorToTickSize, FloorToDecimals
              placeOrderWithRetry(LIMIT BUY GTC, 3 retries, jitter backoff)
              200ms rate limit delay
           4. If all placed, gridPlaced=true
       → safeUpdateTP()
```

## Price Precision (Hyperliquid Rules)

File: `internal/utils/price_rounder.go`

Three hard constraints:
1. **5 significant figures max**: e.g., `102.345` ✓, `102.3456` ✗
2. **Max (6 - szDecimals) decimal places**: perps MAX_DECIMALS=6
3. **Integer prices always valid**: e.g., `100000` ✓

All order prices go through `RoundToSigFigs(price, 5, maxPriceDecimals)` before submit.

Entry orders use LIMIT GTC at `price × 1.01` (1% above market, maker-friendly).
Grid orders use LIMIT GTC at calculated grid prices.

## Stale Price Protection

- `PriceUpdate` carries server timestamp (ms)
- `IsStale(maxLatency)` checks `time.Since(eventTime) > maxLatency`
- Ticks older than 2 seconds are dropped (`strategy.go:386`)
- Prevents FSM from entering on stale prices during WS reconnect or latency spikes

## FSM Freeze During Reconciliation

- `EventResyncStart` → `frozen.Store(true)` — all ticks and order updates discarded
- `EventResyncEnd` → `go sleep(2s) then frozen.Store(false)` — delay allows WS history event drain
- Prevents websocket reconnect history events from corrupting FSM state

## Initial Sync Protection

- `syncState()` runs on Start, sets `initialSyncDone` after 3s delay
- All `OrderUpdate` FILLED events before `initialSyncDone` are discarded
- Prevents historical fill events (pushed async by WS after reconnect) from triggering false FSM transitions

## REST Price Fallback

When WebSocket is down, REST API polls every 10s and publishes ticks with local timestamps:
- `restPriceFallback()` goroutine in `internal/exchange/hyperliquid.go:737-770`
- Skips when WS is active to avoid duplicate price events

## Adding Features

### New Event Type
1. Add constant in `internal/core/event_bus.go`
2. Publish from source component
3. Subscribe in `strategy/strategy.go` handler

### New Strategy State
1. Define in `internal/strategy/strategy.go` as `const StateName State = "NAME"`
2. Add transition logic in appropriate handler
3. Update state machine comments

### New REST API Method
1. Add method to `HyperliquidAdapter` in `internal/exchange/hyperliquid.go`
2. Use `h.infoClient.Xxx()` or `h.exchangeClient.Xxx()` pattern
3. Wrap errors with context

## Testing
- No tests exist yet; create `_test.go` files alongside source
- Use table-driven tests
- Mock external dependencies (exchange client, storage)
