# AGENTS.md

## Build / Test / Lint Commands

```bash
# Build binary
go build -o bot ./cmd/bot/

# Run all tests
go test ./...

# Run single test (example pattern)
go test -run TestFunctionName ./internal/utils/...
go test -v -run TestCalculateATR ./internal/utils/

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
- **Acronyms**: Keep uppercase (e.g., `ATR`, `TP`, `API`)
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
| `internal/config` | Viper-based config loading from YAML/env |
| `internal/core` | Event bus with Pub/Sub pattern |
| `internal/exchange` | Hyperliquid WebSocket + REST adapter (ExchangeAdapter interface) |
| `internal/strategy` | Martingale FSM (states: IDLE → PLACING_GRID → IN_POSITION) |
| `internal/storage` | GORM + SQLite, Redis for locking |
| `internal/utils` | Indicators (ATR), rounding, Zap logger |

## Key Constants
- `MinOrderValue = 10.0` - Minimum USDC order value for Hyperliquid Perps (动态头仓下限)
- Event queue buffer: 1000
- Grid levels: 9 max (1h/2h/4h/8h/12h/1d/3d/1w/1M)
- Quantity scaling: 首仓=1x, 加仓1=0.5x, 加仓2=0.5x, 加仓3=1x, 加仓4=1x, then Fibonacci (2,3,5,8...)
- Price polling interval: 10s
- Position monitor interval: 5s
- Grid order API rate limit: 200ms between orders
- WebSocket heartbeat: 30s ping interval, 10s pong timeout
- WebSocket reconnect: up to 10 retries with exponential backoff (2s initial, 60s max)
- REST resync on reconnect: query position + open orders + recent fills

## Key Config Parameters
- `base_ratio: 0.05` - 头仓金额 = 账户 USDC 余额 × base_ratio（动态计算，每次开仓前实时查询）
- `max_safety_orders: 9` - 最大网格层数
- `atr_period: 14` - ATR 计算周期

## Grid Strategy Details

### ATR Grid Distances (9 Levels)

| Level | Timeframe | ATR Source | Description |
|-------|-----------|------------|-------------|
| 1 | 1h | `fetchATR("1h")` | 首层保护 |
| 2 | 2h | `fetchATR("2h")` | 第二层保护 |
| 3 | 4h | `fetchATR("4h")` | 中短期保护 |
| 4 | 8h | `fetchATR("8h")` | 中期保护 |
| 5 | 12h | `fetchATR("12h")` | 中长期保护 |
| 6 | 1d | `fetchATR("1d")` | 长期保护 |
| 7 | 3d | `fetchATR("3d")` | 长期保护 |
| 8 | 1w | `fetchATR("1w")` | 超长期保护 |
| 9 | 1M | `fetchATR("1M")` | 最深层保护 |

- Distances are **relative to previous order**, not absolute
- ATR fetch failure fallback: `entryPrice * 0.01`
- Beyond level 9, fallback to last defined distance (ATR(1M))

### Quantity Scaling (getGridMultiplier)

首仓 = base_ratio，前两次加仓使用半仓，从第三次起斐波那契递增：

```go
func (s *MartingaleStrategy) getGridMultiplier(level int) float64 {
    switch level {
    case 1: return 1.0   // 首仓
    case 2: return 0.5   // 第一次加仓（半仓）
    case 3: return 0.5   // 第二次加仓（半仓）
    case 4: return 1.0   // 第三次加仓
    case 5: return 1.0   // 第四次加仓
    default:
        // 斐波那契递增：level 6→2.0, 7→3.0, 8→5.0, 9→8.0
        a, b := 1.0, 1.0
        for i := 6; i <= level; i++ { a, b = b, a+b }
        return b
    }
}
```

Generates: 1.0, 0.5, 0.5, 1.0, 1.0, 2.0, 3.0, 5.0, 8.0 for levels 1-9.

### Take Profit (TP)

- TP price: `avgPrice + ATR(30m)` (always uses 30-minute ATR)
- TP quantity: full position close
- Updated after each safety order fill

## Dynamic Notional Calculation

头仓金额通过 `calcMinNotional()` 动态计算：

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

- 调用时机：`enterLong()` 和 `placeGridOrders()` 各调用一次，同一轮下单内缓存结果
- `enterLong` 中计算 `unitQty = calcMinNotional() / currentPrice`
- `placeGridOrders` 中计算 `unitQty = calcMinNotional() / entryPrice`，循环内按 `getGridMultiplier(i)` 缩放

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