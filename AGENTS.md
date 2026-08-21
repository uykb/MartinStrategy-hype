# AGENTS.md

## Build / Verification Commands
```bash
# Build binary
go build -o bot ./cmd/bot/

# Code checks
go vet ./...
go fmt ./...

# Run locally
go run cmd/bot/main.go
```
*Note on testing: No `_test.go` files exist yet. When adding new features, write table-driven tests alongside the source and mock exchange/storage clients.*

## Concurrency & Liveness Patterns
- **TryLock Prevention**: `placeGridOrders` and `updateTP` use `gridMu.TryLock()` and `tpMu.TryLock()`. If locked, they skip execution and set dirty flags (`tpDirty`) rather than blocking the main event loop.
- **Lock Boundaries**: Keep all network/exchange API calls outside of locks (`s.mu.Lock`/`RLock`) to prevent deadlocks and network call delays from blocking the event bus.
- **State Rollback**: Roll back FSM states to their previous state if downstream network actions fail.
- **Goroutine Liveness**: All background goroutines must run inside a `defer recover()` wrapper with a 5-second sleep before self-healing restart.
- **Thread-safe Flags**: Cross-goroutine flags (`frozen`, `tpDirty`, `initialSyncDone`) must be managed via `atomic.Bool`.

## Hyperliquid API Quirks & Limits
- **Price Precision Rules**: Price must not exceed 5 significant figures AND max `(6 - szDecimals)` decimals. Use `RoundToSigFigs(price, 5, maxPriceDecimals)`.
- **Epsilon Quantity Flooring**: To counter IEEE 754 float inaccuracies (e.g. 2.53 stored as 2.529999...), add a small epsilon (`+0.00000001`) before calling `FloorToTickSize` or `FloorToDecimals` on quantities.
- **MinNotional Protection**: Hyperliquid has a ~$10 USDC order minimum. If a floor-truncated order size falls below `MinNotional`, bump it by adding exactly one `stepSize`.
- **Market Order Simulation**: Hyperliquid lacks native market orders; simulate them via IOC limit orders at `price * 1.05` for BUYs, or `price * 0.95` for SELLs.
- **USDC Balance Queries**: Accounts with unified margin keep funds in the Spot balance. `GetBalance()` queries both perp and spot USDC balances and returns the maximum.

## FSM & Take Profit (TP) Rules
- **Take Profit Markup**: TP price is a fixed +0.80% (`entryPrice * 1.008`) rounded to 5 sig figs.
- **TP Anti-Chasing (防追价)**: Skip modifying or recreating TP if `tpPrice <= marketPrice`. This prevents restart/reconnect events from immediately filling a recreated TP limit order at market.
- **ReduceOnly**: All TP orders are SELL LIMIT orders and must enforce `ReduceOnly` to prevent opening unintended reverse positions.
- **TP Update Priority**: Prefer `ModifyOrder` (atomic cancel/replace). On failure, query the real exchange state using `findLiveTP()` to determine if the order actually succeeded on the exchange before attempting a manual cancel + create.
- **No Restart Grid Replacement**: On startup, query the on-chain position and re-calibrate/claim existing TP. Do not re-place grid orders to avoid doubling leverage and liquidation risk.
- **Cycle Reset**: Upon a SELL FILLED event, poll `GetPosition()` until size is 0 (up to 30s) before resetting the FSM to `IDLE`.
- **Cycle ID Protection**: A sequential `cycleID` tracks FSM generations. Asynchronous cancellations verify that `cycleID` matches before executing to avoid cancelling grid orders placed in a newer cycle.

## Connection & Synchronization Safeguards
- **Initial Sync Delay**: Ignore WS `OrderUpdate` events for the first 3 seconds after startup (`initialSyncDone` flag) to drain historical fills sent by the websocket.
- **FSM Freeze**: During WebSocket reconnect resyncs, freeze the FSM (`frozen` flag) to ignore all ticks and order updates, delaying unfreezing by 2 seconds to allow the WS history cache to drain.
- **REST Fallback**: If the WebSocket dies, the REST API polls price every 10s and publishes price ticks with local timestamps.
