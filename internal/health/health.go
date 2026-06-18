// Package health 提供 HTTP 健康检查端点，用于部署监控和运维探针。
//
// ★ P2 加固：生产环境 7×24 运行必备，支持 Kubernetes liveness/readiness 探针。
//
// 端点说明：
//   - GET /healthz  → liveness 探针（进程存活即可返回 200）
//   - GET /readyz   → readiness 探针（WS 连接 + 策略就绪才返回 200）
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/uykb/MartinStrategy/internal/utils"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// 健康状态数据源接口
// ---------------------------------------------------------------------------

// WSStatusProvider 提供 WebSocket 连接状态查询
type WSStatusProvider interface {
	IsWSActive() bool
}

// StrategyStatusProvider 提供策略 FSM 状态查询
type StrategyStatusProvider interface {
	CurrentState() string
	IsFrozen() bool
}

// ---------------------------------------------------------------------------
// HealthServer 核心结构
// ---------------------------------------------------------------------------

// HealthServer HTTP 健康检查服务器
type HealthServer struct {
	server *http.Server
	wg     sync.WaitGroup

	// 外部状态数据源
	wsProvider       WSStatusProvider
	strategyProvider StrategyStatusProvider

	// 启动时间（用于计算运行时长）
	startTime time.Time
}

// NewHealthServer 创建健康检查服务器
func NewHealthServer(addr string, ws WSStatusProvider, strategy StrategyStatusProvider) *HealthServer {
	mux := http.NewServeMux()
	hs := &HealthServer{
		wsProvider:       ws,
		strategyProvider: strategy,
		startTime:        time.Now(),
	}

	mux.HandleFunc("/healthz", hs.handleHealthz)
	mux.HandleFunc("/readyz", hs.handleReadyz)

	hs.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	return hs
}

// Start 启动健康检查 HTTP 服务器
func (h *HealthServer) Start() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		utils.Logger.Info("健康检查服务器启动", zap.String("addr", h.server.Addr))
		if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			utils.Logger.Error("健康检查服务器异常退出", zap.Error(err))
		}
	}()
}

// Stop 优雅关闭健康检查服务器
func (h *HealthServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.server.Shutdown(ctx); err != nil {
		utils.Logger.Error("健康检查服务器关闭失败", zap.Error(err))
	}
	h.wg.Wait()
	utils.Logger.Info("健康检查服务器已关闭")
}

// ---------------------------------------------------------------------------
// 端点处理器
// ---------------------------------------------------------------------------

// handleHealthz liveness 探针：进程存活即返回 200
func (h *HealthServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "ok",
		"uptime_s": time.Since(h.startTime).Seconds(),
	})
}

// handleReadyz readiness 探针：WS 连接 + 策略就绪才返回 200
func (h *HealthServer) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	wsActive := false
	frozen := false
	strategyState := "unknown"

	if h.wsProvider != nil {
		wsActive = h.wsProvider.IsWSActive()
	}
	if h.strategyProvider != nil {
		strategyState = h.strategyProvider.CurrentState()
		frozen = h.strategyProvider.IsFrozen()
	}

	// 就绪条件：WS 连接活跃 且 FSM 未冻结
	ready := wsActive && !frozen

	status := http.StatusServiceUnavailable
	if ready {
		status = http.StatusOK
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ready":          ready,
		"ws_active":      wsActive,
		"strategy_state": strategyState,
		"frozen":         frozen,
		"uptime_s":       time.Since(h.startTime).Seconds(),
	})
}
