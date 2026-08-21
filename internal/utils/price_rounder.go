// Package utils 提供通用工具函数。
// price_rounder.go 实现了 Hyperliquid 交易所严格要求的
// "5 位有效数字" 价格截断规则，防止下单被拒。
package utils

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// Hyperliquid 价格精度规则
// ---------------------------------------------------------------------------
//
// Hyperliquid 对下单价格有两条硬性约束：
//
//   1. 最多 5 位有效数字（significant figures）
//      例如：102.345 → 5 位有效数字 ✓
//            102.3456 → 6 位有效数字 ✗（会被交易所拒绝）
//
//   2. 最多 (6 - szDecimals) 位小数（perps 的 MAX_DECIMALS=6）
//      例如：szDecimals=0 → 最多 6 位小数
//            szDecimals=5 → 最多 1 位小数
//
//   3. 整数价格始终合法（不受有效数字限制）
//      例如：100000 ✓
//
// 本文件提供 RoundToSigFigs 函数统一处理上述规则。
// ---------------------------------------------------------------------------

// RoundToSigFigs 将价格截断到指定有效数字位数，并限制最大小数位数。
//
// 参数：
//   - price:      原始价格
//   - sigFigs:    有效数字位数（Hyperliquid 要求 5）
//   - maxDecimals: 最大允许小数位数（perps = 6 - szDecimals）
//
// 返回：
//   - 截断后的价格（float64）
//
// 示例：
//
//	RoundToSigFigs(102.3456, 5, 2) → 102.35
//	RoundToSigFigs(0.00123456, 5, 6) → 0.0012346
//	RoundToSigFigs(100000.0, 5, 0) → 100000（整数价格始终合法）
func RoundToSigFigs(price float64, sigFigs int, maxDecimals int) float64 {
	// 防御性检查
	if price == 0 || sigFigs <= 0 {
		return 0
	}

	// 规则 3：整数价格始终合法
	if price == math.Trunc(price) && math.Abs(price) >= 1 {
		return price
	}

	// 规则 1：截断到 sigFigs 位有效数字
	rounded := roundToSignificantFigures(price, sigFigs)

	// 规则 2：限制最大小数位数
	if maxDecimals >= 0 {
		pow := math.Pow(10, float64(maxDecimals))
		rounded = math.Round(rounded*pow) / pow
	}

	return rounded
}

// roundToSignificantFigures 将浮点数截断到 n 位有效数字
// 使用 Go 标准库的格式化能力避免浮点精度陷阱
func roundToSignificantFigures(val float64, n int) float64 {
	if val == 0 || n <= 0 {
		return 0
	}

	// 利用 Go 的 %g 格式化：自动选择 %e 或 %f 中更紧凑的表示
	// %g 的精度参数即为有效数字位数
	format := fmt.Sprintf("%%.%dg", n)
	str := fmt.Sprintf(format, val)

	// 解析回 float64
	result, err := parseFloat(str)
	if err != nil {
		// 降级：直接返回原值
		return val
	}
	return result
}

// parseFloat 安全解析浮点数字符串，兼容科学计数法
func parseFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)
	return strconv.ParseFloat(s, 64)
}

// CalcMaxPriceDecimals 根据 szDecimals 计算 Hyperliquid 允许的最大价格小数位数。
// 公式：maxDecimals = MAX_DECIMALS - szDecimals
// 其中 MAX_DECIMALS 对永续合约为 6，对现货为 8。
func CalcMaxPriceDecimals(szDecimals int, isSpot bool) int {
	maxDecimals := 6
	if isSpot {
		maxDecimals = 8
	}
	result := maxDecimals - szDecimals
	if result < 0 {
		result = 0
	}
	return result
}
