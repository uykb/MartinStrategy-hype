package utils

import (
	"math"
)

// Convert float64 slice to precision for orders
// 注意：此函数使用四舍五入，仅适用于价格等非数量字段。
// 数量（Size）计算请使用 FloorToDecimals。
func ToFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return math.Round(num*output) / output
}

// FloorToDecimals 将数量向下取整到指定小数位数（Floor truncation）。
//
// 用于所有 HYPE 代币数量（Size）的计算，确保：
//   - 不会因四舍五入/向上取整导致余额不足（Insufficient Funds）被拒单
//   - 平仓数量 ≤ 实际持仓量，不会产生反向微型尾仓（幽灵仓位）
//
// 示例：FloorToDecimals(0.666, 2) → 0.66（而非 0.67）
//
// ★ 浮点容错修复：IEEE 754 下 2.53 实际存储为 2.5299999999999998，
// 直接 math.Floor(2.53*100) 会得到 252（而非 253），导致 Floor(2.53,2)=2.52。
// 这会让"仓位变化检测"失效（2.52 与 2.53 Floor 后相同 → 误判未变化 → TP 不更新），
// 最终出现"持仓 2.53 / TP 2.52"的不一致，无法一次性全部止盈。
// 加 epsilon 与 FloorToTickSize 保持一致的容错策略，消除浮点尾差误判。
func FloorToDecimals(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return math.Floor(num*output+0.00000001) / output
}

// FloorToTickSize 将数量向下取整到 tickSize 的整数倍。
//
// 与 RoundUpToTickSize 不同，本函数严格向下取整，适用于数量计算。
// 示例：FloorToTickSize(0.1666, 0.01) → 0.16（而非 0.17）
func FloorToTickSize(num float64, tickSize float64) float64 {
	if tickSize == 0 {
		return num
	}
	return math.Floor(num/tickSize+0.00000001) * tickSize
}

// RoundUpToTickSize rounds up a number to the nearest multiple of tickSize
// 注意：此函数使用向上取整，仅适用于价格等非数量字段。
// 数量（Size）计算请使用 FloorToTickSize。
// Example: num=0.1666, tickSize=0.01 -> 0.17
func RoundUpToTickSize(num float64, tickSize float64) float64 {
	if tickSize == 0 {
		return num
	}
	return math.Ceil(num/tickSize-0.00000001) * tickSize
}

// RoundToTickSize rounds a number to the nearest multiple of tickSize (standard rounding)
// Used for price formatting to match exchange filters
func RoundToTickSize(num float64, tickSize float64) float64 {
	if tickSize == 0 {
		return num
	}
	return math.Round(num/tickSize) * tickSize
}
