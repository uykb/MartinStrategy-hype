package utils

import (
	"math"

	"github.com/markcheno/go-talib"
)

// CalculateATR calculates the Average True Range
// klines: slice of High, Low, Close prices
// period: typical value is 14
func CalculateATR(highs, lows, closes []float64, period int) float64 {
	if len(highs) < period+1 {
		return 0
	}

	atr := talib.Atr(highs, lows, closes, period)
	if len(atr) == 0 {
		return 0
	}

	// Return the latest ATR value
	return atr[len(atr)-1]
}

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
func FloorToDecimals(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return math.Floor(num*output) / output
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
