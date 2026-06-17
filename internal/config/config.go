// Package config 提供 Viper 驱动的配置加载，
// 支持 YAML 文件 + 环境变量覆盖（前缀 MARTIN_）。
//
// 重构说明：
//   - ExchangeConfig 新增 Hyperliquid 专属字段
//   - ApiKey 字段复用为 Agent 钱包私钥
//   - ApiSecret 字段复用为主钱包地址
//   - Symbol 字段不再带 USDT 后缀（Hyperliquid 使用 "HYPE" 而非 "HYPEUSDT"）
package config

import (
	"strings"

	"github.com/spf13/viper"
)

// Config 根配置结构
type Config struct {
	Exchange ExchangeConfig `mapstructure:"exchange"`
	Strategy StrategyConfig `mapstructure:"strategy"`
	Storage  StorageConfig  `mapstructure:"storage"`
	Log      LogConfig      `mapstructure:"log"`
	Health   *HealthConfig  `mapstructure:"health"` // ★ P2 加固：健康检查配置
}

// ExchangeConfig 交易所配置
//
// Hyperliquid 适配说明：
//   - api_key:     存储 Agent 钱包私钥（十六进制，不含 0x 前缀）
//   - api_secret:  存储主钱包地址（十六进制，含 0x 前缀）
//   - symbol:      交易对名称（如 "HYPE"，不带 USDT 后缀）
//   - use_testnet: 是否使用 Hyperliquid 测试网
type ExchangeConfig struct {
	ApiKey     string `mapstructure:"api_key"`     // Agent 钱包私钥
	ApiSecret  string `mapstructure:"api_secret"`  // 主钱包地址
	Symbol     string `mapstructure:"symbol"`       // 交易对（如 "HYPE"）
	UseTestnet bool   `mapstructure:"use_testnet"`  // 是否使用测试网
}

// StrategyConfig 策略配置
type StrategyConfig struct {
	MaxSafetyOrders int     `mapstructure:"max_safety_orders"` // 最大网格层数
	AtrPeriod       int     `mapstructure:"atr_period"`        // ATR 计算周期
	BaseRatio       float64 `mapstructure:"base_ratio"`        // 头仓比例（余额 × base_ratio）
}

// StorageConfig 存储配置
type StorageConfig struct {
	SqlitePath string `mapstructure:"sqlite_path"`
	RedisAddr  string `mapstructure:"redis_addr"`
	RedisPass  string `mapstructure:"redis_pass"`
	RedisDB    int    `mapstructure:"redis_db"`
}

// LogConfig 日志配置
type LogConfig struct {
	Level string `mapstructure:"level"`
}

// HealthConfig 健康检查配置（★ P2 加固）
type HealthConfig struct {
	Addr string `mapstructure:"addr"` // 监听地址（如 ":8080"）
}

// LoadConfig 从 YAML 文件加载配置，环境变量可覆盖
func LoadConfig(path string) (*Config, error) {
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")

	// 环境变量覆盖
	viper.SetEnvPrefix("MARTIN")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
