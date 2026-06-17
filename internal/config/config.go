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
	"fmt"
	"os"
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

// LoadConfig 加载配置，支持 YAML 文件 + 环境变量覆盖（前缀 MARTIN_）。
// 如果 config.yaml 不存在，则纯靠环境变量 + 默认值运行（适合 Docker 部署）。
func LoadConfig(path string) (*Config, error) {
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")

	// 设置默认值（Docker 部署时无需 config.yaml）
	viper.SetDefault("exchange.symbol", "HYPE")
	viper.SetDefault("exchange.use_testnet", false)
	viper.SetDefault("strategy.max_safety_orders", 9)
	viper.SetDefault("strategy.atr_period", 14)
	viper.SetDefault("strategy.base_ratio", 0.05)
	viper.SetDefault("storage.sqlite_path", "bot.db")
	viper.SetDefault("storage.redis_addr", "localhost:6379")
	viper.SetDefault("storage.redis_db", 0)
	viper.SetDefault("log.level", "info")
	viper.SetDefault("health.addr", ":8080")

	// 环境变量覆盖（前缀 MARTIN_，如 MARTIN_EXCHANGE_API_KEY）
	viper.SetEnvPrefix("MARTIN")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// config.yaml 可选：不存在时不报错，纯靠环境变量 + 默认值
	if err := viper.ReadInConfig(); err != nil {
		if !os.IsNotExist(err) {
			// 文件存在但格式错误，仍然报错
			return nil, fmt.Errorf("读取配置文件失败: %w", err)
		}
		// 文件不存在：使用环境变量 + 默认值
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}
	return &cfg, nil
}
