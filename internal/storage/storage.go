package storage

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/uykb/MartinStrategy/internal/utils"
)

type Database struct {
	Sqlite *gorm.DB
	Redis  *redis.Client
}

// Order model for persisting state
type Order struct {
	ID        uint   `gorm:"primaryKey"`
	Symbol    string `gorm:"index"`
	OrderID   int64  `gorm:"uniqueIndex"`
	Side      string
	Type      string
	Price     float64
	Quantity  float64
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// BotState stores the current state of the FSM
type BotState struct {
	ID           uint `gorm:"primaryKey"`
	CurrentState string
	PositionSize float64
	AvgPrice     float64
	UpdatedAt    time.Time
}

func InitStorage(sqlitePath, redisAddr, redisPass string, redisDB int) (*Database, error) {
	// 确保 SQLite 数据库文件的父目录存在
	dir := filepath.Dir(sqlitePath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	// Initialize SQLite
	db, err := gorm.Open(sqlite.Open(sqlitePath), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Auto Migrate
	if err := db.AutoMigrate(&Order{}, &BotState{}); err != nil {
		return nil, err
	}

	// Initialize Redis（可选：仅当 redisAddr 非空时启用，不阻断启动）
	var rdb *redis.Client
	if redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{
			Addr:            redisAddr,
			Password:        redisPass,
			DB:              redisDB,
			MaxRetries:      1,
			PoolSize:        3,
			MinIdleConns:    1,
			ConnMaxIdleTime: 30 * time.Second,
		})

		// 测试连接，失败时关闭客户端并置空
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := rdb.Ping(ctx).Result(); err != nil {
			utils.Logger.Warn("Redis 连接失败，分布式锁功能不可用（单实例部署可忽略）",
				zap.String("addr", redisAddr),
				zap.Error(err))
			rdb.Close()
			rdb = nil
		} else {
			utils.Logger.Info("Redis 连接成功", zap.String("addr", redisAddr))
		}
	} else {
		utils.Logger.Info("未配置 Redis，分布式锁功能禁用（单实例部署正常）")
	}

	return &Database{
		Sqlite: db,
		Redis:  rdb,
	}, nil
}

// AcquireLock uses Redis to ensure single instance execution
func (d *Database) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if d.Redis == nil {
		// Redis 不可用时，始终返回 true（允许运行，无分布式锁保护）
		return true, nil
	}
	return d.Redis.SetNX(ctx, key, "locked", ttl).Result()
}
