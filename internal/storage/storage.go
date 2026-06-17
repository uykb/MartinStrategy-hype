package storage

import (
	"context"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
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
	// Initialize SQLite
	db, err := gorm.Open(sqlite.Open(sqlitePath), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Auto Migrate
	if err := db.AutoMigrate(&Order{}, &BotState{}); err != nil {
		return nil, err
	}

	// Initialize Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPass,
		DB:       redisDB,
	})

	return &Database{
		Sqlite: db,
		Redis:  rdb,
	}, nil
}

// AcquireLock uses Redis to ensure single instance execution
func (d *Database) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return d.Redis.SetNX(ctx, key, "locked", ttl).Result()
}
