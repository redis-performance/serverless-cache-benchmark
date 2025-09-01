package cmd

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient implements CacheClient for Redis
type RedisClient struct {
	client *redis.Client
}

// RedisConfig holds Redis connection configuration
type RedisConfig struct {
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolTimeout     time.Duration
	ConnMaxIdleTime time.Duration
	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration
}

func NewRedisClientFromURI(uri string, config RedisConfig) (*RedisClient, error) {
	opts, err := redis.ParseURL(uri)
	if err != nil {
		return nil, err
	}
	// Apply configurable timeouts and retry settings
	opts.DialTimeout = config.DialTimeout
	opts.ReadTimeout = config.ReadTimeout
	opts.WriteTimeout = config.WriteTimeout
	opts.ConnMaxIdleTime = config.ConnMaxIdleTime
	opts.PoolTimeout = config.PoolTimeout
	opts.MaxRetries = config.MaxRetries
	opts.MinRetryBackoff = config.MinRetryBackoff
	opts.MaxRetryBackoff = config.MaxRetryBackoff

	rdb := redis.NewClient(opts)
	return &RedisClient{client: rdb}, nil
}

func NewRedisClient(addr, password string, db int) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &RedisClient{client: rdb}
}

func (r *RedisClient) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

func (r *RedisClient) Close() error {
	return r.client.Close()
}

func (r *RedisClient) Name() string {
	return "Redis"
}
