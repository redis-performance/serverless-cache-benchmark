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

func NewRedisClientFromURI(uri string) (*RedisClient, error) {
	opts, err := redis.ParseURL(uri)
	if err != nil {
		return nil, err
	}
	opts.ConnMaxIdleTime = 5 * time.Second
	opts.PoolTimeout = 5 * time.Second
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
