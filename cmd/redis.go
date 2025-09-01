package cmd

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient implements CacheClient for Redis
type RedisClient struct {
	client        *redis.Client
	clusterClient *redis.ClusterClient
	isCluster     bool
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
	ClusterMode     bool
}

func NewRedisClientFromURI(uri string, config RedisConfig) (*RedisClient, error) {
	if config.ClusterMode {
		return NewRedisClusterClientFromURI(uri, config)
	}

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
	return &RedisClient{client: rdb, isCluster: false}, nil
}

func NewRedisClusterClientFromURI(uri string, config RedisConfig) (*RedisClient, error) {
	opts, err := redis.ParseURL(uri)
	if err != nil {
		return nil, err
	}

	// Create cluster options from single node options
	clusterOpts := &redis.ClusterOptions{
		Addrs:           []string{opts.Addr}, // Start with single address, cluster discovery will find others
		Password:        opts.Password,
		DialTimeout:     config.DialTimeout,
		ReadTimeout:     config.ReadTimeout,
		WriteTimeout:    config.WriteTimeout,
		PoolTimeout:     config.PoolTimeout,
		ConnMaxIdleTime: config.ConnMaxIdleTime,
		MaxRetries:      config.MaxRetries,
		MinRetryBackoff: config.MinRetryBackoff,
		MaxRetryBackoff: config.MaxRetryBackoff,
	}

	// Apply TLS settings if the URI uses rediss://
	if opts.TLSConfig != nil {
		clusterOpts.TLSConfig = opts.TLSConfig
	}

	rdb := redis.NewClusterClient(clusterOpts)
	return &RedisClient{clusterClient: rdb, isCluster: true}, nil
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
	if r.isCluster {
		return r.clusterClient.Set(ctx, key, value, expiration).Err()
	}
	return r.client.Set(ctx, key, value, expiration).Err()
}

func (r *RedisClient) Get(ctx context.Context, key string) ([]byte, error) {
	var result string
	var err error

	if r.isCluster {
		result, err = r.clusterClient.Get(ctx, key).Result()
	} else {
		result, err = r.client.Get(ctx, key).Result()
	}

	if err != nil {
		return nil, err
	}
	return []byte(result), nil
}

func (r *RedisClient) Ping(ctx context.Context) error {
	if r.isCluster {
		return r.clusterClient.Ping(ctx).Err()
	}
	return r.client.Ping(ctx).Err()
}

func (r *RedisClient) Close() error {
	if r.isCluster {
		return r.clusterClient.Close()
	}
	return r.client.Close()
}

func (r *RedisClient) Name() string {
	if r.isCluster {
		return "Redis Cluster"
	}
	return "Redis"
}
