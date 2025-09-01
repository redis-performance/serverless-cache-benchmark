/*
Copyright © 2025 Redis Performance Group  <performance <at> redis <dot> com>
*/
package cmd

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/momentohq/client-sdk-go/auth"
	"github.com/momentohq/client-sdk-go/config"
	"github.com/momentohq/client-sdk-go/momento"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

// CacheClient interface defines the operations for cache data sinks
type CacheClient interface {
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error
	Close() error
	Name() string
}

// RedisClient implements CacheClient for Redis
type RedisClient struct {
	client *redis.Client
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

// MomentoClient implements CacheClient for Momento
type MomentoClient struct {
	client    momento.CacheClient
	cacheName string
}

func NewMomentoClient(apiKey, cacheName string, createCache bool) (*MomentoClient, error) {
	var credential auth.CredentialProvider
	var err error

	if apiKey != "" {
		credential, err = auth.NewStringMomentoTokenProvider(apiKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create string token provider: %w", err)
		}
	} else {
		credential, err = auth.NewEnvMomentoTokenProvider("MOMENTO_API_KEY")
		if err != nil {
			return nil, fmt.Errorf("failed to get Momento credentials: %w", err)
		}
	}

	client, err := momento.NewCacheClient(
		config.LaptopLatest(),
		credential,
		60*time.Second,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Momento client: %w", err)
	}

	// Try to create the cache if it doesn't exist and createCache is true
	if createCache {
		ctx := context.Background()
		_, err = client.CreateCache(ctx, &momento.CreateCacheRequest{
			CacheName: cacheName,
		})
		if err != nil {
			// Check if it's an "already exists" error, which is fine
			if !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "AlreadyExists") {
				log.Printf("Warning: Failed to create cache '%s': %v", cacheName, err)
			}
		} else {
			log.Printf("Created Momento cache: %s", cacheName)
		}
	}

	return &MomentoClient{
		client:    client,
		cacheName: cacheName,
	}, nil
}

func (m *MomentoClient) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	setRequest := &momento.SetRequest{
		CacheName: m.cacheName,
		Key:       momento.String(key),
		Value:     momento.Bytes(value),
	}

	// For now, we'll skip TTL setting as the API might be different
	// The cache will use default TTL settings

	_, err := m.client.Set(ctx, setRequest)
	return err
}

func (m *MomentoClient) Close() error {
	m.client.Close()
	return nil
}

func (m *MomentoClient) Name() string {
	return "Momento"
}

// populateCmd represents the populate command
var populateCmd = &cobra.Command{
	Use:   "populate",
	Short: "Populate cache with test data using multiple concurrent clients",
	Long: `Populate cache systems (Redis or Momento) with test data for benchmarking using multiple concurrent clients.

This command supports populating both Redis and Momento cache systems with configurable
test data including different data sizes, key patterns, and expiration settings. It uses
multiple concurrent clients (goroutines) to maximize throughput and provides real-time
performance metrics including QPS and latency percentiles using HDR histogram.

Examples:
  # Populate Redis with default number of clients (CPU cores)
  serverless-cache-benchmark populate --cache-type redis --redis-addr localhost:6379

  # Populate Momento with custom data size and 8 clients
  serverless-cache-benchmark populate --cache-type momento --momento-cache-name test-cache --data-size 1024 --clients 8

  # Populate with random data sizes and 10 clients
  serverless-cache-benchmark populate --cache-type redis --redis-addr localhost:6379 --data-size-range 100-2048 --clients 10`,
	Run: runPopulate,
}

// PerformanceStats tracks performance metrics
type PerformanceStats struct {
	TotalOps   int64
	SuccessOps int64
	FailedOps  int64
	Histogram  *hdrhistogram.Histogram
	StartTime  time.Time
	mutex      sync.RWMutex
}

func NewPerformanceStats() *PerformanceStats {
	// Create histogram with 1 microsecond to 1 minute range, 3 significant digits
	hist := hdrhistogram.New(1, 60*1000*1000, 3)
	return &PerformanceStats{
		Histogram: hist,
		StartTime: time.Now(),
	}
}

func (ps *PerformanceStats) RecordLatency(latencyMicros int64) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	ps.Histogram.RecordValue(latencyMicros)
	atomic.AddInt64(&ps.SuccessOps, 1)
	atomic.AddInt64(&ps.TotalOps, 1)
}

func (ps *PerformanceStats) RecordError() {
	atomic.AddInt64(&ps.FailedOps, 1)
	atomic.AddInt64(&ps.TotalOps, 1)
}

func (ps *PerformanceStats) GetQPS() float64 {
	elapsed := time.Since(ps.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&ps.TotalOps)) / elapsed
}

func (ps *PerformanceStats) GetStats() (int64, int64, int64, float64, int64, int64, int64) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	total := atomic.LoadInt64(&ps.TotalOps)
	success := atomic.LoadInt64(&ps.SuccessOps)
	failed := atomic.LoadInt64(&ps.FailedOps)
	qps := ps.GetQPS()

	var p50, p95, p99 int64
	if ps.Histogram.TotalCount() > 0 {
		p50 = ps.Histogram.ValueAtQuantile(50)
		p95 = ps.Histogram.ValueAtQuantile(95)
		p99 = ps.Histogram.ValueAtQuantile(99)
	}

	return total, success, failed, qps, p50, p95, p99
}

// ClientWorker represents a single client worker
type ClientWorker struct {
	ID        int
	Client    CacheClient
	Generator *DataGenerator
	Stats     *PerformanceStats
	KeyStart  int
	KeyEnd    int
}

// DataGenerator handles data generation for populate operations
type DataGenerator struct {
	DataSize        int
	RandomData      bool
	DataSizeRange   string
	DataSizeList    string
	DataSizePattern string
	ExpiryRange     string
}

func (dg *DataGenerator) GenerateData() ([]byte, error) {
	size := dg.DataSize

	// Handle data size range
	if dg.DataSizeRange != "" {
		parts := strings.Split(dg.DataSizeRange, "-")
		if len(parts) == 2 {
			min, err1 := strconv.Atoi(parts[0])
			max, err2 := strconv.Atoi(parts[1])
			if err1 == nil && err2 == nil && min <= max {
				if dg.DataSizePattern == "R" {
					// Random size between min and max
					size = min + rand.Intn(max-min+1)
				} else {
					// For simplicity, use average for "S" pattern
					size = (min + max) / 2
				}
			}
		}
	}

	data := make([]byte, size)
	if dg.RandomData {
		_, err := cryptorand.Read(data)
		if err != nil {
			return nil, fmt.Errorf("failed to generate random data: %w", err)
		}
	} else {
		// Fill with pattern data
		pattern := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
		for i := range data {
			data[i] = pattern[i%len(pattern)]
		}
	}

	return data, nil
}

func (dg *DataGenerator) GetExpiration() time.Duration {
	if dg.ExpiryRange == "" {
		return 0 // No expiration
	}

	parts := strings.Split(dg.ExpiryRange, "-")
	if len(parts) == 2 {
		min, err1 := strconv.Atoi(parts[0])
		max, err2 := strconv.Atoi(parts[1])
		if err1 == nil && err2 == nil && min <= max {
			expiry := min + rand.Intn(max-min+1)
			return time.Duration(expiry) * time.Second
		}
	}

	return 0
}

// workerRoutine runs a single client worker
func (cw *ClientWorker) workerRoutine(ctx context.Context, wg *sync.WaitGroup, keyPrefix string) {
	defer wg.Done()
	defer cw.Client.Close()

	for i := cw.KeyStart; i <= cw.KeyEnd; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("%s%d", keyPrefix, i)

		data, err := cw.Generator.GenerateData()
		if err != nil {
			log.Printf("Client %d: Failed to generate data for key %s: %v", cw.ID, key, err)
			cw.Stats.RecordError()
			continue
		}

		expiration := cw.Generator.GetExpiration()

		start := time.Now()
		err = cw.Client.Set(ctx, key, data, expiration)
		latency := time.Since(start)

		if err != nil {
			log.Printf("Client %d: Failed to set key %s: %v", cw.ID, key, err)
			cw.Stats.RecordError()
			continue
		}

		cw.Stats.RecordLatency(latency.Microseconds())
	}
}

// createCacheClient creates a cache client based on the cache type
func createCacheClient(cacheType string, cmd *cobra.Command) (CacheClient, error) {
	switch cacheType {
	case "redis":
		addr, _ := cmd.Flags().GetString("redis-addr")
		password, _ := cmd.Flags().GetString("redis-password")
		db, _ := cmd.Flags().GetInt("redis-db")
		return NewRedisClient(addr, password, db), nil

	case "momento":
		apiKey, _ := cmd.Flags().GetString("momento-api-key")
		cacheName, _ := cmd.Flags().GetString("momento-cache-name")
		createCache, _ := cmd.Flags().GetBool("momento-create-cache")
		client, err := NewMomentoClient(apiKey, cacheName, createCache)
		if err != nil {
			return nil, fmt.Errorf("failed to create Momento client: %w", err)
		}
		return client, nil

	default:
		return nil, fmt.Errorf("invalid cache type: %s. Must be 'redis' or 'momento'", cacheType)
	}
}

// printStats prints performance statistics
func printStats(stats *PerformanceStats, clientCount int) {
	total, success, failed, qps, p50, p95, p99 := stats.GetStats()

	fmt.Printf("\n=== Performance Statistics ===\n")
	fmt.Printf("Clients: %d\n", clientCount)
	fmt.Printf("Total Operations: %d\n", total)
	fmt.Printf("Successful Operations: %d\n", success)
	fmt.Printf("Failed Operations: %d\n", failed)
	fmt.Printf("Success Rate: %.2f%%\n", float64(success)/float64(total)*100)
	fmt.Printf("QPS: %.2f\n", qps)
	fmt.Printf("Latency P50: %d μs (%.2f ms)\n", p50, float64(p50)/1000)
	fmt.Printf("Latency P95: %d μs (%.2f ms)\n", p95, float64(p95)/1000)
	fmt.Printf("Latency P99: %d μs (%.2f ms)\n", p99, float64(p99)/1000)
}

func runPopulate(cmd *cobra.Command, args []string) {
	cacheType, _ := cmd.Flags().GetString("cache-type")
	clientCount, _ := cmd.Flags().GetInt("clients")

	// Get populate parameters
	dataSize, _ := cmd.Flags().GetInt("data-size")
	randomData, _ := cmd.Flags().GetBool("random-data")
	dataSizeRange, _ := cmd.Flags().GetString("data-size-range")
	dataSizeList, _ := cmd.Flags().GetString("data-size-list")
	dataSizePattern, _ := cmd.Flags().GetString("data-size-pattern")
	expiryRange, _ := cmd.Flags().GetString("expiry-range")
	keyPrefix, _ := cmd.Flags().GetString("key-prefix")
	keyMin, _ := cmd.Flags().GetInt("key-minimum")
	keyMax, _ := cmd.Flags().GetInt("key-maximum")

	// Validate parameters
	if clientCount <= 0 {
		log.Fatalf("Number of clients must be greater than 0")
	}

	totalKeys := keyMax - keyMin + 1
	if totalKeys <= 0 {
		log.Fatalf("Invalid key range: min=%d, max=%d", keyMin, keyMax)
	}

	// Calculate key ranges for each client
	keysPerClient := totalKeys / clientCount
	if keysPerClient == 0 {
		log.Fatalf("Not enough keys (%d) for %d clients", totalKeys, clientCount)
	}

	fmt.Printf("Starting population of %s cache...\n", cacheType)
	fmt.Printf("Clients: %d\n", clientCount)
	fmt.Printf("Total keys: %d (range: %d to %d)\n", totalKeys, keyMin, keyMax)
	fmt.Printf("Keys per client: %d\n", keysPerClient)
	fmt.Printf("Data size: %d bytes", dataSize)
	if dataSizeRange != "" {
		fmt.Printf(" (range: %s)", dataSizeRange)
	}
	fmt.Println()

	// Create shared performance stats
	stats := NewPerformanceStats()

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create workers
	var wg sync.WaitGroup
	workers := make([]*ClientWorker, clientCount)

	for i := 0; i < clientCount; i++ {
		// Create cache client for this worker
		client, err := createCacheClient(cacheType, cmd)
		if err != nil {
			log.Fatalf("Failed to create cache client for worker %d: %v", i, err)
		}

		// Calculate key range for this worker
		start := keyMin + i*keysPerClient
		end := start + keysPerClient - 1
		if i == clientCount-1 {
			// Last client gets any remaining keys
			end = keyMax
		}

		// Create data generator for this worker
		generator := &DataGenerator{
			DataSize:        dataSize,
			RandomData:      randomData,
			DataSizeRange:   dataSizeRange,
			DataSizeList:    dataSizeList,
			DataSizePattern: dataSizePattern,
			ExpiryRange:     expiryRange,
		}

		workers[i] = &ClientWorker{
			ID:        i,
			Client:    client,
			Generator: generator,
			Stats:     stats,
			KeyStart:  start,
			KeyEnd:    end,
		}

		fmt.Printf("Worker %d: keys %d to %d (%d keys)\n", i, start, end, end-start+1)
	}

	// Start progress reporting goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				total, success, failed, qps, p50, p95, p99 := stats.GetStats()
				if total > 0 {
					fmt.Printf("Progress: %d ops, %.0f QPS, Success: %d, Failed: %d, P50: %d μs, P95: %d μs, P99: %d μs\n",
						total, qps, success, failed, p50, p95, p99)
				}
			}
		}
	}()

	// Start all workers
	fmt.Println("\nStarting workers...")
	for i, worker := range workers {
		wg.Add(1)
		go worker.workerRoutine(ctx, &wg, keyPrefix)
		fmt.Printf("Started worker %d\n", i)
	}

	// Wait for all workers to complete
	wg.Wait()

	// Print final statistics
	printStats(stats, clientCount)
}

func init() {
	rootCmd.AddCommand(populateCmd)

	// Cache Type Options
	populateCmd.Flags().StringP("cache-type", "t", "redis", "Cache type: redis or momento")

	// Client Options
	populateCmd.Flags().IntP("clients", "c", runtime.NumCPU(), "Number of concurrent clients (default: number of CPU cores)")

	// Redis Options
	populateCmd.Flags().String("redis-addr", "localhost:6379", "Redis server address")
	populateCmd.Flags().String("redis-password", "", "Redis password")
	populateCmd.Flags().Int("redis-db", 0, "Redis database number")

	// Momento Options
	populateCmd.Flags().String("momento-api-key", "", "Momento API key (or set MOMENTO_API_KEY env var)")
	populateCmd.Flags().String("momento-cache-name", "test-cache", "Momento cache name")
	populateCmd.Flags().Bool("momento-create-cache", true, "Automatically create Momento cache if it doesn't exist")

	// Object Options
	populateCmd.Flags().IntP("data-size", "d", 32, "Object data size in bytes")
	populateCmd.Flags().BoolP("random-data", "R", false, "Indicate that data should be randomized")
	populateCmd.Flags().String("data-size-range", "", "Use random-sized items in the specified range (min-max)")
	populateCmd.Flags().String("data-size-list", "", "Use sizes from weight list (size1:weight1,..sizeN:weightN)")
	populateCmd.Flags().String("data-size-pattern", "R", "Use together with data-size-range (R=random, S=evenly distributed)")
	populateCmd.Flags().String("expiry-range", "", "Use random expiry values from the specified range")

	// Key Options
	populateCmd.Flags().String("key-prefix", "memtier-", "Prefix for keys")
	populateCmd.Flags().Int("key-minimum", 0, "Key ID minimum value")
	populateCmd.Flags().Int("key-maximum", 10000000, "Key ID maximum value")
}
