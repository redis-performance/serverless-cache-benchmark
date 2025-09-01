/*
Copyright © 2025 Redis Performance Group  <performance <at> redis <dot> com>
*/
package cmd

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
)

// CacheClient interface defines the operations for cache data sinks
type CacheClient interface {
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error
	Close() error
	Name() string
}

// populateCmd represents the populate command
var populateCmd = &cobra.Command{
	Use:   "populate",
	Short: "Populate cache with test data using multiple concurrent clients",
	Long: `Populate cache systems (Redis or Momento) with test data for benchmarking using multiple concurrent clients.

This command supports populating both Redis and Momento cache systems with configurable
test data including different data sizes, key patterns, and expiration settings. It uses
multiple concurrent clients (goroutines) with optional rate limiting and provides real-time
performance metrics including QPS and per-second latency percentiles using HDR histogram.

Examples:
  # Populate Redis with default number of clients (4, optimized for I/O), unlimited rate
  serverless-cache-benchmark populate --cache-type redis --redis-uri redis://localhost:6379

  # Populate Redis with authentication and database selection
  serverless-cache-benchmark populate --cache-type redis --redis-uri redis://user:pass@localhost:6380/2

  # Populate Redis with TLS connection
  serverless-cache-benchmark populate --cache-type redis --redis-uri rediss://localhost:6380

  # Populate Momento with rate limiting at 1000 RPS total
  serverless-cache-benchmark populate --cache-type momento --momento-cache-name test-cache --rps 1000

  # Populate with 4 clients at 500 RPS (125 RPS per client)
  serverless-cache-benchmark populate --cache-type redis --redis-uri redis://localhost:6379 --clients 4 --rps 500

  # Populate with custom Redis timeouts for high-load scenarios
  serverless-cache-benchmark populate --cache-type redis --redis-dial-timeout 30 --redis-read-timeout 30 --redis-max-retries 5`,
	Run: runPopulate,
}

// ClientWorker represents a single client worker
type ClientWorker struct {
	ID        int
	Client    CacheClient
	Stats     *PerformanceStats
	Limiter   *rate.Limiter // Rate limiter for this client
	Generator *DataGenerator
	KeyStart  int
	KeyEnd    int
}

// workerRoutine runs a single client worker with rate limiting and channel-based stats
func (cw *ClientWorker) workerRoutine(ctx context.Context, wg *sync.WaitGroup, keyPrefix string, verbose bool, timeoutSeconds int) {
	defer wg.Done()
	defer cw.Client.Close()

	var errorCount int64
	const maxErrorsToLog = 10 // Limit error logging to prevent spam

	for i := cw.KeyStart; i <= cw.KeyEnd; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Apply rate limiting if configured
		if cw.Limiter != nil {
			err := cw.Limiter.Wait(ctx)
			if err != nil {
				if verbose && errorCount < maxErrorsToLog {
					log.Printf("Client %d: Rate limiter error: %v", cw.ID, err)
				}
				return
			}
		}

		key := fmt.Sprintf("%s%d", keyPrefix, i)

		data, err := cw.Generator.GenerateData()
		if err != nil {
			if verbose && errorCount < maxErrorsToLog {
				log.Printf("Client %d: Failed to generate data for key %s: %v", cw.ID, key, err)
			}
			cw.Stats.RecordError()
			errorCount++
			continue
		}

		expiration := cw.Generator.GetExpiration()

		// Create a timeout context for this operation
		opCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
		start := time.Now()
		err = cw.Client.Set(opCtx, key, data, expiration)
		latency := time.Since(start)
		cancel()

		if err != nil {
			if verbose && errorCount < maxErrorsToLog {
				log.Printf("Client %d: Failed to set key %s: %v", cw.ID, key, err)
			} else if errorCount == maxErrorsToLog {
				log.Printf("Client %d: Suppressing further error logs (too many errors)", cw.ID)
			}
			cw.Stats.RecordError()
			errorCount++
			continue
		}

		// Channel-based stats recording (lock-free, non-blocking)
		cw.Stats.RecordLatency(latency.Microseconds())
	}

	// Don't log completion messages - they're just noise
}

// createCacheClient creates a cache client based on the cache type
func createCacheClient(cacheType string, cmd *cobra.Command) (CacheClient, error) {
	switch cacheType {
	case "redis":
		uri, _ := cmd.Flags().GetString("redis-uri")

		// Build Redis configuration from flags
		dialTimeout, _ := cmd.Flags().GetInt("redis-dial-timeout")
		readTimeout, _ := cmd.Flags().GetInt("redis-read-timeout")
		writeTimeout, _ := cmd.Flags().GetInt("redis-write-timeout")
		poolTimeout, _ := cmd.Flags().GetInt("redis-pool-timeout")
		connMaxIdleTime, _ := cmd.Flags().GetInt("redis-conn-max-idle-time")
		maxRetries, _ := cmd.Flags().GetInt("redis-max-retries")
		minRetryBackoff, _ := cmd.Flags().GetInt("redis-min-retry-backoff")
		maxRetryBackoff, _ := cmd.Flags().GetInt("redis-max-retry-backoff")

		config := RedisConfig{
			DialTimeout:     time.Duration(dialTimeout) * time.Second,
			ReadTimeout:     time.Duration(readTimeout) * time.Second,
			WriteTimeout:    time.Duration(writeTimeout) * time.Second,
			PoolTimeout:     time.Duration(poolTimeout) * time.Second,
			ConnMaxIdleTime: time.Duration(connMaxIdleTime) * time.Second,
			MaxRetries:      maxRetries,
			MinRetryBackoff: time.Duration(minRetryBackoff) * time.Millisecond,
			MaxRetryBackoff: time.Duration(maxRetryBackoff) * time.Millisecond,
		}

		client, err := NewRedisClientFromURI(uri, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create Redis client from URI '%s': %w", uri, err)
		}
		return client, nil

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
	rps, _ := cmd.Flags().GetInt("rps")
	timeoutSeconds, _ := cmd.Flags().GetInt("timeout")
	verbose, _ := cmd.Flags().GetBool("verbose")

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
	if rps > 0 {
		fmt.Printf("Rate limit: %d RPS total (%.2f RPS per client)\n", rps, float64(rps)/float64(clientCount))
	} else {
		fmt.Printf("Rate limit: unlimited\n")
	}
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

		// Create rate limiter for this client if RPS is specified
		var limiter *rate.Limiter
		if rps > 0 {
			clientRPS := float64(rps) / float64(clientCount)
			limiter = rate.NewLimiter(rate.Limit(clientRPS), 1) // Burst of 1
		}

		workers[i] = &ClientWorker{
			ID:        i,
			Client:    client,
			Generator: generator,
			Stats:     stats,
			Limiter:   limiter,
			KeyStart:  start,
			KeyEnd:    end,
		}

		if verbose {
			fmt.Printf("Worker %d: keys %d to %d (%d keys)\n", i, start, end, end-start+1)
		}
	}

	// Start progress reporting goroutine with reduced frequency to minimize overhead
	go func() {
		ticker := time.NewTicker(2 * time.Second) // Reduced frequency
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Only collect stats if we have significant operations to reduce lock contention
				total, success, failed, qps := stats.GetOverallStats()
				if total > 100 { // Only report if we have meaningful progress
					count, p50, p95, p99, max := stats.GetCurrentSecondStats()
					if count > 0 {
						fmt.Printf("Progress: %d ops, %.0f QPS, Success: %d, Failed: %d | Last 2s: %d ops, P50: %d μs, P95: %d μs, P99: %d μs, Max: %d μs\n",
							total, qps, success, failed, count, p50, p95, p99, max)
					} else {
						fmt.Printf("Progress: %d ops, %.0f QPS, Success: %d, Failed: %d | Last 2s: no operations\n",
							total, qps, success, failed)
					}
				}
			}
		}
	}()

	// Start all workers
	if verbose {
		fmt.Println("\nStarting workers...")
	} else {
		fmt.Printf("\nStarting %d workers...\n", clientCount)
	}

	for i, worker := range workers {
		wg.Add(1)
		go worker.workerRoutine(ctx, &wg, keyPrefix, verbose, timeoutSeconds)
		if verbose {
			fmt.Printf("Started worker %d\n", i)
		}
	}

	// Wait for all workers to complete
	wg.Wait()

	// Close the stats collector
	stats.Close()

	// Print final statistics
	printStats(stats, clientCount)
}

func init() {
	rootCmd.AddCommand(populateCmd)

	// Cache Type Options
	populateCmd.Flags().StringP("cache-type", "t", "redis", "Cache type: redis or momento")

	// Client Options
	defaultClients := runtime.NumCPU()
	populateCmd.Flags().IntP("clients", "c", defaultClients, "Number of concurrent clients (default: 4, optimized for I/O)")
	populateCmd.Flags().IntP("rps", "r", 0, "Rate limit in requests per second (0 = unlimited)")
	populateCmd.Flags().IntP("timeout", "T", 10, "Operation timeout in seconds")
	populateCmd.Flags().BoolP("verbose", "v", false, "Enable verbose output (show worker details)")

	// Redis Options
	populateCmd.Flags().StringP("redis-uri", "u", "redis://localhost:6379", "Redis URI (redis://[username[:password]@]host[:port][/db-number] or rediss:// for TLS)")
	populateCmd.Flags().Int("redis-dial-timeout", 30, "Redis dial timeout in seconds")
	populateCmd.Flags().Int("redis-read-timeout", 30, "Redis read timeout in seconds")
	populateCmd.Flags().Int("redis-write-timeout", 30, "Redis write timeout in seconds")
	populateCmd.Flags().Int("redis-pool-timeout", 30, "Redis connection pool timeout in seconds")
	populateCmd.Flags().Int("redis-conn-max-idle-time", 30, "Redis connection max idle time in seconds")
	populateCmd.Flags().Int("redis-max-retries", 3, "Redis maximum number of retries")
	populateCmd.Flags().Int("redis-min-retry-backoff", 1000, "Redis minimum retry backoff in milliseconds")
	populateCmd.Flags().Int("redis-max-retry-backoff", 10000, "Redis maximum retry backoff in milliseconds")

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
