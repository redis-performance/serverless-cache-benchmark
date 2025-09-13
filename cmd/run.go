/*
Copyright © 2025 Redis Performance Group  <performance <at> redis <dot> com>
*/
package cmd

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/spf13/cobra"
	"github.com/vishvananda/netlink"
	"golang.org/x/time/rate"
)

// ZipfGenerator generates keys following Zipf distribution
type ZipfGenerator struct {
	zipf *rand.Zipf
	max  uint64
}

func NewZipfGenerator(max uint64, exponent float64, seed int64) *ZipfGenerator {
	if exponent <= 0 || exponent > 5 {
		exponent = 1.0 // Default safe value
	}

	// Ensure max is at least 1
	if max < 1 {
		max = 1
	}

	source := rand.NewSource(seed)
	rng := rand.New(source)

	// The Go Zipf implementation can return nil in some cases
	// Let's add some debugging and fallback
	zipf := rand.NewZipf(rng, exponent, 1, max)
	if zipf == nil {
		// Fallback: try with different parameters
		zipf = rand.NewZipf(rng, 1.1, 1, max)
		if zipf == nil {
			// Last resort: use uniform distribution
			fmt.Printf("Warning: Zipf generator failed, falling back to uniform distribution\n")
		}
	}

	return &ZipfGenerator{
		zipf: zipf,
		max:  max,
	}
}

func (zg *ZipfGenerator) Next() uint64 {
	if zg.zipf == nil {
		// Fallback to uniform distribution
		return uint64(rand.Intn(int(zg.max))) + 1
	}
	return zg.zipf.Uint64()
}

// TrafficConfig represents a traffic configuration at a specific time
type TrafficConfig struct {
	TimeSeconds int
	Clients     int
	QPS         int // -1 means unlimited
}

// TimeBlockStats tracks actual performance during a time block
type TimeBlockStats struct {
	Config       TrafficConfig
	StartTime    time.Time
	EndTime      time.Time
	ActualGetOps int64
	ActualSetOps int64
	GetErrors    int64
	SetErrors    int64
	GetStats     *PerformanceStats
	SetStats     *PerformanceStats
}

// CSVLogger handles CSV output of performance metrics
type CSVLogger struct {
	file   *os.File
	writer *csv.Writer
	mutex  sync.Mutex
}

// MetricsSnapshot represents a point-in-time snapshot of metrics
type MetricsSnapshot struct {
	Timestamp         time.Time
	ElapsedSeconds    int
	TargetClients     int
	ActualClients     int
	FailedConnections int64
	TargetQPS         int
	ActualTotalQPS    float64
	ActualGetQPS      float64
	ActualSetQPS      float64
	TotalOps          int64
	GetOps            int64
	SetOps            int64
	GetErrors         int64
	SetErrors         int64
	GetLatencyP50     int64
	GetLatencyP95     int64
	GetLatencyP99     int64
	GetLatencyMax     int64
	SetLatencyP50     int64
	SetLatencyP95     int64
	SetLatencyP99     int64
	SetLatencyMax     int64
	NetworkRxMBps     float64
	NetworkTxMBps     float64
	NetworkRxPPS      float64
	NetworkTxPPS      float64
	MemoryUsedGB      float64
	MemoryTotalGB     float64
	CPUPercent        float64
	ProcessMemoryGB   float64
	TotalOutBoundConn int // Legacy TCP connection count from /proc
	ActiveTCPConns    int // Active TCP connections from netlink
	TCPConnDelta      int // Change in TCP connections from previous second
}

// WorkloadStats tracks workload performance metrics
type WorkloadStats struct {
	GetOps            int64
	SetOps            int64
	GetErrors         int64
	SetErrors         int64
	ActiveConnections int64 // Number of active connections
	FailedConnections int64 // Number of failed connection attempts
	GetStats          *PerformanceStats
	SetStats          *PerformanceStats
	SetupStats        *PerformanceStats // For client setup time measurement
	TimeBlocks        []TimeBlockStats  // Performance per time block
	CurrentBlock      *TimeBlockStats   // Currently active time block
	BlockMutex        sync.RWMutex      // Protects time block operations
	CSVLogger         *CSVLogger        // CSV output logger
}

// CloudWatchConfig holds CloudWatch configuration
type CloudWatchConfig struct {
	Enabled   bool
	Region    string
	Namespace string
	Client    *cloudwatch.Client
	Hostname  string
}

func NewWorkloadStats() *WorkloadStats {
	return &WorkloadStats{
		GetStats:   NewPerformanceStats(),
		SetStats:   NewPerformanceStats(),
		SetupStats: NewPerformanceStats(),
		TimeBlocks: make([]TimeBlockStats, 0),
	}
}

// NewCloudWatchConfig creates a new CloudWatch configuration
func NewCloudWatchConfig(enabled bool, region, namespace string) (*CloudWatchConfig, error) {
	cwConfig := &CloudWatchConfig{
		Enabled:   enabled,
		Region:    region,
		Namespace: namespace,
	}

	if !enabled {
		return cwConfig, nil
	}

	// Get hostname
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
		log.Printf("Warning: Failed to get hostname: %v", err)
	}
	cwConfig.Hostname = hostname

	// Load AWS config
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create CloudWatch client
	cwConfig.Client = cloudwatch.NewFromConfig(awsCfg)

	return cwConfig, nil
}

// emitCloudWatchMetrics emits metrics to CloudWatch
func (cw *CloudWatchConfig) emitCloudWatchMetrics(getOps, setOps, totalQPS float64, getP99, setP99 int64, tcpConnCount, activeTcpConns, tcpConnDelta int) {
	if !cw.Enabled || cw.Client == nil {
		return
	}

	// Create metric data
	now := time.Now()
	metricData := []types.MetricDatum{
		{
			MetricName: aws.String("GetOpsPerSecond"),
			Value:      aws.Float64(getOps),
			Unit:       types.StandardUnitCountSecond,
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("Host"),
					Value: aws.String(cw.Hostname),
				},
				{
					Name:  aws.String("LoadTest"),
					Value: aws.String("MomentoPerf"),
				},
			},
			Timestamp:         &now,
			StorageResolution: aws.Int32(1),
		},
		{
			MetricName: aws.String("SetOpsPerSecond"),
			Value:      aws.Float64(setOps),
			Unit:       types.StandardUnitCountSecond,
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("Host"),
					Value: aws.String(cw.Hostname),
				},
				{
					Name:  aws.String("LoadTest"),
					Value: aws.String("MomentoPerf"),
				},
			},
			Timestamp:         &now,
			StorageResolution: aws.Int32(1),
		},
		{
			MetricName: aws.String("TotalQPS"),
			Value:      aws.Float64(totalQPS),
			Unit:       types.StandardUnitCountSecond,
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("Host"),
					Value: aws.String(cw.Hostname),
				},
				{
					Name:  aws.String("LoadTest"),
					Value: aws.String("MomentoPerf"),
				},
			},
			Timestamp:         &now,
			StorageResolution: aws.Int32(1),
		},
		{
			MetricName: aws.String("GetLatencyP99"),
			Value:      aws.Float64(float64(getP99)),
			Unit:       types.StandardUnitMicroseconds,
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("Host"),
					Value: aws.String(cw.Hostname),
				},
				{
					Name:  aws.String("LoadTest"),
					Value: aws.String("MomentoPerf"),
				},
			},
			Timestamp:         &now,
			StorageResolution: aws.Int32(1),
		},
		{
			MetricName: aws.String("SetLatencyP99"),
			Value:      aws.Float64(float64(setP99)),
			Unit:       types.StandardUnitMicroseconds,
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("Host"),
					Value: aws.String(cw.Hostname),
				},
				{
					Name:  aws.String("LoadTest"),
					Value: aws.String("MomentoPerf"),
				},
			},
			Timestamp:         &now,
			StorageResolution: aws.Int32(1),
		},
		{
			MetricName: aws.String("TcpConnections"),
			Value:      aws.Float64(float64(tcpConnCount)),
			Unit:       types.StandardUnitCount,
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("Host"),
					Value: aws.String(cw.Hostname),
				},
				{
					Name:  aws.String("LoadTest"),
					Value: aws.String("MomentoPerf"),
				},
			},
			Timestamp:         &now,
			StorageResolution: aws.Int32(1),
		},
		{
			MetricName: aws.String("ActiveTcpConnections"),
			Value:      aws.Float64(float64(activeTcpConns)),
			Unit:       types.StandardUnitCount,
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("Host"),
					Value: aws.String(cw.Hostname),
				},
				{
					Name:  aws.String("LoadTest"),
					Value: aws.String("MomentoPerf"),
				},
			},
			Timestamp:         &now,
			StorageResolution: aws.Int32(1),
		},
		{
			MetricName: aws.String("TcpConnectionDelta"),
			Value:      aws.Float64(float64(tcpConnDelta)),
			Unit:       types.StandardUnitCount,
			Dimensions: []types.Dimension{
				{
					Name:  aws.String("Host"),
					Value: aws.String(cw.Hostname),
				},
				{
					Name:  aws.String("LoadTest"),
					Value: aws.String("MomentoPerf"),
				},
			},
			Timestamp:         &now,
			StorageResolution: aws.Int32(1),
		},
	}

	// Emit metrics asynchronously
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := cw.Client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
			Namespace:  aws.String(cw.Namespace),
			MetricData: metricData,
		})

		if err != nil {
			log.Printf("Warning: Failed to emit CloudWatch metrics: %v", err)
		}
	}()
}

// NewCSVLogger creates a new CSV logger with the specified filename
func NewCSVLogger(filename string) (*CSVLogger, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}

	writer := csv.NewWriter(file)
	logger := &CSVLogger{
		file:   file,
		writer: writer,
	}

	// Write CSV header
	header := []string{
		"timestamp", "elapsed_seconds", "target_clients", "actual_clients", "failed_connections", "target_qps",
		"actual_total_qps", "actual_get_qps", "actual_set_qps",
		"total_ops", "get_ops", "set_ops", "get_errors", "set_errors",
		"get_latency_p50_us", "get_latency_p95_us", "get_latency_p99_us", "get_latency_max_us",
		"set_latency_p50_us", "set_latency_p95_us", "set_latency_p99_us", "set_latency_max_us",
		"network_rx_mbps", "network_tx_mbps", "network_rx_pps", "network_tx_pps",
		"memory_used_gb", "memory_total_gb", "cpu_percent", "process_memory_gb",
		"total_out_bound_conn", "active_tcp_conns", "tcp_conn_delta",
	}

	if err := writer.Write(header); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}
	writer.Flush()

	return logger, nil
}

// LogMetrics writes a metrics snapshot to the CSV file
func (cl *CSVLogger) LogMetrics(snapshot MetricsSnapshot) error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	targetQPSStr := strconv.Itoa(snapshot.TargetQPS)
	if snapshot.TargetQPS == -1 {
		targetQPSStr = "unlimited"
	}

	record := []string{
		snapshot.Timestamp.Format(time.RFC3339),
		strconv.Itoa(snapshot.ElapsedSeconds),
		strconv.Itoa(snapshot.TargetClients),
		strconv.Itoa(snapshot.ActualClients),
		strconv.FormatInt(snapshot.FailedConnections, 10),
		targetQPSStr,
		fmt.Sprintf("%.2f", snapshot.ActualTotalQPS),
		fmt.Sprintf("%.2f", snapshot.ActualGetQPS),
		fmt.Sprintf("%.2f", snapshot.ActualSetQPS),
		strconv.FormatInt(snapshot.TotalOps, 10),
		strconv.FormatInt(snapshot.GetOps, 10),
		strconv.FormatInt(snapshot.SetOps, 10),
		strconv.FormatInt(snapshot.GetErrors, 10),
		strconv.FormatInt(snapshot.SetErrors, 10),
		strconv.FormatInt(snapshot.GetLatencyP50, 10),
		strconv.FormatInt(snapshot.GetLatencyP95, 10),
		strconv.FormatInt(snapshot.GetLatencyP99, 10),
		strconv.FormatInt(snapshot.GetLatencyMax, 10),
		strconv.FormatInt(snapshot.SetLatencyP50, 10),
		strconv.FormatInt(snapshot.SetLatencyP95, 10),
		strconv.FormatInt(snapshot.SetLatencyP99, 10),
		strconv.FormatInt(snapshot.SetLatencyMax, 10),
		fmt.Sprintf("%.3f", snapshot.NetworkRxMBps),
		fmt.Sprintf("%.3f", snapshot.NetworkTxMBps),
		fmt.Sprintf("%.0f", snapshot.NetworkRxPPS),
		fmt.Sprintf("%.0f", snapshot.NetworkTxPPS),
		fmt.Sprintf("%.2f", snapshot.MemoryUsedGB),
		fmt.Sprintf("%.2f", snapshot.MemoryTotalGB),
		fmt.Sprintf("%.1f", snapshot.CPUPercent),
		fmt.Sprintf("%.3f", snapshot.ProcessMemoryGB),
		fmt.Sprintf("%d", snapshot.TotalOutBoundConn),
		fmt.Sprintf("%d", snapshot.ActiveTCPConns),
		fmt.Sprintf("%d", snapshot.TCPConnDelta),
	}

	if err := cl.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write CSV record: %w", err)
	}
	cl.writer.Flush()

	return nil
}

// Close closes the CSV logger
func (cl *CSVLogger) Close() error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	if cl.writer != nil {
		cl.writer.Flush()
	}
	if cl.file != nil {
		return cl.file.Close()
	}
	return nil
}

// StartTimeBlock starts tracking a new time block
func (ws *WorkloadStats) StartTimeBlock(config TrafficConfig) {
	ws.BlockMutex.Lock()
	defer ws.BlockMutex.Unlock()

	// Finish current block if exists
	if ws.CurrentBlock != nil {
		ws.CurrentBlock.EndTime = time.Now()
		ws.TimeBlocks = append(ws.TimeBlocks, *ws.CurrentBlock)
	}

	// Start new block
	ws.CurrentBlock = &TimeBlockStats{
		Config:    config,
		StartTime: time.Now(),
		GetStats:  NewPerformanceStats(),
		SetStats:  NewPerformanceStats(),
	}
}

// FinishCurrentTimeBlock finishes the current time block
func (ws *WorkloadStats) FinishCurrentTimeBlock() {
	ws.BlockMutex.Lock()
	defer ws.BlockMutex.Unlock()

	if ws.CurrentBlock != nil {
		ws.CurrentBlock.EndTime = time.Now()
		ws.TimeBlocks = append(ws.TimeBlocks, *ws.CurrentBlock)
		ws.CurrentBlock = nil
	}
}

// RecordOperationInBlock records an operation in the current time block
func (ws *WorkloadStats) RecordOperationInBlock(isSet bool, latencyMicros int64, isError bool) {
	ws.BlockMutex.RLock()
	defer ws.BlockMutex.RUnlock()

	if ws.CurrentBlock == nil {
		return
	}

	if isSet {
		if isError {
			atomic.AddInt64(&ws.CurrentBlock.SetErrors, 1)
		} else {
			atomic.AddInt64(&ws.CurrentBlock.ActualSetOps, 1)
			ws.CurrentBlock.SetStats.RecordLatency(latencyMicros)
		}
	} else {
		if isError {
			atomic.AddInt64(&ws.CurrentBlock.GetErrors, 1)
		} else {
			atomic.AddInt64(&ws.CurrentBlock.ActualGetOps, 1)
			ws.CurrentBlock.GetStats.RecordLatency(latencyMicros)
		}
	}
}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run cache workload with configurable access patterns",
	Long: `Run cache workload tests with configurable access patterns including Zipf distribution,
Set:Get ratios, and time-based testing.

This command runs a mixed workload against Redis or Momento cache systems with realistic
access patterns using Zipf distribution for key selection and configurable Set:Get ratios.

Examples:
  # Run basic workload for 60 seconds with default Zipf distribution
  serverless-cache-benchmark run --cache-type redis --test-time 60

  # Run with high key concentration (Zipf exponent 2.0) and 1:5 Set:Get ratio
  serverless-cache-benchmark run --cache-type redis --key-zipf-exp 2.0 --ratio 1:5 --test-time 120

  # Run with custom key range and clients
  serverless-cache-benchmark run --cache-type redis --key-maximum 1000000 --clients 8 --test-time 300

  # Run with dynamic traffic pattern from CSV file
  serverless-cache-benchmark run --cache-type redis --traffic-pattern traffic.csv`,
	Run: runWorkload,
}

// parseRatio parses Set:Get ratio string like "1:10"
func parseRatio(ratioStr string) (int, int, error) {
	parts := strings.Split(ratioStr, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid ratio format, expected 'set:get' like '1:10'")
	}

	setRatio, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid set ratio: %w", err)
	}

	getRatio, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid get ratio: %w", err)
	}

	if setRatio < 0 || getRatio < 0 {
		return 0, 0, fmt.Errorf("ratios must be non-negative")
	}

	return setRatio, getRatio, nil
}

// TCPMonitor tracks TCP connection counts over time
type TCPMonitor struct {
	mu             sync.RWMutex
	currentCount   int
	previousCount  int
	history        []int
	maxHistorySize int
	lastUpdate     time.Time
	redisPorts     []int // Redis ports to filter connections
}

// updateCount updates the TCP connection count and maintains history
func (tm *TCPMonitor) updateCount(count int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.previousCount = tm.currentCount
	tm.currentCount = count
	tm.lastUpdate = time.Now()

	// Add to history and maintain max size
	tm.history = append(tm.history, count)
	if len(tm.history) > tm.maxHistorySize {
		tm.history = tm.history[1:]
	}
}

// getStats returns current TCP connection statistics
func (tm *TCPMonitor) getStats() (current, delta int, history []int) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	current = tm.currentCount
	delta = tm.currentCount - tm.previousCount
	history = make([]int, len(tm.history))
	copy(history, tm.history)
	return
}

// SystemStats holds lightweight system monitoring data
type SystemStats struct {
	MemoryUsedMB     float64
	MemoryTotalMB    float64
	CPUPercent       float64
	NetworkRxMBps    float64 // Network receive MB/s
	NetworkTxMBps    float64 // Network transmit MB/s
	NetworkRxPPS     float64 // Network receive packets/s
	NetworkTxPPS     float64 // Network transmit packets/s
	OutboundTCPConns int     // Established outbound conn count (from /proc)
	ActiveTCPConns   int     // All active TCP connections (from netlink)
	TCPConnDelta     int     // Change from previous second
	TCPConnHistory   []int   // Last N seconds for trending
}

// NetworkStats holds network interface statistics
type NetworkStats struct {
	RxBytes   uint64
	TxBytes   uint64
	RxPackets uint64
	TxPackets uint64
	Timestamp time.Time
}

var lastNetworkStats *NetworkStats

// startTCPMonitoring starts a goroutine that monitors TCP connections every second
func startTCPMonitoring(ctx context.Context, monitor *TCPMonitor) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count := getTCPConnectionCountFiltered(monitor.redisPorts)
			monitor.updateCount(count)
		}
	}
}

// getTCPConnectionCount gets the count of all established TCP connections using netlink
func getTCPConnectionCount() int {
	// Try netlink first (more accurate)
	socks, err := netlink.SocketDiagTCP(0)
	if err != nil {
		// Fallback to /proc parsing if netlink fails
		return getCurrentTCPCountFromProc()
	}

	count := 0
	for _, s := range socks {
		if s.State == netlink.TCP_ESTABLISHED {
			count++
		}
	}
	return count
}

// getTCPConnectionCountFiltered gets the count of established TCP connections filtered by Redis ports
func getTCPConnectionCountFiltered(redisPorts []int) int {
	return getCurrentTCPCountFromProcFiltered(redisPorts)
}

// extractRedisPorts extracts Redis server ports from configuration for TCP filtering
func extractRedisPorts(cmd *cobra.Command, cacheType string) []int {
	var ports []int

	if cacheType == "redis" {
		redisURI, _ := cmd.Flags().GetString("redis-uri")
		if redisURI != "" {
			// Parse Redis URI to extract port
			if strings.HasPrefix(redisURI, "redis://") || strings.HasPrefix(redisURI, "rediss://") {
				// Remove protocol
				uri := redisURI
				if strings.HasPrefix(uri, "redis://") {
					uri = uri[8:]
				} else if strings.HasPrefix(uri, "rediss://") {
					uri = uri[9:]
				}

				// Extract host:port part (before any path/query)
				if idx := strings.Index(uri, "/"); idx != -1 {
					uri = uri[:idx]
				}
				if idx := strings.Index(uri, "?"); idx != -1 {
					uri = uri[:idx]
				}

				// Remove username:password@ if present
				if idx := strings.LastIndex(uri, "@"); idx != -1 {
					uri = uri[idx+1:]
				}

				// Extract port
				if idx := strings.LastIndex(uri, ":"); idx != -1 {
					if portStr := uri[idx+1:]; portStr != "" {
						if port, err := strconv.Atoi(portStr); err == nil {
							ports = append(ports, port)
						}
					}
				} else {
					// Default Redis port
					ports = append(ports, 6379)
				}
			}
		}
	}
	// For Momento, we don't filter by port since it uses HTTPS

	return ports
}

// getCurrentTCPCountFromProc gets TCP connection count from /proc, filtering for Redis connections
func getCurrentTCPCountFromProc() int {
	return getCurrentTCPCountFromProcFiltered(nil)
}

// getCurrentTCPCountFromProcFiltered gets TCP connection count from /proc with optional port filtering
func getCurrentTCPCountFromProcFiltered(redisPorts []int) int {
	files := []string{"/proc/net/tcp", "/proc/net/tcp6"}
	totalOutboundConns := 0

	for _, path := range files {
		f, err := os.Open(path)
		if err == nil {
			sc := bufio.NewScanner(f)
			if !sc.Scan() { // skip header
				f.Close()
				continue
			}
			for sc.Scan() {
				fields := fastSplit(sc.Text())
				if len(fields) >= 4 && fields[3] == "01" { // ESTABLISHED
					// If Redis ports are specified, filter by destination port
					if redisPorts != nil && len(redisPorts) > 0 {
						if len(fields) >= 3 {
							// Parse remote address (format: IP:PORT in hex)
							remoteAddr := fields[2]
							if colonIdx := strings.LastIndex(remoteAddr, ":"); colonIdx != -1 {
								portHex := remoteAddr[colonIdx+1:]
								if port, err := strconv.ParseInt(portHex, 16, 32); err == nil {
									isRedisPort := false
									for _, redisPort := range redisPorts {
										if int(port) == redisPort {
											isRedisPort = true
											break
										}
									}
									if isRedisPort {
										totalOutboundConns++
									}
								}
							}
						}
					} else {
						// No filtering, count all established connections
						totalOutboundConns++
					}
				}
			}
			f.Close()
		}
	}
	return totalOutboundConns
}

// getSystemStats returns current system resource usage (lightweight)
func getSystemStats(tcpMonitor *TCPMonitor) SystemStats {
	stats := SystemStats{}

	// Get memory info from /proc/meminfo
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "MemTotal:") {
				if fields := strings.Fields(line); len(fields) >= 2 {
					if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
						stats.MemoryTotalMB = kb / 1024
					}
				}
			} else if strings.HasPrefix(line, "MemAvailable:") {
				if fields := strings.Fields(line); len(fields) >= 2 {
					if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
						availableMB := kb / 1024
						stats.MemoryUsedMB = stats.MemoryTotalMB - availableMB
					}
				}
			}
		}
	}

	// Get CPU usage from /proc/loadavg (1-minute load average)
	if data, err := os.ReadFile("/proc/loadavg"); err == nil {
		if fields := strings.Fields(string(data)); len(fields) >= 1 {
			if load, err := strconv.ParseFloat(fields[0], 64); err == nil {
				// Convert load average to rough CPU percentage
				// Load of 1.0 = 100% on single core, so divide by number of CPUs
				stats.CPUPercent = (load / float64(runtime.NumCPU())) * 100
				if stats.CPUPercent > 100 {
					stats.CPUPercent = 100
				}
			}
		}
	}

	// Get network statistics
	networkStats := getNetworkStats()
	if networkStats != nil {
		stats.NetworkRxMBps = networkStats.NetworkRxMBps
		stats.NetworkTxMBps = networkStats.NetworkTxMBps
		stats.NetworkRxPPS = networkStats.NetworkRxPPS
		stats.NetworkTxPPS = networkStats.NetworkTxPPS
	}

	// Get network statistics
	netStats := getNetworkStats()
	if netStats != nil {
		stats.NetworkRxMBps = netStats.NetworkRxMBps
		stats.NetworkTxMBps = netStats.NetworkTxMBps
		stats.NetworkRxPPS = netStats.NetworkRxPPS
		stats.NetworkTxPPS = netStats.NetworkTxPPS
	}

	// Get enhanced TCP connection data from monitor if available
	if tcpMonitor != nil {
		current, delta, history := tcpMonitor.getStats()
		stats.ActiveTCPConns = current
		stats.TCPConnDelta = delta
		stats.TCPConnHistory = history
	}

	// Get established TCP Conn Count (fallback/legacy method)
	stats.OutboundTCPConns = getCurrentTCPCountFromProc()

	return stats
}

func fastSplit(s string) []string {
	res := make([]string, 0, 16)
	start := -1
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != ' ' && c != '\t' {
			if start < 0 {
				start = i
			}
		} else if start >= 0 {
			res = append(res, s[start:i])
			start = -1
		}
	}
	if start >= 0 {
		res = append(res, s[start:])
	}
	return res
}

// getNetworkStats returns network bandwidth and PPS statistics
func getNetworkStats() *SystemStats {
	currentStats := readNetworkStats()
	if currentStats == nil || lastNetworkStats == nil {
		lastNetworkStats = currentStats
		return nil
	}

	// Calculate time difference
	timeDiff := currentStats.Timestamp.Sub(lastNetworkStats.Timestamp).Seconds()
	if timeDiff <= 0 {
		return nil
	}

	// Calculate bandwidth (bytes/sec -> MB/s)
	rxMBps := float64(currentStats.RxBytes-lastNetworkStats.RxBytes) / timeDiff / (1024 * 1024)
	txMBps := float64(currentStats.TxBytes-lastNetworkStats.TxBytes) / timeDiff / (1024 * 1024)

	// Calculate packets per second
	rxPPS := float64(currentStats.RxPackets-lastNetworkStats.RxPackets) / timeDiff
	txPPS := float64(currentStats.TxPackets-lastNetworkStats.TxPackets) / timeDiff

	// Update last stats
	lastNetworkStats = currentStats

	return &SystemStats{
		NetworkRxMBps: rxMBps,
		NetworkTxMBps: txMBps,
		NetworkRxPPS:  rxPPS,
		NetworkTxPPS:  txPPS,
	}
}

// readNetworkStats reads network statistics from /proc/net/dev
func readNetworkStats() *NetworkStats {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return nil
	}

	lines := strings.Split(string(data), "\n")
	var totalRxBytes, totalTxBytes, totalRxPackets, totalTxPackets uint64

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, ":") && !strings.HasPrefix(line, "Inter-") && !strings.HasPrefix(line, "face") {
			// Parse network interface line
			parts := strings.Fields(strings.Replace(line, ":", " ", 1))
			if len(parts) >= 17 {
				// Skip loopback interface
				if parts[0] == "lo" {
					continue
				}

				// Parse RX bytes (column 1), RX packets (column 2)
				if rxBytes, err := strconv.ParseUint(parts[1], 10, 64); err == nil {
					totalRxBytes += rxBytes
				}
				if rxPackets, err := strconv.ParseUint(parts[2], 10, 64); err == nil {
					totalRxPackets += rxPackets
				}

				// Parse TX bytes (column 9), TX packets (column 10)
				if txBytes, err := strconv.ParseUint(parts[9], 10, 64); err == nil {
					totalTxBytes += txBytes
				}
				if txPackets, err := strconv.ParseUint(parts[10], 10, 64); err == nil {
					totalTxPackets += txPackets
				}
			}
		}
	}

	return &NetworkStats{
		RxBytes:   totalRxBytes,
		TxBytes:   totalTxBytes,
		RxPackets: totalRxPackets,
		TxPackets: totalTxPackets,
		Timestamp: time.Now(),
	}
}

// getProcessMemoryMB returns current process memory usage in MB
func getProcessMemoryMB() float64 {
	if data, err := os.ReadFile("/proc/self/status"); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "VmRSS:") {
				if fields := strings.Fields(line); len(fields) >= 2 {
					if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
						return kb / 1024 // Convert KB to MB
					}
				}
			}
		}
	}
	return 0
}

// parseTrafficPattern parses a CSV file with traffic configuration
func parseTrafficPattern(filename string) ([]TrafficConfig, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open traffic pattern file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comment = '#'
	reader.TrimLeadingSpace = true

	var configs []TrafficConfig
	lineNum := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV line %d: %w", lineNum+1, err)
		}

		lineNum++

		// Skip header line if it looks like one
		if lineNum == 1 && (strings.ToLower(record[0]) == "time_seconds" || strings.ToLower(record[0]) == "time") {
			continue
		}

		if len(record) != 3 {
			return nil, fmt.Errorf("line %d: expected 3 columns (time_seconds,clients,qps), got %d", lineNum, len(record))
		}

		timeSeconds, err := strconv.Atoi(strings.TrimSpace(record[0]))
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid time_seconds '%s': %w", lineNum, record[0], err)
		}

		clients, err := strconv.Atoi(strings.TrimSpace(record[1]))
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid clients '%s': %w", lineNum, record[1], err)
		}

		qpsStr := strings.TrimSpace(record[2])
		var qps int
		if qpsStr == "-1" || strings.ToLower(qpsStr) == "unlimited" {
			qps = -1
		} else {
			qps, err = strconv.Atoi(qpsStr)
			if err != nil {
				return nil, fmt.Errorf("line %d: invalid qps '%s': %w", lineNum, record[2], err)
			}
		}

		if timeSeconds < 0 {
			return nil, fmt.Errorf("line %d: time_seconds cannot be negative", lineNum)
		}
		if clients < 0 {
			return nil, fmt.Errorf("line %d: clients cannot be negative", lineNum)
		}
		if qps < -1 {
			return nil, fmt.Errorf("line %d: qps cannot be less than -1", lineNum)
		}

		configs = append(configs, TrafficConfig{
			TimeSeconds: timeSeconds,
			Clients:     clients,
			QPS:         qps,
		})
	}

	if len(configs) == 0 {
		return nil, fmt.Errorf("no valid traffic configurations found in file")
	}

	// Sort by time to ensure proper ordering
	for i := 0; i < len(configs)-1; i++ {
		for j := i + 1; j < len(configs); j++ {
			if configs[i].TimeSeconds > configs[j].TimeSeconds {
				configs[i], configs[j] = configs[j], configs[i]
			}
		}
	}

	return configs, nil
}

// createAndTestCacheClient creates a cache client and measures setup time including ping
func createAndTestCacheClient(ctx context.Context, cacheType string, cmd *cobra.Command, stats *WorkloadStats) (CacheClient, error) {
	setupStart := time.Now()

	// Create the client
	client, err := createCacheClientForRun(ctx, cacheType, cmd)
	if err != nil {
		return nil, err
	}

	// Get configurable connection timeout
	connectionTimeout, _ := cmd.Flags().GetInt("connection-timeout")

	// Test connectivity with ping
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(connectionTimeout)*time.Second)
	defer cancel()

	err = client.Ping(ctx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("ping failed: %w", err)
	}

	// Record setup time
	setupTime := time.Since(setupStart)
	stats.SetupStats.RecordLatency(setupTime.Microseconds())

	return client, nil
}

// createCacheClientForRun creates a cache client for the run command (reuses populate logic)
func createCacheClientForRun(ctx context.Context, cacheType string, cmd *cobra.Command) (CacheClient, error) {
	switch cacheType {
	case "redis":
		uri, _ := cmd.Flags().GetString("redis-uri")
		clusterMode, _ := cmd.Flags().GetBool("cluster-mode")

		// Build Redis configuration from flags
		dialTimeout, _ := cmd.Flags().GetInt("redis-dial-timeout")
		readTimeout, _ := cmd.Flags().GetInt("redis-read-timeout")
		writeTimeout, _ := cmd.Flags().GetInt("redis-write-timeout")
		poolTimeout, _ := cmd.Flags().GetInt("redis-pool-timeout")
		connMaxIdleTime, _ := cmd.Flags().GetInt("redis-conn-max-idle-time")
		maxRetries, _ := cmd.Flags().GetInt("redis-max-retries")
		minRetryBackoff, _ := cmd.Flags().GetInt("redis-min-retry-backoff")
		maxRetryBackoff, _ := cmd.Flags().GetInt("redis-max-retry-backoff")
		poolSize, _ := cmd.Flags().GetInt("redis-pool-size")

		config := RedisConfig{
			DialTimeout:     time.Duration(dialTimeout) * time.Second,
			ReadTimeout:     time.Duration(readTimeout) * time.Second,
			WriteTimeout:    time.Duration(writeTimeout) * time.Second,
			PoolTimeout:     time.Duration(poolTimeout) * time.Second,
			ConnMaxIdleTime: time.Duration(connMaxIdleTime) * time.Second,
			MaxRetries:      maxRetries,
			MinRetryBackoff: time.Duration(minRetryBackoff) * time.Millisecond,
			MaxRetryBackoff: time.Duration(maxRetryBackoff) * time.Millisecond,
			ClusterMode:     clusterMode,
			PoolSize:        poolSize,
		}

		client, err := NewRedisClientFromURI(uri, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create Redis client from URI '%s': %w", uri, err)
		}
		return client, nil

	case "momento":
		apiKey, _ := cmd.Flags().GetString("momento-api-key")
		cacheName, _ := cmd.Flags().GetString("momento-cache-name")
		defaultTTL, _ := cmd.Flags().GetInt("default-ttl")
		clientConnCount, _ := cmd.Flags().GetUint32("momento-client-conn-count")

		// Don't create cache per worker - it should be created once upfront
		client, err := NewMomentoClient(ctx, apiKey, cacheName, false, defaultTTL, clientConnCount)
		if err != nil {
			return nil, fmt.Errorf("failed to create Momento client: %w", err)
		}
		return client, nil

	default:
		return nil, fmt.Errorf("invalid cache type: %s. Must be 'redis' or 'momento'", cacheType)
	}
}

func runWorkload(cmd *cobra.Command, args []string) {
	// Print version info
	fmt.Printf("serverless-cache-benchmark run\n")
	fmt.Printf("Git Commit: %s", gitSHA1)
	if gitDirty != "0" && gitDirty != "unknown" {
		fmt.Printf(" (dirty)")
	}
	fmt.Printf("\n\n")

	// Start profiling if requested
	cpuProfile, _ := cmd.Flags().GetString("cpu-profile")
	memProfile, _ := cmd.Flags().GetString("mem-profile")
	blockProfile, _ := cmd.Flags().GetString("block-profile")
	mutexProfile, _ := cmd.Flags().GetString("mutex-profile")
	pprofAddr, _ := cmd.Flags().GetString("pprof-addr")
	blockProfileRate, _ := cmd.Flags().GetInt("block-profile-rate")
	mutexProfileFraction, _ := cmd.Flags().GetInt("mutex-profile-fraction")

	// Enable block profiling if requested
	if blockProfile != "" && blockProfileRate > 0 {
		runtime.SetBlockProfileRate(blockProfileRate)
		fmt.Printf("Block profiling enabled with rate: %d\n", blockProfileRate)

		if blockProfile != "" {
			defer func() {
				f, err := os.Create(blockProfile)
				if err != nil {
					log.Printf("Could not create block profile: %v", err)
					return
				}
				defer f.Close()

				if err := pprof.Lookup("block").WriteTo(f, 0); err != nil {
					log.Printf("Could not write block profile: %v", err)
				} else {
					fmt.Printf("Block profile written to: %s\n", blockProfile)
				}
			}()
		}
	}

	// Enable mutex profiling if requested
	if mutexProfile != "" && mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(mutexProfileFraction)
		fmt.Printf("Mutex profiling enabled with fraction: %d\n", mutexProfileFraction)

		if mutexProfile != "" {
			defer func() {
				f, err := os.Create(mutexProfile)
				if err != nil {
					log.Printf("Could not create mutex profile: %v", err)
					return
				}
				defer f.Close()

				if err := pprof.Lookup("mutex").WriteTo(f, 0); err != nil {
					log.Printf("Could not write mutex profile: %v", err)
				} else {
					fmt.Printf("Mutex profile written to: %s\n", mutexProfile)
				}
			}()
		}
	}

	// Start pprof HTTP server if requested
	if pprofAddr != "" {
		go func() {
			fmt.Printf("Starting pprof HTTP server on http://%s/debug/pprof/\n", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof HTTP server failed: %v", err)
			}
		}()
	}

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatalf("Could not create CPU profile: %v", err)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("Could not start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
		fmt.Printf("CPU profiling enabled, writing to: %s\n", cpuProfile)
	}

	if memProfile != "" {
		defer func() {
			f, err := os.Create(memProfile)
			if err != nil {
				log.Printf("Could not create memory profile: %v", err)
				return
			}
			defer f.Close()

			runtime.GC() // Get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Printf("Could not write memory profile: %v", err)
			} else {
				fmt.Printf("Memory profile written to: %s\n", memProfile)
			}
		}()
	}

	// Check if this is a connection setup benchmark
	connSetupOnly, _ := cmd.Flags().GetBool("conn-setup-only")
	if connSetupOnly {
		runConnectionSetupBenchmark(cmd, args)
		return
	}

	// Get command parameters
	cacheType, _ := cmd.Flags().GetString("cache-type")
	clientCount, _ := cmd.Flags().GetInt("clients")
	rps, _ := cmd.Flags().GetInt("rps")
	timeoutSeconds, _ := cmd.Flags().GetInt("timeout")
	verbose, _ := cmd.Flags().GetBool("verbose")
	quiet, _ := cmd.Flags().GetBool("quiet")

	// Workload parameters
	zipfExp, _ := cmd.Flags().GetFloat64("key-zipf-exp")
	testTime, _ := cmd.Flags().GetInt("test-time")
	ratioStr, _ := cmd.Flags().GetString("ratio")
	measureSetup, _ := cmd.Flags().GetBool("measure-setup")
	trafficPatternFile, _ := cmd.Flags().GetString("traffic-pattern")
	csvOutput, _ := cmd.Flags().GetString("csv-output")

	// Key parameters
	keyPrefix, _ := cmd.Flags().GetString("key-prefix")
	keyMin, _ := cmd.Flags().GetInt("key-minimum")
	keyMax, _ := cmd.Flags().GetInt("key-maximum")

	// Data parameters
	dataSize, _ := cmd.Flags().GetInt("data-size")
	randomData, _ := cmd.Flags().GetBool("random-data")
	defaultTTL, _ := cmd.Flags().GetInt("default-ttl")

	// CloudWatch parameters
	cloudwatchEnabled, _ := cmd.Flags().GetBool("cloudwatch-enabled")
	cloudwatchRegion, _ := cmd.Flags().GetString("cloudwatch-region")
	cloudwatchNamespace, _ := cmd.Flags().GetString("cloudwatch-namespace")

	// Parse and validate parameters
	setRatio, getRatio, err := parseRatio(ratioStr)
	if err != nil {
		log.Fatalf("Invalid ratio: %v", err)
	}

	if zipfExp <= 0 || zipfExp > 5 {
		log.Fatalf("Zipf exponent must be between 0 and 5, got: %f", zipfExp)
	}

	if testTime <= 0 {
		log.Fatalf("Test time must be positive, got: %d", testTime)
	}

	totalKeys := keyMax - keyMin + 1
	if totalKeys <= 0 {
		log.Fatalf("Invalid key range: min=%d, max=%d", keyMin, keyMax)
	}

	// Create workload stats
	stats := NewWorkloadStats()
	defer stats.GetStats.Close()
	defer stats.SetStats.Close()
	defer stats.SetupStats.Close()

	// Initialize CSV logging
	if csvOutput == "" {
		// Generate default filename with timestamp
		timestamp := time.Now().Format("20060102-150405")
		if trafficPatternFile != "" {
			csvOutput = fmt.Sprintf("workload-dynamic-%s.csv", timestamp)
		} else {
			csvOutput = fmt.Sprintf("workload-static-%s.csv", timestamp)
		}
	}

	csvLogger, err := NewCSVLogger(csvOutput)
	if err != nil {
		log.Fatalf("Failed to create CSV logger: %v", err)
	}
	stats.CSVLogger = csvLogger
	defer csvLogger.Close()

	fmt.Printf("Logging metrics to: %s\n", csvOutput)

	// Initialize CloudWatch configuration
	cloudwatchConfig, err := NewCloudWatchConfig(cloudwatchEnabled, cloudwatchRegion, cloudwatchNamespace)
	if err != nil {
		log.Printf("Warning: Failed to initialize CloudWatch: %v", err)
		cloudwatchConfig = &CloudWatchConfig{Enabled: false}
	} else if cloudwatchEnabled {
		fmt.Printf("CloudWatch metrics enabled: region=%s, namespace=%s, host=%s\n",
			cloudwatchRegion, cloudwatchNamespace, cloudwatchConfig.Hostname)
	}

	workerCount, _ := cmd.Flags().GetInt("momento-client-worker-count")

	// For Momento, create cache once upfront to avoid spam
	if cacheType == "momento" {
		apiKey, _ := cmd.Flags().GetString("momento-api-key")
		cacheName, _ := cmd.Flags().GetString("momento-cache-name")
		createCache, _ := cmd.Flags().GetBool("momento-create-cache")
		clientConnCount, _ := cmd.Flags().GetUint32("momento-client-conn-count")

		if createCache {
			// Create a temporary client just to create the cache
			tempClient, err := NewMomentoClient(context.Background(), apiKey, cacheName, true, defaultTTL, clientConnCount)
			if err != nil {
				log.Fatalf("Failed to create Momento cache: %v", err)
			}
			tempClient.Close()
		}
	}

	fmt.Printf("Starting %s workload run...\n", cacheType)
	fmt.Printf("Clients: %d\n", clientCount)
	fmt.Printf("Test duration: %d seconds\n", testTime)
	fmt.Printf("Key range: %d to %d (%d total keys)\n", keyMin, keyMax, totalKeys)
	fmt.Printf("Zipf exponent: %.2f\n", zipfExp)
	fmt.Printf("Set:Get ratio: %d:%d\n", setRatio, getRatio)
	if rps > 0 {
		fmt.Printf("Rate limit: %d RPS total (%.2f RPS per client)\n", rps, float64(rps)/float64(clientCount))
	} else {
		fmt.Printf("Rate limit: unlimited\n")
	}
	fmt.Printf("Data size: %d bytes\n", dataSize)
	fmt.Println()

	// Check if using traffic pattern or static configuration
	if trafficPatternFile != "" {
		// Use dynamic traffic pattern
		runDynamicWorkload(cmd, trafficPatternFile, cacheType, zipfExp, ratioStr, keyPrefix, keyMin,
			totalKeys, dataSize, randomData, defaultTTL, workerCount, measureSetup, verbose, quiet, timeoutSeconds, stats)
	} else {
		// Use static configuration - run the original logic
		runStaticWorkload(cmd, cacheType, clientCount, rps, zipfExp, ratioStr, keyPrefix, keyMin,
			totalKeys, dataSize, randomData, defaultTTL, workerCount, measureSetup, verbose, quiet, timeoutSeconds, testTime, stats, cloudwatchConfig)
	}
}

// runStaticWorkload runs the original static workload logic
func runStaticWorkload(cmd *cobra.Command, cacheType string, clientCount, rps int, zipfExp float64,
	ratioStr, keyPrefix string, keyMin, totalKeys, dataSize int, randomData bool, defaultTTL int, workerCount int, measureSetup, verbose, quiet bool,
	timeoutSeconds, testTime int, stats *WorkloadStats, cloudwatchConfig *CloudWatchConfig) {

	connectionDelayMs, _ := cmd.Flags().GetInt("connection-delay-ms")

	// Parse ratio
	setRatio, getRatio, err := parseRatio(ratioStr)
	if err != nil {
		log.Fatalf("Invalid ratio: %v", err)
	}

	// Create data generator
	generator := &DataGenerator{
		DataSize:   dataSize,
		RandomData: randomData,
		DefaultTTL: defaultTTL,
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTime)*time.Second)
	defer cancel()

	// Create TCP monitor and start monitoring
	tcpMonitor := &TCPMonitor{maxHistorySize: 10}
	go startTCPMonitoring(ctx, tcpMonitor)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Handle signals in a separate goroutine
	go func() {
		<-sigChan
		fmt.Print("\r" + strings.Repeat(" ", 150) + "\r") // Clear progress line
		fmt.Println("\nReceived interrupt signal. Stopping workload and printing summary...")
		cancel() // Cancel context to stop all workers
	}()

	// Create workers
	var wg sync.WaitGroup

	fmt.Printf("Setting up %d clients...\n", clientCount)
	setupStart := time.Now()

	for i := 0; i < clientCount; i++ {
		// Create rate limiter for this client if specified
		var limiter *rate.Limiter
		if rps > 0 {
			clientRPS := float64(rps) / float64(clientCount)
			limiter = rate.NewLimiter(rate.Limit(clientRPS), 1)
		}

		wg.Add(1)

		// Add connection delay to prevent connection storms
		if connectionDelayMs > 0 {
			time.Sleep(time.Duration(connectionDelayMs) * time.Millisecond)
		}

		// Let each worker create its own connection in parallel
		switch cacheType {
		case "momento":
			go runMomentoWorkerWithConnectionCreation(ctx, &wg, i, cacheType, cmd, totalKeys, zipfExp,
				generator, stats, workerCount, setRatio, getRatio, keyPrefix, keyMin, limiter,
				timeoutSeconds, measureSetup, verbose, quiet)
		case "redis":
			// Get Redis worker count
			redisWorkerCount, _ := cmd.Flags().GetInt("redis-client-worker-count")
			go runRedisWorkerWithConnectionCreation(ctx, &wg, i, cacheType, cmd, totalKeys, zipfExp,
				generator, stats, redisWorkerCount, setRatio, getRatio, keyPrefix, keyMin, limiter,
				timeoutSeconds, measureSetup, verbose, quiet)
		default:
			go runWorkerWithConnectionCreation(ctx, &wg, i, cacheType, cmd, totalKeys, zipfExp,
				generator, stats, setRatio, getRatio, keyPrefix, keyMin, limiter,
				timeoutSeconds, measureSetup, verbose, quiet)
		}
	}

	totalSetupTime := time.Since(setupStart)
	if measureSetup {
		fmt.Printf("All clients setup completed in %.2f seconds (including connectivity tests)\n", totalSetupTime.Seconds())
	} else {
		fmt.Printf("All clients setup completed in %.2f seconds\n", totalSetupTime.Seconds())
	}

	// Start progress reporting
	go reportStaticProgress(ctx, stats, testTime, clientCount, verbose, cloudwatchConfig, tcpMonitor)

	// Wait for all workers to complete
	wg.Wait()

	// Clear progress line and print final results
	fmt.Print("\r" + strings.Repeat(" ", 150) + "\r")
	printFinalResults(stats, testTime, measureSetup)
}

// runDynamicWorkload runs workload with dynamic traffic patterns
func runDynamicWorkload(cmd *cobra.Command, trafficPatternFile, cacheType string, zipfExp float64,
	ratioStr, keyPrefix string, keyMin, totalKeys, dataSize int, randomData bool, defaultTTL int, workerCount int, measureSetup, verbose, quiet bool,
	timeoutSeconds int, stats *WorkloadStats) {

	// Parse traffic pattern
	trafficConfigs, err := parseTrafficPattern(trafficPatternFile)
	if err != nil {
		log.Fatalf("Failed to parse traffic pattern: %v", err)
	}

	// Parse ratio
	setRatio, getRatio, err := parseRatio(ratioStr)
	if err != nil {
		log.Fatalf("Invalid ratio: %v", err)
	}

	// Create data generator
	generator := &DataGenerator{
		DataSize:   dataSize,
		RandomData: randomData,
		DefaultTTL: defaultTTL,
	}

	fmt.Printf("Starting dynamic workload with %d traffic configurations...\n", len(trafficConfigs))
	maxClients := 0
	for i, config := range trafficConfigs {
		if config.Clients > maxClients {
			maxClients = config.Clients
		}
		qpsStr := "unlimited"
		if config.QPS != -1 {
			qpsStr = fmt.Sprintf("%d", config.QPS)
		}
		fmt.Printf("  %d. Time %ds: %d clients, %s QPS\n", i+1, config.TimeSeconds, config.Clients, qpsStr)
	}
	fmt.Printf("\nMax clients planned: %d\n", maxClients)
	fmt.Printf("Note: Each client creates a TCP connection. Ensure system limits allow this.\n")
	fmt.Println()

	// Calculate total test time
	totalTestTime := trafficConfigs[len(trafficConfigs)-1].TimeSeconds + 10 // Add 10 seconds buffer
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(totalTestTime)*time.Second)
	defer cancel()

	// Create TCP monitor and start monitoring
	tcpMonitor := &TCPMonitor{maxHistorySize: 10}
	go startTCPMonitoring(ctx, tcpMonitor)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Handle signals in a separate goroutine
	go func() {
		<-sigChan
		fmt.Print("\r" + strings.Repeat(" ", 150) + "\r") // Clear progress line
		fmt.Println("\nReceived interrupt signal. Stopping workload and printing summary...")
		cancel() // Cancel context to stop all workers
	}()

	// Start traffic pattern manager
	go manageTrafficPattern(ctx, trafficConfigs, cacheType, cmd, generator, stats,
		setRatio, getRatio, keyPrefix, workerCount, keyMin, totalKeys, zipfExp, measureSetup, verbose, quiet, timeoutSeconds)

	// Start progress reporting
	go reportProgress(ctx, stats, verbose, tcpMonitor)

	// Wait for context to complete
	<-ctx.Done()

	// Finish current time block
	stats.FinishCurrentTimeBlock()

	// Print final results with time block breakdown
	printDynamicFinalResults(stats, trafficConfigs, measureSetup)
}

// runWorkerInternal contains the actual worker logic without WaitGroup management
func runWorkerInternal(ctx context.Context, workerID int, client CacheClient,
	totalKeys int, zipfExp float64, generator *DataGenerator, stats *WorkloadStats,
	setRatio, getRatio int, keyPrefix string, keyMin int,
	limiter *rate.Limiter, timeoutSeconds int, verbose bool) {

	// Create a worker-specific Zipf generator with unique seed to ensure different key patterns
	seed := time.Now().UnixNano() + int64(workerID*1000)
	if verbose {
		fmt.Printf("Worker %d: Creating Zipf generator with totalKeys=%d, zipfExp=%f, seed=%d\n",
			workerID, totalKeys, zipfExp, seed)
	}
	zipfGen := NewZipfGenerator(uint64(totalKeys), zipfExp, seed)

	totalRatio := setRatio + getRatio
	if totalRatio == 0 {
		return // Nothing to do
	}

	var opCount int64

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Apply rate limiting if configured
		if limiter != nil {
			err := limiter.Wait(ctx)
			if err != nil {
				return
			}
		}

		// Determine operation type based on ratio
		opCount++
		isSet := (opCount % int64(totalRatio)) < int64(setRatio)

		// Generate key using Zipf distribution
		keyOffset := zipfGen.Next()
		key := fmt.Sprintf("%s%d", keyPrefix, keyMin+int(keyOffset))

		// Create operation timeout context
		opCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)

		if isSet {
			// Perform SET operation
			data, err := generator.GenerateData()
			if err != nil {
				atomic.AddInt64(&stats.SetErrors, 1)
				cancel()
				continue
			}

			// Get expiration from generator (uses DefaultTTL if set)
			expiration := generator.GetExpiration()

			start := time.Now()
			err = client.Set(opCtx, key, data, expiration)
			latency := time.Since(start)
			cancel()

			if err != nil {
				atomic.AddInt64(&stats.SetErrors, 1)
				stats.RecordOperationInBlock(true, 0, true)
			} else {
				atomic.AddInt64(&stats.SetOps, 1)
				stats.SetStats.RecordLatency(latency.Microseconds())
				stats.RecordOperationInBlock(true, latency.Microseconds(), false)
			}
		} else {
			// Perform GET operation
			start := time.Now()
			_, err := client.Get(opCtx, key)
			latency := time.Since(start)
			cancel()

			if err != nil {
				atomic.AddInt64(&stats.GetErrors, 1)
				stats.RecordOperationInBlock(false, 0, true)
			} else {
				atomic.AddInt64(&stats.GetOps, 1)
				stats.GetStats.RecordLatency(latency.Microseconds())
				stats.RecordOperationInBlock(false, latency.Microseconds(), false)
			}
		}
	}
}

// runMomentoWorkerInternal contains the actual worker logic without WaitGroup management
func runMomentoWorkerInternal(ctx context.Context, workerID int, client CacheClient,
	totalKeys int, zipfExp float64, generator *DataGenerator, stats *WorkloadStats,
	setRatio, getRatio int, keyPrefix string, workerCount int, keyMin int,
	limiter *rate.Limiter, timeoutSeconds int, verbose bool) {

	// Create a worker-specific Zipf generator with unique seed to ensure different key patterns
	seed := time.Now().UnixNano() + int64(workerID*1000)
	if verbose {
		fmt.Printf("Worker %d: Creating Zipf generator with totalKeys=%d, zipfExp=%f, seed=%d\n",
			workerID, totalKeys, zipfExp, seed)
	}
	zipfGen := NewZipfGenerator(uint64(totalKeys), zipfExp, seed)

	totalRatio := setRatio + getRatio
	if totalRatio == 0 {
		return // Nothing to do
	}

	var opCount int64
	if verbose {
		fmt.Printf("Worker %d: Using producer-consumer batching with %d consumers\n", workerID, workerCount)
	}

	// Use producer-consumer model for continuous request processing
	runProducerConsumer(ctx, workerID, client, totalKeys, zipfExp, generator, stats,
		setRatio, getRatio, keyPrefix, keyMin, timeoutSeconds, workerCount, &opCount, zipfGen, verbose, limiter)
}

// runProducerConsumer implements producer-consumer model for continuous request processing
func runProducerConsumer(ctx context.Context, workerID int, client CacheClient,
	totalKeys int, zipfExp float64, generator *DataGenerator, stats *WorkloadStats,
	setRatio, getRatio int, keyPrefix string, keyMin int, timeoutSeconds int, numConsumers int,
	opCount *int64, zipfGen *ZipfGenerator, verbose bool, limiter *rate.Limiter) {

	// Create channels for producer-consumer communication
	requestChan := make(chan requestInfo, numConsumers*2) // Buffer to prevent blocking

	// Start consumer goroutines
	var consumerWG sync.WaitGroup
	for i := 0; i < numConsumers; i++ {
		consumerWG.Add(1)
		go func(consumerID int) {
			defer consumerWG.Done()
			for request := range requestChan {
				select {
				case <-ctx.Done():
					return
				default:
					result := processRequest(ctx, request, client, generator, timeoutSeconds, verbose)
					// Process and record result
					if result.isSet {
						if result.isError {
							atomic.AddInt64(&stats.SetErrors, 1)
							stats.RecordOperationInBlock(true, 0, true)
						} else {
							atomic.AddInt64(&stats.SetOps, 1)
							stats.SetStats.RecordLatency(result.latencyMicros)
							stats.RecordOperationInBlock(true, result.latencyMicros, false)
						}
					} else {
						if result.isError {
							atomic.AddInt64(&stats.GetErrors, 1)
							stats.RecordOperationInBlock(false, 0, true)
						} else {
							atomic.AddInt64(&stats.GetOps, 1)
							stats.GetStats.RecordLatency(result.latencyMicros)
							stats.RecordOperationInBlock(false, result.latencyMicros, false)
						}
					}
				}
			}
		}(i)
	}

	// Producer loop - generate requests continuously
	for {
		// Apply rate limiting if configured
		if limiter != nil {
			err := limiter.Wait(ctx)
			if err != nil {
				close(requestChan)
				consumerWG.Wait()
				return
			}
		}

		// Generate request info
		*opCount++
		isSet := (*opCount % int64(setRatio+getRatio)) < int64(setRatio)
		keyOffset := zipfGen.Next()
		key := fmt.Sprintf("%s%d", keyPrefix, keyMin+int(keyOffset))

		request := requestInfo{
			workerID: workerID,
			isSet:    isSet,
			key:      key,
		}

		// Send request to consumers (blocking if full)
		select {
		case requestChan <- request:
		case <-ctx.Done():
			close(requestChan)
			consumerWG.Wait()
			return
		}
	}
}

// requestInfo holds information for a single cache operation request
type requestInfo struct {
	workerID int
	isSet    bool
	key      string
}

// processRequest processes a single cache request
func processRequest(ctx context.Context, request requestInfo, client CacheClient,
	generator *DataGenerator, timeoutSeconds int, verbose bool) workloadResult {
	// Create operation timeout context before timing
	opCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()

	if request.isSet {
		// Generate data BEFORE timing the operation
		data, err := generator.GenerateData()
		if err != nil {
			return workloadResult{isSet: true, isError: true, latencyMicros: 0}
		}

		// Get expiration from generator (uses DefaultTTL if set)
		expiration := generator.GetExpiration()

		// Time ONLY the cache operation
		start := time.Now()
		err = client.Set(opCtx, request.key, data, expiration)
		latency := time.Since(start)

		if err != nil {
			if verbose {
				log.Printf("Worker %d: Set operation failed for key %s: %v", request.workerID, request.key, err)
			}
			return workloadResult{isSet: true, isError: true, latencyMicros: 0}
		} else {
			return workloadResult{isSet: true, isError: false, latencyMicros: latency.Microseconds()}
		}
	} else {
		// Perform GET operation
		// Time ONLY the cache operation
		start := time.Now()
		_, err := client.Get(opCtx, request.key)
		latency := time.Since(start)

		if err != nil {
			if verbose {
				log.Printf("Worker %d: Get operation failed for key %s: %v", request.workerID, request.key, err)
			}
			return workloadResult{isSet: false, isError: true, latencyMicros: 0}
		} else {
			return workloadResult{isSet: false, isError: false, latencyMicros: latency.Microseconds()}
		}
	}
}

// workloadResult represents the result of a single request operation
type workloadResult struct {
	isSet         bool
	isError       bool
	latencyMicros int64
}

// runWorkerWithConnectionCreation creates its own connection and then runs the worker
func runWorkerWithConnectionCreation(ctx context.Context, wg *sync.WaitGroup, workerID int,
	cacheType string, cmd *cobra.Command, totalKeys int, zipfExp float64,
	generator *DataGenerator, stats *WorkloadStats, setRatio, getRatio int,
	keyPrefix string, keyMin int, limiter *rate.Limiter, timeoutSeconds int,
	measureSetup, verbose, quiet bool) {

	defer wg.Done()

	// Create cache client in this goroutine (parallel connection creation)
	var client CacheClient
	var err error

	if measureSetup {
		client, err = createAndTestCacheClient(ctx, cacheType, cmd, stats)
	} else {
		client, err = createCacheClientForRun(ctx, cacheType, cmd)
	}

	if err != nil {
		// Always log connection failures as they're critical
		log.Printf("Worker %d: Failed to create client: %v", workerID, err)
		atomic.AddInt64(&stats.FailedConnections, 1)
		return
	}
	defer client.Close()

	// Track successful connection
	atomic.AddInt64(&stats.ActiveConnections, 1)
	defer atomic.AddInt64(&stats.ActiveConnections, -1)

	if verbose && !quiet {
		log.Printf("Worker %d: Successfully created client connection", workerID)
	}

	// Now run the normal worker routine (but don't call wg.Done() again)
	runWorkerInternal(ctx, workerID, client, totalKeys, zipfExp, generator, stats,
		setRatio, getRatio, keyPrefix, keyMin, limiter, timeoutSeconds, verbose)
}

// runMomentoWorkerWithConnectionCreation creates its own connection and then runs the worker
func runMomentoWorkerWithConnectionCreation(ctx context.Context, wg *sync.WaitGroup, workerID int,
	cacheType string, cmd *cobra.Command, totalKeys int, zipfExp float64,
	generator *DataGenerator, stats *WorkloadStats, workerCount int, setRatio, getRatio int,
	keyPrefix string, keyMin int, limiter *rate.Limiter, timeoutSeconds int,
	measureSetup, verbose, quiet bool) {

	defer wg.Done()

	// Create cache client in this goroutine (parallel connection creation)

	// No need for measureSetup ping, just create the cache client as
	// eager connection already occurs under the hood.
	client, err := createCacheClientForRun(ctx, cacheType, cmd)
	if err != nil {
		// Always log connection failures as they're critical
		log.Printf("Worker %d: Failed to create client: %v", workerID, err)
		return
	}

	if verbose && !quiet {
		clientConnCount, _ := cmd.Flags().GetUint32("momento-client-conn-count")
		log.Printf("Worker %d: Successfully created Momento client with %d TCP connections", workerID, clientConnCount)
	}

	// Now run the normal worker routine (but don't call wg.Done() again)
	runMomentoWorkerInternal(ctx, workerID, client, totalKeys, zipfExp, generator, stats,
		setRatio, getRatio, keyPrefix, workerCount, keyMin, limiter, timeoutSeconds, verbose)
}

// runRedisWorkerWithConnectionCreation creates its own connection and then runs the worker with multiple workers per client
func runRedisWorkerWithConnectionCreation(ctx context.Context, wg *sync.WaitGroup, workerID int,
	cacheType string, cmd *cobra.Command, totalKeys int, zipfExp float64,
	generator *DataGenerator, stats *WorkloadStats, workerCount int, setRatio, getRatio int,
	keyPrefix string, keyMin int, limiter *rate.Limiter, timeoutSeconds int,
	measureSetup, verbose, quiet bool) {

	defer wg.Done()

	// Create cache client in this goroutine (parallel connection creation)
	var client CacheClient
	var err error

	if measureSetup {
		client, err = createAndTestCacheClient(ctx, cacheType, cmd, stats)
	} else {
		client, err = createCacheClientForRun(ctx, cacheType, cmd)
	}

	if err != nil {
		// Always log connection failures as they're critical
		log.Printf("Worker %d: Failed to create client: %v", workerID, err)
		atomic.AddInt64(&stats.FailedConnections, 1)
		return
	}

	// Track successful connection
	atomic.AddInt64(&stats.ActiveConnections, 1)
	defer func() {
		atomic.AddInt64(&stats.ActiveConnections, -1)
		client.Close()
	}()

	if verbose && !quiet {
		poolSize, _ := cmd.Flags().GetInt("redis-pool-size")
		log.Printf("Worker %d: Successfully created Redis client with pool size %d and %d workers", workerID, poolSize, workerCount)
	}

	// Now run the Redis worker routine with multiple workers per client
	runRedisWorkerInternal(ctx, workerID, client, totalKeys, zipfExp, generator, stats,
		setRatio, getRatio, keyPrefix, workerCount, keyMin, limiter, timeoutSeconds, verbose)
}

// runRedisWorkerInternal contains the actual Redis worker logic with multiple workers per client
func runRedisWorkerInternal(ctx context.Context, workerID int, client CacheClient,
	totalKeys int, zipfExp float64, generator *DataGenerator, stats *WorkloadStats,
	setRatio, getRatio int, keyPrefix string, workerCount int, keyMin int,
	limiter *rate.Limiter, timeoutSeconds int, verbose bool) {

	// Create a worker-specific Zipf generator with unique seed to ensure different key patterns
	seed := time.Now().UnixNano() + int64(workerID*1000)
	if verbose {
		fmt.Printf("Worker %d: Creating Zipf generator with totalKeys=%d, zipfExp=%f, seed=%d\n",
			workerID, totalKeys, zipfExp, seed)
	}
	zipfGen := NewZipfGenerator(uint64(totalKeys), zipfExp, seed)

	totalRatio := setRatio + getRatio
	if totalRatio == 0 {
		return // Nothing to do
	}

	var opCount int64
	if verbose {
		fmt.Printf("Worker %d: Using producer-consumer batching with %d consumers\n", workerID, workerCount)
	}

	// Use producer-consumer model for continuous request processing (same as Momento)
	runProducerConsumer(ctx, workerID, client, totalKeys, zipfExp, generator, stats,
		setRatio, getRatio, keyPrefix, keyMin, timeoutSeconds, workerCount, &opCount, zipfGen, verbose, limiter)
}

// manageTrafficPattern manages dynamic client scaling and QPS changes
func manageTrafficPattern(ctx context.Context, configs []TrafficConfig, cacheType string,
	cmd *cobra.Command, generator *DataGenerator, stats *WorkloadStats,
	setRatio, getRatio int, keyPrefix string, workerCount int, keyMin, totalKeys int, zipfExp float64,
	measureSetup, verbose, quiet bool, timeoutSeconds int) {
	connectionDelayMs, _ := cmd.Flags().GetInt("connection-delay-ms")
	var activeWorkers []context.CancelFunc
	var wg sync.WaitGroup
	startTime := time.Now()

	for i, config := range configs {
		// Wait until it's time for this configuration
		targetTime := time.Duration(config.TimeSeconds) * time.Second
		elapsed := time.Since(startTime)
		if targetTime > elapsed {
			select {
			case <-time.After(targetTime - elapsed):
			case <-ctx.Done():
				fmt.Printf("\nTraffic manager: Context cancelled while waiting for config %d\n", i+1)
				return
			}
		}

		// Start new time block tracking
		stats.StartTimeBlock(config)

		qpsStr := "unlimited"
		if config.QPS != -1 {
			qpsStr = fmt.Sprintf("%d", config.QPS)
		}

		// Always log scaling events (not just in verbose mode)
		fmt.Printf("\nTime %ds: Scaling to %d clients, %s QPS (config %d/%d)\n",
			config.TimeSeconds, config.Clients, qpsStr, i+1, len(configs))

		// Stop excess workers if scaling down
		currentWorkers := len(activeWorkers)
		if config.Clients < currentWorkers {
			stoppedWorkers := currentWorkers - config.Clients
			fmt.Printf("  Stopping %d workers (scaling down from %d to %d)\n",
				stoppedWorkers, currentWorkers, config.Clients)
			for i := config.Clients; i < currentWorkers; i++ {
				activeWorkers[i]() // Cancel the worker
			}
			activeWorkers = activeWorkers[:config.Clients]
		}

		// Start new workers if scaling up (create connections in parallel)
		newWorkers := config.Clients - currentWorkers
		if newWorkers > 0 {
			fmt.Printf("  Starting %d new workers (scaling up from %d to %d)\n",
				newWorkers, currentWorkers, config.Clients)

			for i := currentWorkers; i < config.Clients; i++ {
				// Check if context is still valid
				select {
				case <-ctx.Done():
					fmt.Printf("  Context cancelled while starting worker %d\n", i)
					return
				default:
				}

				// Create rate limiter
				var limiter *rate.Limiter
				if config.QPS > 0 {
					clientRPS := float64(config.QPS) / float64(config.Clients)
					limiter = rate.NewLimiter(rate.Limit(clientRPS), 1)
				}

				// Create worker context
				workerCtx, workerCancel := context.WithCancel(ctx)
				activeWorkers = append(activeWorkers, workerCancel)

				wg.Add(1)

				// Add connection delay to prevent connection storms
				if connectionDelayMs > 0 {
					time.Sleep(time.Duration(connectionDelayMs) * time.Millisecond)
				}
				switch cacheType {
				case "redis":
					// Get Redis worker count
					redisWorkerCount, _ := cmd.Flags().GetInt("redis-client-worker-count")
					go runRedisWorkerWithConnectionCreation(workerCtx, &wg, i, cacheType, cmd, totalKeys, zipfExp,
						generator, stats, redisWorkerCount, setRatio, getRatio, keyPrefix, keyMin, limiter,
						timeoutSeconds, measureSetup, verbose, quiet)
				case "momento":
					go runMomentoWorkerWithConnectionCreation(workerCtx, &wg, i, cacheType, cmd, totalKeys, zipfExp,
						generator, stats, workerCount, setRatio, getRatio, keyPrefix, keyMin, limiter,
						timeoutSeconds, measureSetup, verbose, quiet)
				default:
					log.Fatalf("Invalid cache type: %s", cacheType)
				}
			}
			fmt.Printf("  Successfully initiated %d new workers\n", newWorkers)
		}
	}

	// Wait for context cancellation
	<-ctx.Done()

	// Cancel all workers
	for _, cancel := range activeWorkers {
		cancel()
	}

	// Wait for all workers to finish
	wg.Wait()
}

// reportProgress reports workload progress with a progress bar
func reportProgress(ctx context.Context, stats *WorkloadStats, verbose bool, tcpMonitor *TCPMonitor) {
	ticker := time.NewTicker(MetricWindowSizeSeconds * time.Second)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			// Clear the progress line and print final newline
			fmt.Print("\r" + strings.Repeat(" ", 150) + "\r")
			return
		case <-ticker.C:
			getOps := atomic.LoadInt64(&stats.GetOps)
			setOps := atomic.LoadInt64(&stats.SetOps)
			getErrors := atomic.LoadInt64(&stats.GetErrors)
			setErrors := atomic.LoadInt64(&stats.SetErrors)

			totalOps := getOps + setOps
			elapsed := time.Since(startTime)

			if totalOps > 0 {
				// Create progress bar
				progressBar := createProgressBar(elapsed, stats)

				// Get current client count and target info
				currentClients := getCurrentClientCount(stats)
				targetClients, targetQPS := getCurrentTargetInfo(stats)

				// Get current second stats for progress bar display
				getCurrentOps, getP50, getP95, getP99, getMax := stats.GetStats.GetPreviousWindowStats()
				setCurrentOps, setP50, setP95, setP99, setMax := stats.SetStats.GetPreviousWindowStats()

				// Calculate current metric window AVG QPS
				currentWindowGetOps := float64(getCurrentOps / MetricWindowSizeSeconds)
				currentWindowSetOps := float64(setCurrentOps / MetricWindowSizeSeconds)
				currentTotalQPS := currentWindowGetOps + currentWindowSetOps

				// Get system resource usage
				sysStats := getSystemStats(tcpMonitor)
				procMemMB := getProcessMemoryMB()

				// Create metrics snapshot and log to CSV
				if stats.CSVLogger != nil {
					// Get connection stats
					activeConns := atomic.LoadInt64(&stats.ActiveConnections)
					failedConns := atomic.LoadInt64(&stats.FailedConnections)

					snapshot := MetricsSnapshot{
						Timestamp:         time.Now(),
						ElapsedSeconds:    int(elapsed.Seconds()),
						TargetClients:     targetClients,
						ActualClients:     int(activeConns),
						FailedConnections: failedConns,
						TargetQPS:         targetQPS,
						ActualTotalQPS:    currentTotalQPS,
						ActualGetQPS:      currentWindowGetOps,
						ActualSetQPS:      currentWindowSetOps,
						TotalOps:          totalOps,
						GetOps:            getOps,
						SetOps:            setOps,
						GetErrors:         getErrors,
						SetErrors:         setErrors,
						GetLatencyP50:     getP50,
						GetLatencyP95:     getP95,
						GetLatencyP99:     getP99,
						GetLatencyMax:     getMax,
						SetLatencyP50:     setP50,
						SetLatencyP95:     setP95,
						SetLatencyP99:     setP99,
						SetLatencyMax:     setMax,
						NetworkRxMBps:     sysStats.NetworkRxMBps,
						NetworkTxMBps:     sysStats.NetworkTxMBps,
						NetworkRxPPS:      sysStats.NetworkRxPPS,
						NetworkTxPPS:      sysStats.NetworkTxPPS,
						MemoryUsedGB:      sysStats.MemoryUsedMB / 1024,
						MemoryTotalGB:     sysStats.MemoryTotalMB / 1024,
						CPUPercent:        sysStats.CPUPercent,
						ProcessMemoryGB:   procMemMB / 1024,
						TotalOutBoundConn: sysStats.OutboundTCPConns,
						ActiveTCPConns:    sysStats.ActiveTCPConns,
						TCPConnDelta:      sysStats.TCPConnDelta,
					}
					stats.CSVLogger.LogMetrics(snapshot)
				}

				activeConns := atomic.LoadInt64(&stats.ActiveConnections)
				failedConns := atomic.LoadInt64(&stats.FailedConnections)
				// Format the progress line with resource monitoring
				progressLine := fmt.Sprintf(
					"\n%s\n"+
						"Clients : %d\n"+
						"\n"+
						"Active conns : %d\tFailed conns : %d\n"+
						"\n"+
						"Throughput\n"+
						"  Ops/s   : Overall: %.0f  |  GET: %.0f/s  |  SET: %.0f/s\n"+
						"\n"+
						"Latency\n"+
						"  GET     : p50 %.2f ms | p99 %.2f ms\n"+
						"  SET     : p50 %.2f ms | p99 %.2f ms\n"+
						"\n"+
						"System\n"+
						"  Memory  : %.1fGB / %.1fGB\n"+
						"  CPU     : %.0f%%\n"+
						"  ProcMem : %.1fGB\n"+
						"  Network : Rx %.1f MB/s | Tx %.1f MB/s\n"+
						"  TCP Conns: %d (Δ%+d) | OutBound: %d",
					progressBar,
					currentClients,
					activeConns,
					failedConns,
					currentTotalQPS, currentWindowGetOps, currentWindowSetOps,
					float64(getP50)/1000.0, float64(getP99)/1000.0,
					float64(setP50)/1000.0, float64(setP99)/1000.0,
					sysStats.MemoryUsedMB/1024, sysStats.MemoryTotalMB/1024,
					sysStats.CPUPercent,
					procMemMB/1024,
					sysStats.NetworkRxMBps, sysStats.NetworkTxMBps,
					sysStats.ActiveTCPConns, sysStats.TCPConnDelta, sysStats.OutboundTCPConns,
				)

				// Truncate if too long for terminal
				if len(progressLine) > 150 {
					progressLine = progressLine[:147] + "..."
				}

				fmt.Print(progressLine)
			}
		}
	}
}

// createProgressBar creates a visual progress bar based on current time block
func createProgressBar(elapsed time.Duration, stats *WorkloadStats) string {
	stats.BlockMutex.RLock()
	defer stats.BlockMutex.RUnlock()

	if stats.CurrentBlock == nil {
		return "[----] 00:00"
	}

	blockElapsed := elapsed
	if !stats.CurrentBlock.StartTime.IsZero() {
		blockElapsed = time.Since(stats.CurrentBlock.StartTime)
	}

	// Create a simple progress indicator
	barWidth := 20
	progress := int(blockElapsed.Seconds()) % (barWidth * 2)
	if progress > barWidth {
		progress = (barWidth * 2) - progress
	}

	bar := "["
	for i := 0; i < barWidth; i++ {
		if i < progress {
			bar += "="
		} else if i == progress {
			bar += ">"
		} else {
			bar += "-"
		}
	}
	bar += "]"

	// Add elapsed time
	minutes := int(elapsed.Minutes())
	seconds := int(elapsed.Seconds()) % 60
	timeStr := fmt.Sprintf("%02d:%02d", minutes, seconds)

	return fmt.Sprintf("%s %s", bar, timeStr)
}

// getCurrentClientCount returns the current number of active clients
func getCurrentClientCount(stats *WorkloadStats) int {
	stats.BlockMutex.RLock()
	defer stats.BlockMutex.RUnlock()

	if stats.CurrentBlock != nil {
		return stats.CurrentBlock.Config.Clients
	}
	return 0
}

// getCurrentTargetInfo returns current target clients and QPS
func getCurrentTargetInfo(stats *WorkloadStats) (int, int) {
	stats.BlockMutex.RLock()
	defer stats.BlockMutex.RUnlock()

	if stats.CurrentBlock != nil {
		return stats.CurrentBlock.Config.Clients, stats.CurrentBlock.Config.QPS
	}
	return 0, 0
}

// reportStaticProgress reports progress for static workload with progress bar
func reportStaticProgress(ctx context.Context, stats *WorkloadStats, testTime int, clientCount int, verbose bool, cloudwatchConfig *CloudWatchConfig, tcpMonitor *TCPMonitor) {
	ticker := time.NewTicker(MetricWindowSizeSeconds * time.Second)
	defer ticker.Stop()

	startTime := time.Now()
	totalDuration := time.Duration(testTime) * time.Second

	for {
		select {
		case <-ctx.Done():
			// Clear the progress line
			//fmt.Print("\r" + strings.Repeat(" ", 150) + "\r")
			return
		case <-ticker.C:
			getOps := atomic.LoadInt64(&stats.GetOps)
			setOps := atomic.LoadInt64(&stats.SetOps)
			getErrors := atomic.LoadInt64(&stats.GetErrors)
			setErrors := atomic.LoadInt64(&stats.SetErrors)

			totalOps := getOps + setOps
			elapsed := time.Since(startTime)

			if totalOps > 0 {
				// Get current monitoring window stats
				getCurrentOps, getP50, getP95, getP99, getMax := stats.GetStats.GetPreviousWindowStats()
				setCurrentOps, setP50, setP95, setP99, setMax := stats.SetStats.GetPreviousWindowStats()

				// Calculate current metric window AVG QPS
				currentWindowGetOps := float64(getCurrentOps / MetricWindowSizeSeconds)
				currentWindowSetOps := float64(setCurrentOps / MetricWindowSizeSeconds)
				currentTotalQPS := currentWindowGetOps + currentWindowSetOps

				// Create progress bar for static workload
				progressBar := createStaticProgressBar(elapsed, totalDuration)

				// Get system resource usage
				sysStats := getSystemStats(tcpMonitor)
				procMemMB := getProcessMemoryMB()

				// Create metrics snapshot and log to CSV
				if stats.CSVLogger != nil {
					// Get connection stats
					activeConns := atomic.LoadInt64(&stats.ActiveConnections)
					failedConns := atomic.LoadInt64(&stats.FailedConnections)

					snapshot := MetricsSnapshot{
						Timestamp:         time.Now(),
						ElapsedSeconds:    int(elapsed.Seconds()),
						TargetClients:     clientCount,
						ActualClients:     int(activeConns),
						FailedConnections: failedConns,
						TargetQPS:         -1, // Static workload doesn't have target QPS
						ActualTotalQPS:    currentTotalQPS,
						ActualGetQPS:      currentWindowGetOps,
						ActualSetQPS:      currentWindowSetOps,
						TotalOps:          totalOps,
						GetOps:            getOps,
						SetOps:            setOps,
						GetErrors:         getErrors,
						SetErrors:         setErrors,
						GetLatencyP50:     getP50,
						GetLatencyP95:     getP95,
						GetLatencyP99:     getP99,
						GetLatencyMax:     getMax,
						SetLatencyP50:     setP50,
						SetLatencyP95:     setP95,
						SetLatencyP99:     setP99,
						SetLatencyMax:     setMax,
						NetworkRxMBps:     sysStats.NetworkRxMBps,
						NetworkTxMBps:     sysStats.NetworkTxMBps,
						NetworkRxPPS:      sysStats.NetworkRxPPS,
						NetworkTxPPS:      sysStats.NetworkTxPPS,
						MemoryUsedGB:      sysStats.MemoryUsedMB / 1024,
						MemoryTotalGB:     sysStats.MemoryTotalMB / 1024,
						CPUPercent:        sysStats.CPUPercent,
						ProcessMemoryGB:   procMemMB / 1024,
						TotalOutBoundConn: sysStats.OutboundTCPConns,
						ActiveTCPConns:    sysStats.ActiveTCPConns,
						TCPConnDelta:      sysStats.TCPConnDelta,
					}
					stats.CSVLogger.LogMetrics(snapshot)
				}

				progressLine := fmt.Sprintf(
					"\n%s\n"+
						"Clients : %d\n"+
						"\n"+
						"Throughput\n"+
						"  Ops/s   : Overall: %.0f  |  GET: %.0f/s  |  SET: %.0f/s\n"+
						"\n"+
						"Latency\n"+
						"  GET     : p50 %.2f ms | p99 %.2f ms\n"+
						"  SET     : p50 %.2f ms | p99 %.2f ms\n"+
						"\n"+
						"System\n"+
						"  Memory  : %.1fGB / %.1fGB\n"+
						"  CPU     : %.0f%%\n"+
						"  ProcMem : %.1fGB\n"+
						"  Network : Rx %.1f MB/s | Tx %.1f MB/s\n"+
						"  TCP Conns: %d (Δ%+d) | OutBound: %d",
					progressBar,
					clientCount,
					currentTotalQPS, currentWindowGetOps, currentWindowSetOps,
					float64(getP50)/1000.0, float64(getP99)/1000.0,
					float64(setP50)/1000.0, float64(setP99)/1000.0,
					sysStats.MemoryUsedMB/1024, sysStats.MemoryTotalMB/1024,
					sysStats.CPUPercent,
					procMemMB/1024,
					sysStats.NetworkRxMBps, sysStats.NetworkTxMBps,
					sysStats.ActiveTCPConns, sysStats.TCPConnDelta, sysStats.OutboundTCPConns,
				)

				fmt.Print(progressLine)

				// Emit CloudWatch metrics
				cloudwatchConfig.emitCloudWatchMetrics(currentWindowGetOps, currentWindowSetOps, currentTotalQPS, getP99, setP99, sysStats.OutboundTCPConns, sysStats.ActiveTCPConns, sysStats.TCPConnDelta)
			}
		}
	}
}

// createStaticProgressBar creates a progress bar for static workload
func createStaticProgressBar(elapsed, total time.Duration) string {
	barWidth := 10
	progress := float64(elapsed) / float64(total)
	if progress > 1.0 {
		progress = 1.0
	}

	filled := int(progress * float64(barWidth))

	bar := "["
	for i := 0; i < barWidth; i++ {
		if i < filled {
			bar += "="
		} else if i == filled && progress < 1.0 {
			bar += ">"
		} else {
			bar += "-"
		}
	}
	bar += "]"

	// Add elapsed/total time
	elapsedMin := int(elapsed.Minutes())
	elapsedSec := int(elapsed.Seconds()) % 60
	totalMin := int(total.Minutes())
	totalSec := int(total.Seconds()) % 60

	timeStr := fmt.Sprintf("%02d:%02d/%02d:%02d", elapsedMin, elapsedSec, totalMin, totalSec)

	return fmt.Sprintf("%s %s", bar, timeStr)
}

// printFinalResults prints the final workload results
func printFinalResults(stats *WorkloadStats, testTime int, measureSetup bool) {
	getOps := atomic.LoadInt64(&stats.GetOps)
	setOps := atomic.LoadInt64(&stats.SetOps)
	getErrors := atomic.LoadInt64(&stats.GetErrors)
	setErrors := atomic.LoadInt64(&stats.SetErrors)
	activeConns := atomic.LoadInt64(&stats.ActiveConnections)
	failedConns := atomic.LoadInt64(&stats.FailedConnections)

	totalOps := getOps + setOps
	totalErrors := getErrors + setErrors

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("WORKLOAD RESULTS")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Printf("Test Duration: %d seconds\n", testTime)
	fmt.Printf("Total Operations: %d\n", totalOps)
	fmt.Printf("Total Errors: %d (%.2f%%)\n", totalErrors, float64(totalErrors)/float64(totalOps)*100)
	fmt.Printf("Active Connections: %d\n", activeConns)
	fmt.Printf("Failed Connections: %d\n", failedConns)
	fmt.Println()

	// Client setup statistics (only if measurement was enabled)
	if measureSetup {
		_, _, _, _, setupP50, setupP95, setupP99 := stats.SetupStats.GetStats()
		setupCount := stats.SetupStats.Histogram.TotalCount()
		if setupCount > 0 {
			fmt.Printf("Client Setup Statistics (%d clients):\n", setupCount)
			fmt.Printf("Setup Time - P50: %d μs, P95: %d μs, P99: %d μs\n", setupP50, setupP95, setupP99)
			fmt.Printf("(includes client creation + ping/connectivity test)\n")
			fmt.Println()
		}
	}

	// GET statistics
	if getOps > 0 {
		getQPS := float64(getOps) / float64(testTime)
		_, _, _, _, getP50, getP95, getP99 := stats.GetStats.GetStats()

		fmt.Printf("GET Operations: %d\n", getOps)
		fmt.Printf("AVG GET QPS: %.2f\n", getQPS)
		fmt.Printf("GET Errors: %d (%.2f%%)\n", getErrors, float64(getErrors)/float64(getOps)*100)
		fmt.Printf("GET Latency - P50: %d μs, P95: %d μs, P99: %d μs\n", getP50, getP95, getP99)
		fmt.Println()
	}

	// SET statistics
	if setOps > 0 {
		setQPS := float64(setOps) / float64(testTime)
		_, _, _, _, setP50, setP95, setP99 := stats.SetStats.GetStats()

		fmt.Printf("SET Operations: %d\n", setOps)
		fmt.Printf("AVG SET QPS: %.2f\n", setQPS)
		fmt.Printf("SET Errors: %d (%.2f%%)\n", setErrors, float64(setErrors)/float64(setOps)*100)
		fmt.Printf("SET Latency - P50: %d μs, P95: %d μs, P99: %d μs\n", setP50, setP95, setP99)
	}

	fmt.Println(strings.Repeat("=", 60))
}

// printDynamicFinalResults prints results with time block breakdown
func printDynamicFinalResults(stats *WorkloadStats, configs []TrafficConfig, measureSetup bool) {
	getOps := atomic.LoadInt64(&stats.GetOps)
	setOps := atomic.LoadInt64(&stats.SetOps)
	getErrors := atomic.LoadInt64(&stats.GetErrors)
	setErrors := atomic.LoadInt64(&stats.SetErrors)
	activeConns := atomic.LoadInt64(&stats.ActiveConnections)
	failedConns := atomic.LoadInt64(&stats.FailedConnections)

	totalOps := getOps + setOps
	totalErrors := getErrors + setErrors

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("DYNAMIC WORKLOAD RESULTS")
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("Total Operations: %d\n", totalOps)
	fmt.Printf("Total Errors: %d (%.2f%%)\n", totalErrors, float64(totalErrors)/float64(totalOps)*100)
	fmt.Printf("Active Connections: %d\n", activeConns)
	fmt.Printf("Failed Connections: %d\n", failedConns)
	fmt.Println()

	// Overall statistics
	if getOps > 0 {
		_, _, _, _, getP50, getP95, getP99 := stats.GetStats.GetStats()
		fmt.Printf("Overall GET - Ops: %d, Errors: %d, P50: %d μs, P95: %d μs, P99: %d μs\n",
			getOps, getErrors, getP50, getP95, getP99)
	}

	if setOps > 0 {
		_, _, _, _, setP50, setP95, setP99 := stats.SetStats.GetStats()
		fmt.Printf("Overall SET - Ops: %d, Errors: %d, P50: %d μs, P95: %d μs, P99: %d μs\n",
			setOps, setErrors, setP50, setP95, setP99)
	}
	fmt.Println()

	// Client setup statistics (only if measurement was enabled)
	if measureSetup {
		_, _, _, _, setupP50, setupP95, setupP99 := stats.SetupStats.GetStats()
		setupCount := stats.SetupStats.Histogram.TotalCount()
		if setupCount > 0 {
			fmt.Printf("Client Setup Statistics (%d clients):\n", setupCount)
			fmt.Printf("Setup Time - P50: %d μs, P95: %d μs, P99: %d μs\n", setupP50, setupP95, setupP99)
			fmt.Printf("(includes client creation + ping/connectivity test)\n")
			fmt.Println()
		}
	}

	// Time block breakdown
	fmt.Println("TIME BLOCK BREAKDOWN:")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-8s %-8s %-12s %-12s %-12s %-12s %-12s %-12s\n",
		"Time", "Clients", "Target QPS", "Actual QPS", "GET Ops", "SET Ops", "GET P95", "SET P95")
	fmt.Println(strings.Repeat("-", 80))

	for _, block := range stats.TimeBlocks {
		duration := block.EndTime.Sub(block.StartTime).Seconds()
		actualGetOps := atomic.LoadInt64(&block.ActualGetOps)
		actualSetOps := atomic.LoadInt64(&block.ActualSetOps)
		totalBlockOps := actualGetOps + actualSetOps
		actualQPS := float64(totalBlockOps) / duration

		targetQPSStr := "unlimited"
		if block.Config.QPS != -1 {
			targetQPSStr = fmt.Sprintf("%d", block.Config.QPS)
		}

		var getP95, setP95 int64
		if actualGetOps > 0 {
			_, _, _, _, _, getP95, _ = block.GetStats.GetStats()
		}
		if actualSetOps > 0 {
			_, _, _, _, _, setP95, _ = block.SetStats.GetStats()
		}

		fmt.Printf("%-8ds %-8d %-12s %-12.0f %-12d %-12d %-12d %-12d\n",
			block.Config.TimeSeconds, block.Config.Clients, targetQPSStr, actualQPS,
			actualGetOps, actualSetOps, getP95, setP95)
	}

	fmt.Println(strings.Repeat("=", 80))
}

// runConnectionSetupBenchmark benchmarks connection setup time
func runConnectionSetupBenchmark(cmd *cobra.Command, args []string) {
	// Get command parameters
	cacheType, _ := cmd.Flags().GetString("cache-type")
	clientCount, _ := cmd.Flags().GetInt("clients")
	timeoutSeconds, _ := cmd.Flags().GetInt("timeout")
	verbose, _ := cmd.Flags().GetBool("verbose")

	// Enable block profiling for connection setup benchmark if not already set
	blockProfileRate, _ := cmd.Flags().GetInt("block-profile-rate")
	if blockProfileRate > 0 {
		runtime.SetBlockProfileRate(blockProfileRate)
		fmt.Printf("Block profiling enabled for connection setup benchmark (rate: %d)\n", blockProfileRate)
	}

	// Enable mutex profiling for connection setup benchmark if not already set
	mutexProfileFraction, _ := cmd.Flags().GetInt("mutex-profile-fraction")
	if mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(mutexProfileFraction)
		fmt.Printf("Mutex profiling enabled for connection setup benchmark (fraction: %d)\n", mutexProfileFraction)
	}

	fmt.Printf("=== Connection Setup Benchmark ===\n")
	fmt.Printf("Cache Type: %s\n", cacheType)
	fmt.Printf("Target Connections: %d\n", clientCount)
	fmt.Printf("Connection Timeout: %d seconds\n", timeoutSeconds)
	fmt.Println()

	// Create performance stats for connection setup times
	setupStats := NewPerformanceStats()
	defer setupStats.Close()

	// Track successful and failed connections
	var successCount, failureCount int64
	var wg sync.WaitGroup

	fmt.Printf("Creating %d connections as fast as possible...\n", clientCount)
	startTime := time.Now()

	// Start progress reporting
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reportConnectionSetupProgress(ctx, &successCount, &failureCount, clientCount, startTime, setupStats)

	// Create connections concurrently
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()

			// Measure connection setup time (create + ping)
			connStart := time.Now()
			client, err := createCacheClientForRun(ctx, cacheType, cmd)
			if err != nil {
				atomic.AddInt64(&failureCount, 1)
				if verbose {
					log.Printf("Connection %d failed to create: %v", connID, err)
				}
				return
			}
			defer client.Close()

			// Test connectivity with ping
			// PING Already happens under hood when establishing client dont need to do a second time
			//ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
			//defer cancel()
			//
			//err = client.Ping(ctx)
			setupTime := time.Since(connStart)

			if err != nil {
				atomic.AddInt64(&failureCount, 1)
				if verbose {
					log.Printf("Connection %d ping failed: %v", connID, err)
				}
			} else {
				atomic.AddInt64(&successCount, 1)
				setupStats.RecordLatency(setupTime.Microseconds())
				if verbose {
					log.Printf("Connection %d setup successful in %v", connID, setupTime)
				}
			}
		}(i)
	}

	// Wait for all connections to complete
	wg.Wait()
	cancel() // Stop progress reporting
	totalTime := time.Since(startTime)

	// Clear progress line
	fmt.Print("\r" + strings.Repeat(" ", 120) + "\r")

	// Get final statistics from atomic counters
	finalSuccessCount := atomic.LoadInt64(&successCount)
	finalFailureCount := atomic.LoadInt64(&failureCount)

	// Get latency statistics from performance stats
	_, _, _, _, p50, p95, p99 := setupStats.GetStats()
	maxSetupTime := setupStats.Histogram.Max()

	fmt.Printf("\n=== Connection Setup Results ===\n")
	fmt.Printf("Total Time: %v\n", totalTime)
	fmt.Printf("Total Connections Attempted: %d\n", clientCount)
	fmt.Printf("Successful Connections: %d\n", finalSuccessCount)
	fmt.Printf("Failed Connections: %d\n", finalFailureCount)
	fmt.Printf("Success Rate: %.2f%%\n", float64(finalSuccessCount)/float64(clientCount)*100)
	fmt.Printf("Connections per Second: %.2f\n", float64(finalSuccessCount)/totalTime.Seconds())
	fmt.Println()
	fmt.Printf("Connection Setup Latency Statistics:\n")
	fmt.Printf("  P50: %d μs (%.2f ms)\n", p50, float64(p50)/1000)
	fmt.Printf("  P95: %d μs (%.2f ms)\n", p95, float64(p95)/1000)
	fmt.Printf("  P99: %d μs (%.2f ms)\n", p99, float64(p99)/1000)
	fmt.Printf("  Max: %d μs (%.2f ms)\n", maxSetupTime, float64(maxSetupTime)/1000)
	fmt.Println()

	if finalFailureCount > 0 {
		fmt.Printf("Note: %d connections failed. Check network connectivity and server capacity.\n", finalFailureCount)
	}
}

// reportConnectionSetupProgress reports real-time progress for connection setup benchmark
func reportConnectionSetupProgress(ctx context.Context, successCount, failureCount *int64, totalConnections int, startTime time.Time, setupStats *PerformanceStats) {
	ticker := time.NewTicker(500 * time.Millisecond) // Update every 500ms for responsive feedback
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Clear the progress line
			fmt.Print("\r" + strings.Repeat(" ", 120) + "\r")
			return
		case <-ticker.C:
			currentSuccess := atomic.LoadInt64(successCount)
			currentFailure := atomic.LoadInt64(failureCount)
			totalCompleted := currentSuccess + currentFailure

			elapsed := time.Since(startTime)

			if totalCompleted > 0 {
				// Calculate progress percentage
				progress := float64(totalCompleted) / float64(totalConnections)
				if progress > 1.0 {
					progress = 1.0
				}

				// Create progress bar
				progressBar := createConnectionProgressBar(progress, elapsed)

				// Calculate connection rate
				connRate := float64(totalCompleted) / elapsed.Seconds()

				// Get median connection time from stats
				var medianTime int64
				if currentSuccess > 0 {
					_, _, _, _, medianTime, _, _ = setupStats.GetStats()
				}

				// Format the progress line
				progressLine := fmt.Sprintf("\r%s | Completed: %d/%d | Success: %d | Failed: %d | Rate: %.0f conn/s | Median: %.0f ms",
					progressBar, totalCompleted, totalConnections, currentSuccess, currentFailure,
					connRate, float64(medianTime)/1000)

				// Truncate if too long for terminal
				if len(progressLine) > 120 {
					progressLine = progressLine[:117] + "..."
				}

				fmt.Print(progressLine)
			}
		}
	}
}

// createConnectionProgressBar creates a progress bar for connection setup
func createConnectionProgressBar(progress float64, elapsed time.Duration) string {
	barWidth := 20
	filled := int(progress * float64(barWidth))

	bar := "["
	for i := 0; i < barWidth; i++ {
		if i < filled {
			bar += "="
		} else if i == filled && progress < 1.0 {
			bar += ">"
		} else {
			bar += "-"
		}
	}
	bar += "]"

	// Add elapsed time and percentage
	seconds := int(elapsed.Seconds())
	minutes := seconds / 60
	secs := seconds % 60
	timeStr := fmt.Sprintf("%02d:%02d", minutes, secs)

	return fmt.Sprintf("%s %s (%.1f%%)", bar, timeStr, progress*100)
}

func init() {
	rootCmd.AddCommand(runCmd)

	// Cache Type Options
	runCmd.Flags().StringP("cache-type", "t", "redis", "Cache type: redis or momento")

	// Client Options
	defaultClients := runtime.NumCPU()
	runCmd.Flags().IntP("clients", "c", defaultClients, "Number of concurrent clients")
	runCmd.Flags().IntP("rps", "r", 0, "Rate limit in requests per second (0 = unlimited)")
	runCmd.Flags().IntP("timeout", "T", 10, "Operation timeout in seconds")
	runCmd.Flags().BoolP("verbose", "v", false, "Enable verbose output")
	runCmd.Flags().Bool("conn-setup-only", false, "Only benchmark connection setup time (create connections + PING as fast as possible)")

	// Profiling Options
	runCmd.Flags().String("cpu-profile", "", "Write CPU profile to file")
	runCmd.Flags().String("mem-profile", "", "Write memory profile to file")
	runCmd.Flags().String("block-profile", "", "Write block profile to file")
	runCmd.Flags().String("mutex-profile", "", "Write mutex profile to file")
	runCmd.Flags().String("pprof-addr", "", "Enable pprof HTTP server on address (e.g., localhost:6060)")
	runCmd.Flags().Int("block-profile-rate", 1, "Block profile rate (0 = disabled, 1 = every blocking event)")
	runCmd.Flags().Int("mutex-profile-fraction", 1, "Mutex profile fraction (0 = disabled, 1 = every mutex contention)")

	// Redis Options (reuse from populate)
	runCmd.Flags().StringP("redis-uri", "u", "redis://localhost:6379", "Redis URI")
	runCmd.Flags().Bool("cluster-mode", false, "Run client in cluster mode")
	runCmd.Flags().Int("redis-dial-timeout", 60, "Redis dial timeout in seconds")
	runCmd.Flags().Int("redis-read-timeout", 60, "Redis read timeout in seconds")
	runCmd.Flags().Int("redis-write-timeout", 60, "Redis write timeout in seconds")
	runCmd.Flags().Int("redis-pool-timeout", 120, "Redis connection pool timeout in seconds")
	runCmd.Flags().Int("redis-conn-max-idle-time", 120, "Redis connection max idle time in seconds")
	runCmd.Flags().Int("redis-max-retries", 3, "Redis maximum number of retries")
	runCmd.Flags().Int("redis-min-retry-backoff", 1000, "Redis minimum retry backoff in milliseconds")
	runCmd.Flags().Int("redis-max-retry-backoff", 10000, "Redis maximum retry backoff in milliseconds")
	runCmd.Flags().Int("redis-pool-size", 10, "Redis connection pool size per worker")
	runCmd.Flags().Int("redis-client-worker-count", 1, "Set number of workload generators for each redis client")

	// Momento Options (reuse from populate)
	runCmd.Flags().String("momento-api-key", "", "Momento API key (or set MOMENTO_API_KEY env var)")
	runCmd.Flags().String("momento-cache-name", "test-cache", "Momento cache name")
	runCmd.Flags().Bool("momento-create-cache", true, "Automatically create Momento cache if it doesn't exist")
	runCmd.Flags().Int("connection-timeout", 180, "Connection timeout in seconds for client creation and ping")
	runCmd.Flags().Int("connection-delay-ms", 0, "Delay in milliseconds between worker connection attempts (helps with connection storms)")
	runCmd.Flags().Uint32("momento-client-conn-count", 1, "Set number of TCP conn each momento client creates")
	runCmd.Flags().Int("momento-client-worker-count", 1, "Set number of workload generators for each momento client")

	// Workload-specific Options
	runCmd.Flags().Float64("key-zipf-exp", 1.0, "Zipf distribution exponent (0 < exp <= 5), higher = more concentration")
	runCmd.Flags().Int("test-time", 60, "Number of seconds to run the test")
	runCmd.Flags().String("ratio", "1:10", "Set:Get ratio (e.g., 1:10 means 1 set for every 10 gets)")
	runCmd.Flags().Bool("measure-setup", true, "Measure client setup time including ping/connectivity test")
	runCmd.Flags().String("traffic-pattern", "", "CSV file with traffic pattern (time_seconds,clients,qps). Overrides --clients and --rps")
	runCmd.Flags().String("csv-output", "", "CSV file to log performance metrics (default: auto-generated filename)")
	runCmd.Flags().Bool("quiet", false, "Suppress verbose output and worker creation logs")
	runCmd.Flags().Int("default-ttl", 3600, "Default TTL in seconds for cache entries (0 = no expiration for Redis, 60s minimum for Momento)")

	// Key Options
	runCmd.Flags().String("key-prefix", "memtier-", "Prefix for keys")
	runCmd.Flags().Int("key-minimum", 0, "Key ID minimum value")
	runCmd.Flags().Int("key-maximum", 10000000, "Key ID maximum value")

	// Data Options
	runCmd.Flags().IntP("data-size", "d", 32, "Object data size in bytes")
	runCmd.Flags().BoolP("random-data", "R", false, "Use random data instead of pattern data")

	// CloudWatch Options
	runCmd.Flags().Bool("cloudwatch-enabled", false, "Enable CloudWatch metrics emission")
	runCmd.Flags().String("cloudwatch-region", "us-east-2", "AWS region for CloudWatch metrics")
	runCmd.Flags().String("cloudwatch-namespace", "MomentoBenchmark", "CloudWatch namespace for metrics")
}
