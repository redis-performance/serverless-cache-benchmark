package cmd

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// PerformanceStats tracks performance metrics with per-second latency tracking
type PerformanceStats struct {
	TotalOps   int64
	SuccessOps int64
	FailedOps  int64
	Histogram  *hdrhistogram.Histogram
	StartTime  time.Time
	mutex      sync.RWMutex
	// Per-second histograms
	currentSecond    int64
	currentHistogram *hdrhistogram.Histogram
	secondHistograms map[int64]*hdrhistogram.Histogram
	histogramMutex   sync.RWMutex
}

func NewPerformanceStats() *PerformanceStats {
	// Create histogram with 1 microsecond to 1 minute range, 3 significant digits
	hist := hdrhistogram.New(1, 60*1000*1000, 3)
	return &PerformanceStats{
		Histogram:        hist,
		StartTime:        time.Now(),
		secondHistograms: make(map[int64]*hdrhistogram.Histogram),
		currentHistogram: hdrhistogram.New(1, 60*1000*1000, 3),
	}
}

func (ps *PerformanceStats) RecordLatency(latencyMicros int64) {
	now := time.Now()
	second := now.Unix()

	// Record in overall histogram
	ps.mutex.Lock()
	ps.Histogram.RecordValue(latencyMicros)
	ps.mutex.Unlock()

	// Record in per-second histogram
	ps.histogramMutex.Lock()
	// Check if we need to create a new histogram for this second
	if second != ps.currentSecond {
		if ps.currentHistogram.TotalCount() > 0 {
			ps.secondHistograms[ps.currentSecond] = ps.currentHistogram
		}
		ps.currentSecond = second
		ps.currentHistogram = hdrhistogram.New(1, 60*1000*1000, 3)
	}
	ps.currentHistogram.RecordValue(latencyMicros)
	ps.histogramMutex.Unlock()

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

// GetCurrentSecondStats returns stats for the current second
func (ps *PerformanceStats) GetCurrentSecondStats() (int64, int64, int64, int64, int64) {
	ps.histogramMutex.RLock()
	defer ps.histogramMutex.RUnlock()

	if ps.currentHistogram.TotalCount() == 0 {
		return 0, 0, 0, 0, 0
	}

	return ps.currentHistogram.TotalCount(),
		ps.currentHistogram.ValueAtQuantile(50),
		ps.currentHistogram.ValueAtQuantile(95),
		ps.currentHistogram.ValueAtQuantile(99),
		ps.currentHistogram.Max()
}

// GetOverallStats returns overall statistics
func (ps *PerformanceStats) GetOverallStats() (int64, int64, int64, float64) {
	total := atomic.LoadInt64(&ps.TotalOps)
	success := atomic.LoadInt64(&ps.SuccessOps)
	failed := atomic.LoadInt64(&ps.FailedOps)
	qps := ps.GetQPS()

	return total, success, failed, qps
}
