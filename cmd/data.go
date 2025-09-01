/*
Copyright © 2025 Redis Performance Group  <performance <at> redis <dot> com>
*/

package cmd

import (
	cryptorand "crypto/rand"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// Pre-computed pattern for efficient data generation
var basePattern = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// fillPatternData efficiently fills a buffer with repeating pattern
func fillPatternData(data []byte) {
	patternLen := len(basePattern)

	// For small buffers, just copy byte by byte
	if len(data) <= patternLen {
		for i := range data {
			data[i] = basePattern[i%patternLen]
		}
		return
	}

	// For larger buffers, copy pattern once then use copy() to double efficiently
	copy(data[:patternLen], basePattern)

	// Double the pattern until we fill most of the buffer
	for copied := patternLen; copied < len(data); copied *= 2 {
		copy(data[copied:], data[:copied])
	}
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
		// For random data, fill with crypto random bytes
		_, err := cryptorand.Read(data)
		if err != nil {
			return nil, fmt.Errorf("failed to generate random data: %w", err)
		}
	} else {
		// For pattern data, use efficient pattern filling
		fillPatternData(data)
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
