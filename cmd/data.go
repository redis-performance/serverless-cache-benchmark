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
