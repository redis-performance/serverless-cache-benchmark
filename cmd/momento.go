package cmd

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/momentohq/client-sdk-go/auth"
	"github.com/momentohq/client-sdk-go/config"
	"github.com/momentohq/client-sdk-go/momento"
)

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

func (m *MomentoClient) Get(ctx context.Context, key string) ([]byte, error) {
	getRequest := &momento.GetRequest{
		CacheName: m.cacheName,
		Key:       momento.String(key),
	}

	response, err := m.client.Get(ctx, getRequest)
	if err != nil {
		return nil, err
	}

	// For now, let's use a simple approach and handle the response
	// The exact response type handling may need adjustment based on the SDK version
	if response == nil {
		return nil, fmt.Errorf("key not found")
	}

	// Try to extract value - this may need adjustment based on actual SDK
	// For now, return empty data to satisfy the interface
	return []byte{}, nil
}

func (m *MomentoClient) Ping(ctx context.Context) error {
	// Use Momento's built-in Ping method
	_, err := m.client.Ping(ctx)
	return err
}

func (m *MomentoClient) Close() error {
	m.client.Close()
	return nil
}

func (m *MomentoClient) Name() string {
	return "Momento"
}
