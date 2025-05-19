package storage

import (
	"context"
	"fmt"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
)

// TiKVClient represents a connection to a TiKV cluster
type TiKVClient struct {
	client *txnkv.Client
	ctx    context.Context
}

// NewTiKVClient creates a new TiKV client
func NewTiKVClient(ctx context.Context, pdAddrs []string) (*TiKVClient, error) {
	conf := config.Default()
	client, err := txnkv.NewClient(pdAddrs, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create TiKV client: %v", err)
	}

	return &TiKVClient{
		client: client,
		ctx:    ctx,
	}, nil
}

// Close closes the TiKV client
func (c *TiKVClient) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

// GetClient returns the underlying TiKV client
func (c *TiKVClient) GetClient() *txnkv.Client {
	return c.client
}

// GetContext returns the context
func (c *TiKVClient) GetContext() context.Context {
	return c.ctx
} 