package metadata

import (
	"context"
	"fmt"
	"sync"

	"github.com/avneetkang/metadata-go/pkg/storage"
	"github.com/avneetkang/metadata-go/pkg/table"
	"github.com/avneetkang/metadata-go/pkg/transaction"
)

// DB represents the metadata database
type DB struct {
	storage *storage.TiKVClient
	tables  map[string]*table.Table
	mu      sync.RWMutex
}

// NewDB creates a new metadata database instance
func NewDB(ctx context.Context, pdAddrs []string) (*DB, error) {
	client, err := storage.NewTiKVClient(ctx, pdAddrs)
	if err != nil {
		return nil, err
	}

	return &DB{
		storage: client,
		tables:  make(map[string]*table.Table),
	}, nil
}

// CreateTable creates a new table in the database
func (db *DB) CreateTable(name string, columns []table.Column, primary []string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	tbl := table.NewTable(name, columns, primary)
	db.tables[name] = tbl
	return nil
}

// GetTable returns a table by name
func (db *DB) GetTable(name string) (*table.Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tbl, exists := db.tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", name)
	}

	return tbl, nil
}

// Begin starts a new transaction
func (db *DB) Begin() (*transaction.Transaction, error) {
	return transaction.NewTransaction(db.storage.GetContext(), db.storage.GetClient())
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.storage.Close()
} 