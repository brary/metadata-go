package transaction

import (
	"context"
	"fmt"
	"sync"

	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/kv"
)

// Transaction represents a database transaction
type Transaction struct {
	txn    kv.Transaction
	ctx    context.Context
	mu     sync.Mutex
	active bool
}

// NewTransaction creates a new transaction
func NewTransaction(ctx context.Context, client *txnkv.Client) (*Transaction, error) {
	txn, err := client.Begin()
	if err != nil {
		return nil, err
	}

	return &Transaction{
		txn:    txn,
		ctx:    ctx,
		active: true,
	}, nil
}

// Get retrieves a value from the transaction
func (t *Transaction) Get(key []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return nil, ErrTransactionClosed
	}

	return t.txn.Get(t.ctx, key)
}

// Set sets a value in the transaction
func (t *Transaction) Set(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	return t.txn.Set(key, value)
}

// Delete removes a key from the transaction
func (t *Transaction) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	return t.txn.Delete(key)
}

// Commit commits the transaction
func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	err := t.txn.Commit(t.ctx)
	if err == nil {
		t.active = false
	}
	return err
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return ErrTransactionClosed
	}

	err := t.txn.Rollback()
	if err == nil {
		t.active = false
	}
	return err
}

// Errors
var (
	ErrTransactionClosed = fmt.Errorf("transaction is closed")
) 