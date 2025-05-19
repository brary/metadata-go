package table

import (
	"context"
	"os"
	"testing"

	"github.com/avneetkang/metadata-go/pkg/storage"
	"github.com/avneetkang/metadata-go/pkg/transaction"
	"github.com/stretchr/testify/assert"
)

func setupTest(t *testing.T) (*Table, *transaction.Transaction, func()) {
	// Get PD address from environment or use default
	pdAddr := os.Getenv("TIKV_PD_ADDR")
	if pdAddr == "" {
		pdAddr = "127.0.0.1:2379"
	}

	// Create TiKV client
	client, err := storage.NewTiKVClient(context.Background(), []string{pdAddr})
	if err != nil {
		t.Fatalf("Failed to create TiKV client: %v", err)
	}

	// Create test table
	testTable := NewTable("test_table", []Column{
		{Name: "id", Type: "int", Nullable: false},
		{Name: "name", Type: "string", Nullable: false},
		{Name: "email", Type: "string", Nullable: true},
	}, []string{"id"})

	// Create transaction
	txn, err := transaction.NewTransaction(context.Background(), client.GetClient())
	if err != nil {
		t.Fatalf("Failed to create transaction: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		txn.Rollback()
		client.Close()
	}

	return testTable, txn, cleanup
}

func TestTableInsert(t *testing.T) {
	table, txn, cleanup := setupTest(t)
	defer cleanup()

	// Test successful insert
	row := map[string]interface{}{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
	}

	err := table.Insert(txn, row)
	assert.NoError(t, err)

	// Test duplicate insert
	err = table.Insert(txn, row)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Test insert with missing required field
	invalidRow := map[string]interface{}{
		"id":    2,
		"email": "jane@example.com",
	}
	err = table.Insert(txn, invalidRow)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required column")
}

func TestTableUpdate(t *testing.T) {
	table, txn, cleanup := setupTest(t)
	defer cleanup()

	// Insert initial row
	row := map[string]interface{}{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
	}
	err := table.Insert(txn, row)
	assert.NoError(t, err)

	// Test successful update
	update := map[string]interface{}{
		"id":    1,
		"email": "john.doe@example.com",
	}
	err = table.Update(txn, update)
	assert.NoError(t, err)

	// Verify update
	updated, err := table.Get(txn, map[string]interface{}{"id": 1})
	assert.NoError(t, err)
	assert.Equal(t, "John Doe", updated["name"])
	assert.Equal(t, "john.doe@example.com", updated["email"])

	// Test update non-existent row
	nonExistent := map[string]interface{}{
		"id":    999,
		"name":  "Non Existent",
		"email": "none@example.com",
	}
	err = table.Update(txn, nonExistent)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestTableDelete(t *testing.T) {
	table, txn, cleanup := setupTest(t)
	defer cleanup()

	// Insert test row
	row := map[string]interface{}{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
	}
	err := table.Insert(txn, row)
	assert.NoError(t, err)

	// Test successful delete
	err = table.Delete(txn, map[string]interface{}{"id": 1})
	assert.NoError(t, err)

	// Verify deletion
	_, err = table.Get(txn, map[string]interface{}{"id": 1})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Test delete non-existent row
	err = table.Delete(txn, map[string]interface{}{"id": 999})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestTableGet(t *testing.T) {
	table, txn, cleanup := setupTest(t)
	defer cleanup()

	// Insert test row
	row := map[string]interface{}{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
	}
	err := table.Insert(txn, row)
	assert.NoError(t, err)

	// Test successful get
	retrieved, err := table.Get(txn, map[string]interface{}{"id": 1})
	assert.NoError(t, err)
	assert.Equal(t, row["id"], retrieved["id"])
	assert.Equal(t, row["name"], retrieved["name"])
	assert.Equal(t, row["email"], retrieved["email"])

	// Test get non-existent row
	_, err = table.Get(txn, map[string]interface{}{"id": 999})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Test get with missing primary key
	_, err = table.Get(txn, map[string]interface{}{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "primary key column")
} 