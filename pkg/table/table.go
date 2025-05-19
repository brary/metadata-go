package table

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/avneetkang/metadata-go/pkg/transaction"
)

// Column represents a table column definition
type Column struct {
	Name     string
	Type     string
	Nullable bool
	Default  interface{}
}

// Table represents a metadata table
type Table struct {
	Name    string
	Columns []Column
	Primary []string
}

// NewTable creates a new table definition
func NewTable(name string, columns []Column, primary []string) *Table {
	return &Table{
		Name:    name,
		Columns: columns,
		Primary: primary,
	}
}

// ValidateRow checks if a row matches the table schema
func (t *Table) ValidateRow(row map[string]interface{}) error {
	// Check if all required columns are present
	for _, col := range t.Columns {
		if !col.Nullable {
			if _, exists := row[col.Name]; !exists {
				return fmt.Errorf("required column %s is missing", col.Name)
			}
		}
	}
	return nil
}

// SerializeRow converts a row to bytes for storage
func (t *Table) SerializeRow(row map[string]interface{}) ([]byte, error) {
	return json.Marshal(row)
}

// DeserializeRow converts stored bytes back to a row
func (t *Table) DeserializeRow(data []byte) (map[string]interface{}, error) {
	var row map[string]interface{}
	err := json.Unmarshal(data, &row)
	return row, err
}

// generateKey creates a key for storing a row
func (t *Table) generateKey(row map[string]interface{}) ([]byte, error) {
	// Create a key using table name and primary key values
	keyParts := []string{t.Name}
	
	for _, pk := range t.Primary {
		val, exists := row[pk]
		if !exists {
			return nil, fmt.Errorf("primary key column %s is missing", pk)
		}
		keyParts = append(keyParts, fmt.Sprintf("%v", val))
	}
	
	return []byte(strings.Join(keyParts, ":")), nil
}

// Insert adds a new row to the table
func (t *Table) Insert(txn *transaction.Transaction, row map[string]interface{}) error {
	// Validate the row
	if err := t.ValidateRow(row); err != nil {
		return fmt.Errorf("invalid row: %v", err)
	}

	// Generate the key
	key, err := t.generateKey(row)
	if err != nil {
		return fmt.Errorf("failed to generate key: %v", err)
	}

	// Check if row already exists
	existing, err := txn.Get(key)
	if err == nil && existing != nil {
		return fmt.Errorf("row with primary key already exists")
	}

	// Serialize the row
	value, err := t.SerializeRow(row)
	if err != nil {
		return fmt.Errorf("failed to serialize row: %v", err)
	}

	// Store the row
	return txn.Set(key, value)
}

// Update modifies an existing row in the table
func (t *Table) Update(txn *transaction.Transaction, row map[string]interface{}) error {
	// Validate the row
	if err := t.ValidateRow(row); err != nil {
		return fmt.Errorf("invalid row: %v", err)
	}

	// Generate the key
	key, err := t.generateKey(row)
	if err != nil {
		return fmt.Errorf("failed to generate key: %v", err)
	}

	// Check if row exists
	existing, err := txn.Get(key)
	if err != nil {
		return fmt.Errorf("failed to check existing row: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("row does not exist")
	}

	// Deserialize existing row
	existingRow, err := t.DeserializeRow(existing)
	if err != nil {
		return fmt.Errorf("failed to deserialize existing row: %v", err)
	}

	// Update the row
	for k, v := range row {
		existingRow[k] = v
	}

	// Serialize the updated row
	value, err := t.SerializeRow(existingRow)
	if err != nil {
		return fmt.Errorf("failed to serialize updated row: %v", err)
	}

	// Store the updated row
	return txn.Set(key, value)
}

// Delete removes a row from the table
func (t *Table) Delete(txn *transaction.Transaction, primaryKeyValues map[string]interface{}) error {
	// Verify all primary key columns are provided
	for _, pk := range t.Primary {
		if _, exists := primaryKeyValues[pk]; !exists {
			return fmt.Errorf("primary key column %s is missing", pk)
		}
	}

	// Generate the key
	key, err := t.generateKey(primaryKeyValues)
	if err != nil {
		return fmt.Errorf("failed to generate key: %v", err)
	}

	// Check if row exists
	existing, err := txn.Get(key)
	if err != nil {
		return fmt.Errorf("failed to check existing row: %v", err)
	}
	if existing == nil {
		return fmt.Errorf("row does not exist")
	}

	// Delete the row
	return txn.Delete(key)
}

// Get retrieves a row from the table
func (t *Table) Get(txn *transaction.Transaction, primaryKeyValues map[string]interface{}) (map[string]interface{}, error) {
	// Verify all primary key columns are provided
	for _, pk := range t.Primary {
		if _, exists := primaryKeyValues[pk]; !exists {
			return nil, fmt.Errorf("primary key column %s is missing", pk)
		}
	}

	// Generate the key
	key, err := t.generateKey(primaryKeyValues)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %v", err)
	}

	// Get the row
	data, err := txn.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get row: %v", err)
	}
	if data == nil {
		return nil, fmt.Errorf("row does not exist")
	}

	// Deserialize the row
	return t.DeserializeRow(data)
} 