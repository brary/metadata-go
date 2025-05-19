package table

import (
	"encoding/json"
	"fmt"
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