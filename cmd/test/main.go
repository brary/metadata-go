package main

import (
	"context"
	"fmt"
	"log"

	"github.com/avneetkang/metadata-go/pkg/metadata"
	"github.com/avneetkang/metadata-go/pkg/table"
)

func main() {
	// Create a context
	ctx := context.Background()

	// Connect to TiKV (assuming local development setup)
	db, err := metadata.NewDB(ctx, []string{"127.0.0.1:2379"})
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Define table schema
	columns := []table.Column{
		{Name: "id", Type: "string", Nullable: false},
		{Name: "name", Type: "string", Nullable: false},
		{Name: "age", Type: "int", Nullable: true},
	}

	// Create a test table
	err = db.CreateTable("users", columns, []string{"id"})
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("Table 'users' created successfully")

	// Get the table
	tbl, err := db.GetTable("users")
	if err != nil {
		log.Fatalf("Failed to get table: %v", err)
	}

	// Start a transaction
	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a test row
	row := map[string]interface{}{
		"id":   "user1",
		"name": "John Doe",
		"age":  30,
	}

	// Validate the row
	if err := tbl.ValidateRow(row); err != nil {
		log.Fatalf("Row validation failed: %v", err)
	}

	// Serialize the row
	data, err := tbl.SerializeRow(row)
	if err != nil {
		log.Fatalf("Failed to serialize row: %v", err)
	}

	// Store the row
	key := []byte(fmt.Sprintf("users:%s", row["id"]))
	err = txn.Set(key, data)
	if err != nil {
		log.Fatalf("Failed to store row: %v", err)
	}

	// Commit the transaction
	err = txn.Commit()
	if err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
	fmt.Println("Successfully stored user data")

	// Start a new transaction to read the data
	txn, err = db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin read transaction: %v", err)
	}

	// Read the data
	data, err = txn.Get(key)
	if err != nil {
		log.Fatalf("Failed to read data: %v", err)
	}

	// Deserialize the row
	readRow, err := tbl.DeserializeRow(data)
	if err != nil {
		log.Fatalf("Failed to deserialize row: %v", err)
	}

	fmt.Printf("Read user data: %+v\n", readRow)

	// Commit the read transaction
	err = txn.Commit()
	if err != nil {
		log.Fatalf("Failed to commit read transaction: %v", err)
	}
} 