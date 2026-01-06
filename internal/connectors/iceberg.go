/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	msgtypes "github.com/dataflow-operator/dataflow/internal/types"
)

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

// IcebergSourceConnector implements SourceConnector for Iceberg REST API
type IcebergSourceConnector struct {
	config  *v1.IcebergSourceSpec
	catalog catalog.Catalog
	table   *table.Table
	closed  bool
	mu      sync.Mutex
}

// NewIcebergSourceConnector creates a new Iceberg source connector
func NewIcebergSourceConnector(config *v1.IcebergSourceSpec) *IcebergSourceConnector {
	return &IcebergSourceConnector{
		config: config,
	}
}

// Connect establishes connection to Iceberg REST Catalog
func (i *IcebergSourceConnector) Connect(ctx context.Context) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed {
		return fmt.Errorf("connector is closed")
	}

	// Set AWS environment variables for S3 access
	// These are needed when iceberg-go writes data files directly to S3/MinIO
	if os.Getenv("AWS_REGION") == "" {
		os.Setenv("AWS_REGION", "us-east-1")
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		os.Setenv("AWS_ACCESS_KEY_ID", "admin")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", "password")
	}
	// Set S3 endpoint for MinIO (if not already set)
	// AWS SDK v2 uses AWS_ENDPOINT_URL_S3 or custom resolver
	if os.Getenv("AWS_ENDPOINT_URL_S3") == "" {
		// Try to extract endpoint from REST catalog URL or use default MinIO endpoint
		// For local development with docker-compose, MinIO is typically at http://localhost:9000
		// But when running inside container, it should be http://minio:9000
		// We'll use localhost for now, but this should be configurable
		os.Setenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
	}

	// Create REST catalog
	restCatalog, err := rest.NewCatalog(
		ctx,
		"iceberg",
		i.config.RESTCatalogURL,
		rest.WithOAuthToken(i.config.Token),
	)
	if err != nil {
		return fmt.Errorf("failed to create REST catalog: %w", err)
	}

	i.catalog = restCatalog

	// Load table
	tableIdentifier := catalog.ToIdentifier(i.config.Namespace, i.config.Table)
	tbl, err := restCatalog.LoadTable(ctx, tableIdentifier)
	if err != nil {
		return fmt.Errorf("failed to load table %s.%s: %w", i.config.Namespace, i.config.Table, err)
	}

	i.table = tbl
	return nil
}

// Read returns a channel of messages from Iceberg
func (i *IcebergSourceConnector) Read(ctx context.Context) (<-chan *msgtypes.Message, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.table == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}

	msgChan := make(chan *msgtypes.Message, 100)

	go func() {
		defer close(msgChan)

		// Scan table data using ToArrowRecords for streaming
		scan := i.table.Scan()
		schema, recordIter, err := scan.ToArrowRecords(ctx)
		if err != nil {
			fmt.Printf("ERROR: Iceberg - Failed to scan table: %v\n", err)
			return
		}
		_ = schema // Schema is available if needed

		// Iterate through record batches
		for recordBatch, err := range recordIter {
			if err != nil {
				fmt.Printf("ERROR: Iceberg - Failed to read record batch: %v\n", err)
				continue
			}
			defer recordBatch.Release()

			// Process each row in the record batch
			// Convert Arrow RecordBatch to JSON for simplicity
			// In production, you'd want to properly extract values by type
			for rowIdx := 0; rowIdx < int(recordBatch.NumRows()); rowIdx++ {
				rowData := make(map[string]interface{})

				// Extract data from each column
				// Note: This is a simplified implementation
				// For production, you'd want to properly extract values using Arrow type-specific methods
				for colIdx := 0; colIdx < int(recordBatch.NumCols()); colIdx++ {
					col := recordBatch.Column(colIdx)
					fieldName := recordBatch.Schema().Field(colIdx).Name

					if col.IsNull(rowIdx) {
						rowData[fieldName] = nil
						continue
					}

					// Extract value based on Arrow data type
					// This handles the most common types - extend as needed
					switch arr := col.(type) {
					case *array.String:
						rowData[fieldName] = arr.Value(rowIdx)
					case *array.Binary:
						rowData[fieldName] = string(arr.Value(rowIdx))
					case *array.Int64:
						rowData[fieldName] = arr.Value(rowIdx)
					case *array.Int32:
						rowData[fieldName] = arr.Value(rowIdx)
					case *array.Float64:
						rowData[fieldName] = arr.Value(rowIdx)
					case *array.Float32:
						rowData[fieldName] = arr.Value(rowIdx)
					case *array.Boolean:
						rowData[fieldName] = arr.Value(rowIdx)
					default:
						// For unsupported types, convert to string representation
						// In production, you'd want to handle all Arrow types
						rowData[fieldName] = fmt.Sprintf("<unsupported_type:%T>", arr)
					}
				}

				jsonData, err := json.Marshal(rowData)
				if err != nil {
					continue
				}

				msg := msgtypes.NewMessage(jsonData)
				msg.Metadata["namespace"] = i.config.Namespace
				msg.Metadata["table"] = i.config.Table

				select {
				case msgChan <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return msgChan, nil
}

// Close closes the Iceberg connection
func (i *IcebergSourceConnector) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.closed = true
	i.table = nil
	i.catalog = nil
	return nil
}

// IcebergSinkConnector implements SinkConnector for Iceberg REST API
type IcebergSinkConnector struct {
	config  *v1.IcebergSinkSpec
	catalog catalog.Catalog
	table   *table.Table
	closed  bool
	mu      sync.Mutex
	writeMu sync.Mutex // Mutex for synchronizing write operations
}

// NewIcebergSinkConnector creates a new Iceberg sink connector
func NewIcebergSinkConnector(config *v1.IcebergSinkSpec) *IcebergSinkConnector {
	return &IcebergSinkConnector{
		config: config,
	}
}

// Connect establishes connection to Iceberg REST Catalog
func (i *IcebergSinkConnector) Connect(ctx context.Context) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed {
		return fmt.Errorf("connector is closed")
	}

	// Set AWS environment variables for S3 access
	// These are needed when iceberg-go writes data files directly to S3/MinIO
	if os.Getenv("AWS_REGION") == "" {
		os.Setenv("AWS_REGION", "us-east-1")
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		os.Setenv("AWS_ACCESS_KEY_ID", "admin")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", "password")
	}
	// Set S3 endpoint for MinIO (if not already set)
	// AWS SDK v2 uses AWS_ENDPOINT_URL_S3 or custom resolver
	if os.Getenv("AWS_ENDPOINT_URL_S3") == "" {
		// Try to extract endpoint from REST catalog URL or use default MinIO endpoint
		// For local development with docker-compose, MinIO is typically at http://localhost:9000
		// But when running inside container, it should be http://minio:9000
		// We'll use localhost for now, but this should be configurable
		os.Setenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
	}

	// Create REST catalog
	restCatalog, err := rest.NewCatalog(
		ctx,
		"iceberg",
		i.config.RESTCatalogURL,
		rest.WithOAuthToken(i.config.Token),
	)
	if err != nil {
		return fmt.Errorf("failed to create REST catalog: %w", err)
	}

	i.catalog = restCatalog

	// Auto-create namespace if enabled (defaults to true)
	autoCreateNamespace := true
	if i.config.AutoCreateNamespace != nil {
		autoCreateNamespace = *i.config.AutoCreateNamespace
	}
	if autoCreateNamespace {
		if err := i.ensureNamespace(ctx); err != nil {
			return fmt.Errorf("failed to ensure namespace exists: %w", err)
		}
	}

	// Auto-create table if enabled
	if i.config.AutoCreateTable != nil && *i.config.AutoCreateTable {
		if err := i.ensureTable(ctx); err != nil {
			return fmt.Errorf("failed to ensure table exists: %w", err)
		}
	}

	// Load table
	tableIdentifier := catalog.ToIdentifier(i.config.Namespace, i.config.Table)
	tbl, err := restCatalog.LoadTable(ctx, tableIdentifier)
	if err != nil {
		return fmt.Errorf("failed to load table %s.%s: %w", i.config.Namespace, i.config.Table, err)
	}

	i.table = tbl
	return nil
}

// ensureNamespace creates the namespace if it doesn't exist
func (i *IcebergSinkConnector) ensureNamespace(ctx context.Context) error {
	fmt.Printf("INFO: Iceberg - Checking namespace: %s\n", i.config.Namespace)

	// Try to list tables in namespace to check if it exists
	parentIdent := catalog.ToIdentifier(i.config.Namespace)
	iter := i.catalog.ListTables(ctx, parentIdent)

	// If we can iterate, namespace exists
	hasNext := false
	for range iter {
		hasNext = true
		break
	}

	if !hasNext {
		// Try to create namespace
		fmt.Printf("INFO: Iceberg - Creating namespace: %s\n", i.config.Namespace)
		nsIdent := catalog.ToIdentifier(i.config.Namespace)
		err := i.catalog.CreateNamespace(ctx, nsIdent, nil)
		if err != nil {
			// Check if it's an "already exists" error
			if errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
				fmt.Printf("INFO: Iceberg - Namespace %s already exists\n", i.config.Namespace)
				return nil
			}
			fmt.Printf("ERROR: Iceberg - Failed to create namespace: %v\n", err)
			return fmt.Errorf("failed to create namespace: %w", err)
		}
		fmt.Printf("INFO: Iceberg - Successfully created namespace: %s\n", i.config.Namespace)
	} else {
		fmt.Printf("INFO: Iceberg - Namespace %s already exists\n", i.config.Namespace)
	}

	return nil
}

// ensureTable creates the table if it doesn't exist
func (i *IcebergSinkConnector) ensureTable(ctx context.Context) error {
	fmt.Printf("INFO: Iceberg - Checking table: %s.%s\n", i.config.Namespace, i.config.Table)

	tableIdentifier := catalog.ToIdentifier(i.config.Namespace, i.config.Table)

	// Try to load table to check if it exists
	_, err := i.catalog.LoadTable(ctx, tableIdentifier)
	if err == nil {
		fmt.Printf("INFO: Iceberg - Table %s.%s already exists\n", i.config.Namespace, i.config.Table)
		return nil
	}

	// If table exists in catalog but metadata is missing (corrupted state),
	// we should try to drop and recreate it
	if !errors.Is(err, catalog.ErrNoSuchTable) {
		// Check if it's a metadata loading error (table exists but metadata is missing)
		errStr := err.Error()
		if contains(errStr, "Location does not exist") || contains(errStr, "NotFoundException") {
			fmt.Printf("WARNING: Iceberg - Table %s.%s exists but metadata is missing, attempting to drop and recreate\n", i.config.Namespace, i.config.Table)
			// Try to drop the corrupted table
			if dropErr := i.catalog.DropTable(ctx, tableIdentifier); dropErr != nil {
				fmt.Printf("WARNING: Iceberg - Failed to drop corrupted table: %v\n", dropErr)
				// Continue anyway - CreateTable might handle this
			}
		} else {
			fmt.Printf("ERROR: Iceberg - Failed to check table: %v\n", err)
			return fmt.Errorf("failed to check table: %w", err)
		}
	}

	// Create table with flexible schema
	fmt.Printf("INFO: Iceberg - Creating table: %s.%s\n", i.config.Namespace, i.config.Table)

	// Create schema with "data" field as string for auto-created tables
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	// Create partition spec (no partitions)
	partitionSpec := iceberg.NewPartitionSpec()

	// Create table with explicit location based on warehouse path
	// The location will be auto-generated by the catalog, but we can optionally specify it
	// For REST catalog with S3, the location is typically: s3://warehouse/namespace/table
	opts := []catalog.CreateTableOpt{
		catalog.WithPartitionSpec(&partitionSpec),
		// Location will be auto-generated by catalog based on warehouse configuration
	}

	// Create table
	_, err = i.catalog.CreateTable(ctx, tableIdentifier, schema, opts...)
	if err != nil {
		// Check if it's an "already exists" error
		if errors.Is(err, catalog.ErrTableAlreadyExists) {
			fmt.Printf("INFO: Iceberg - Table %s.%s already exists\n", i.config.Namespace, i.config.Table)
			return nil
		}
		fmt.Printf("ERROR: Iceberg - Failed to create table: %v\n", err)
		return fmt.Errorf("failed to create table: %w", err)
	}

	fmt.Printf("INFO: Iceberg - Successfully created table: %s.%s\n", i.config.Namespace, i.config.Table)
	return nil
}

// Write writes messages to Iceberg using iceberg-go library
func (i *IcebergSinkConnector) Write(ctx context.Context, messages <-chan *msgtypes.Message) error {
	i.mu.Lock()
	if i.table == nil {
		i.mu.Unlock()
		return fmt.Errorf("not connected, call Connect first")
	}
	tbl := i.table
	i.mu.Unlock()

	batch := make([]map[string]interface{}, 0, 10)
	batchSize := 10
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				fmt.Printf("INFO: Iceberg - Context done, flushing batch of %d messages\n", len(batch))
				if err := i.writeBatch(ctx, tbl, batch); err != nil {
					fmt.Printf("ERROR: Iceberg - Failed to flush batch on context done: %v\n", err)
					return err
				}
			}
			return ctx.Err()
		case <-ticker.C:
			// Периодическая запись, если есть накопленные данные
			if len(batch) > 0 {
				fmt.Printf("INFO: Iceberg - Periodic flush, writing batch of %d messages\n", len(batch))
				if err := i.writeBatch(ctx, tbl, batch); err != nil {
					fmt.Printf("ERROR: Iceberg - Failed to write batch on ticker: %v\n", err)
					fmt.Printf("ERROR: Iceberg - Batch data sample: %+v\n", batch[0])
					// Не очищаем batch при ошибке, чтобы можно было повторить попытку
					// Но ограничиваем размер batch, чтобы не накапливать слишком много
					if len(batch) > 100 {
						fmt.Printf("WARNING: Iceberg - Batch too large (%d), clearing to prevent memory issues\n", len(batch))
						batch = batch[:0]
					}
				} else {
					fmt.Printf("INFO: Iceberg - Successfully wrote batch of %d messages\n", len(batch))
					batch = batch[:0]
				}
			}
		case msg, ok := <-messages:
			if !ok {
				// Канал закрыт, записываем оставшиеся данные
				if len(batch) > 0 {
					fmt.Printf("INFO: Iceberg - Channel closed, flushing batch of %d messages\n", len(batch))
					if err := i.writeBatch(ctx, tbl, batch); err != nil {
						fmt.Printf("ERROR: Iceberg - Failed to flush batch on channel close: %v\n", err)
						return err
					}
					fmt.Printf("INFO: Iceberg - Successfully wrote final batch of %d messages\n", len(batch))
				}
				return nil
			}

			var data map[string]interface{}
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				fmt.Printf("ERROR: Iceberg - Failed to unmarshal message: %v, data: %s\n", err, string(msg.Data))
				continue
			}

			// For auto-created tables with "data" field, wrap in data
			// For existing tables, use data as-is
			if i.config.AutoCreateTable != nil && *i.config.AutoCreateTable {
				// Store as JSON string in "data" field for auto-created tables
				jsonData, _ := json.Marshal(data)
				wrappedData := map[string]interface{}{
					"data": string(jsonData),
				}
				batch = append(batch, wrappedData)
			} else {
				batch = append(batch, data)
			}

			// Записываем при достижении размера батча
			if len(batch) >= batchSize {
				fmt.Printf("INFO: Iceberg - Batch size reached (%d), writing to Iceberg\n", len(batch))
				if err := i.writeBatch(ctx, tbl, batch); err != nil {
					fmt.Printf("ERROR: Iceberg - Failed to write batch: %v\n", err)
					fmt.Printf("ERROR: Iceberg - Batch data sample: %+v\n", batch[0])
					// Не возвращаем ошибку сразу, продолжаем накапливать
					// Но ограничиваем размер batch, чтобы не накапливать слишком много
					if len(batch) > 100 {
						fmt.Printf("WARNING: Iceberg - Batch too large (%d), clearing to prevent memory issues\n", len(batch))
						batch = batch[:0]
					}
				} else {
					fmt.Printf("INFO: Iceberg - Successfully wrote batch of %d messages\n", len(batch))
					batch = batch[:0]
				}
			}
		}
	}
}

func (i *IcebergSinkConnector) writeBatch(ctx context.Context, tbl *table.Table, batch []map[string]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	// Synchronize write operations to prevent concurrent commits
	i.writeMu.Lock()
	defer i.writeMu.Unlock()

	fmt.Printf("INFO: Iceberg - Writing batch of %d messages\n", len(batch))

	// Reload table to get the latest metadata before writing
	// This prevents "branch main was created concurrently" errors
	tableIdentifier := catalog.ToIdentifier(i.config.Namespace, i.config.Table)
	latestTable, err := i.catalog.LoadTable(ctx, tableIdentifier)
	if err != nil {
		return fmt.Errorf("failed to reload table before write: %w", err)
	}

	// Get table schema
	icebergSchema := latestTable.Schema()

	// Log schema for debugging
	if icebergSchema != nil {
		icebergFields := icebergSchema.Fields()
		fmt.Printf("INFO: Iceberg - Table schema has %d fields\n", len(icebergFields))
		for _, field := range icebergFields {
			fmt.Printf("INFO: Iceberg - Field: %s, Type: %v, Required: %v\n", field.Name, field.Type, field.Required)
		}
	}

	// Convert batch to Arrow RecordBatch
	recordBatch, err := i.convertBatchToArrow(batch, icebergSchema)
	if err != nil {
		return fmt.Errorf("failed to convert batch to Arrow: %w", err)
	}
	defer recordBatch.Release()

	// Use Append to write data with retry logic for concurrent commit conflicts
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Create RecordReader from RecordBatch for each attempt
		// Reader cannot be reused after Append, so we create a new one each time
		reader, err := array.NewRecordReader(recordBatch.Schema(), []arrow.RecordBatch{recordBatch})
		if err != nil {
			return fmt.Errorf("failed to create record reader: %w", err)
		}

		_, err = latestTable.Append(ctx, reader, nil)
		reader.Release() // Release reader after use

		if err == nil {
			// Success - update cached table reference
			i.mu.Lock()
			i.table = latestTable
			i.mu.Unlock()
			fmt.Printf("INFO: Iceberg - Successfully wrote batch of %d messages\n", len(batch))
			return nil
		}

		// Логируем детали ошибки
		errStr := err.Error()
		fmt.Printf("ERROR: Iceberg - Append failed (attempt %d/%d): %s\n", attempt+1, maxRetries, errStr)

		// Check if it's a concurrent commit error
		if strings.Contains(errStr, "concurrently") || strings.Contains(errStr, "CommitFailedException") {
			if attempt < maxRetries-1 {
				// Wait a bit and reload table for retry
				waitTime := time.Duration(attempt+1) * 100 * time.Millisecond
				fmt.Printf("WARNING: Iceberg - Concurrent commit conflict, waiting %v and retrying (attempt %d/%d)\n", waitTime, attempt+1, maxRetries)
				time.Sleep(waitTime)
				latestTable, err = i.catalog.LoadTable(ctx, tableIdentifier)
				if err != nil {
					return fmt.Errorf("failed to reload table for retry: %w", err)
				}
				continue
			}
		}

		// Проверяем на ошибки подключения к S3/MinIO
		if strings.Contains(errStr, "Unknown failure") || strings.Contains(errStr, "UncheckedSQLException") {
			fmt.Printf("ERROR: Iceberg - Possible S3/MinIO connection issue. Check AWS_ENDPOINT_URL_S3: %s\n", os.Getenv("AWS_ENDPOINT_URL_S3"))
			fmt.Printf("ERROR: Iceberg - AWS credentials: ACCESS_KEY_ID=%s, REGION=%s\n",
				os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_REGION"))
			// Для таких ошибок тоже делаем retry
			if attempt < maxRetries-1 {
				waitTime := time.Duration(attempt+1) * 200 * time.Millisecond
				fmt.Printf("WARNING: Iceberg - Retrying after %v (attempt %d/%d)\n", waitTime, attempt+1, maxRetries)
				time.Sleep(waitTime)
				latestTable, err = i.catalog.LoadTable(ctx, tableIdentifier)
				if err != nil {
					return fmt.Errorf("failed to reload table for retry: %w", err)
				}
				continue
			}
		}

		// Not a retryable error or max retries reached
		return fmt.Errorf("failed to append data to table: %w", err)
	}

	return fmt.Errorf("failed to append data to table after %d retries", maxRetries)
}

// convertBatchToArrow converts a batch of maps to an Arrow RecordBatch
func (i *IcebergSinkConnector) convertBatchToArrow(batch []map[string]interface{}, icebergSchema *iceberg.Schema) (arrow.Record, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("batch is empty")
	}

	// Get the first row to determine fields
	firstRow := batch[0]
	numRows := int64(len(batch))

	// Build Arrow schema based on Iceberg table schema
	// This ensures we match the table schema exactly
	fields := make([]arrow.Field, 0)
	builders := make([]array.Builder, 0)
	mem := memory.DefaultAllocator
	fieldNames := make([]string, 0)

	// Use Iceberg schema to determine field order and types
	// For auto-created tables, we know the schema has only "data" field of type String
	// For existing tables, we need to match the schema exactly
	if icebergSchema != nil {
		icebergFields := icebergSchema.Fields()
		if len(icebergFields) > 0 {
			// Build schema based on Iceberg table schema
			for _, icebergField := range icebergFields {
				fieldName := icebergField.Name
				fieldNames = append(fieldNames, fieldName)

				// Map Iceberg types to Arrow types
				// For auto-created tables, we use String type for the "data" field
				var arrowType arrow.DataType
				// Compare types directly
				if icebergField.Type == iceberg.PrimitiveTypes.String {
					arrowType = arrow.BinaryTypes.String
				} else if icebergField.Type == iceberg.PrimitiveTypes.Int64 {
					arrowType = arrow.PrimitiveTypes.Int64
				} else if icebergField.Type == iceberg.PrimitiveTypes.Int32 {
					arrowType = arrow.PrimitiveTypes.Int32
				} else if icebergField.Type == iceberg.PrimitiveTypes.Float64 {
					arrowType = arrow.PrimitiveTypes.Float64
				} else if icebergField.Type == iceberg.PrimitiveTypes.Float32 {
					arrowType = arrow.PrimitiveTypes.Float32
				} else if icebergField.Type == iceberg.PrimitiveTypes.Bool {
					arrowType = arrow.FixedWidthTypes.Boolean
				} else {
					// Default to string for unknown types
					arrowType = arrow.BinaryTypes.String
				}

				field := arrow.Field{
					Name:     fieldName,
					Type:     arrowType,
					Nullable: !icebergField.Required,
				}
				fields = append(fields, field)

				// Create appropriate builder
				switch arrowType {
				case arrow.BinaryTypes.String:
					builders = append(builders, array.NewStringBuilder(mem))
				case arrow.PrimitiveTypes.Int64:
					builders = append(builders, array.NewInt64Builder(mem))
				case arrow.PrimitiveTypes.Int32:
					builders = append(builders, array.NewInt32Builder(mem))
				case arrow.PrimitiveTypes.Float64:
					builders = append(builders, array.NewFloat64Builder(mem))
				case arrow.PrimitiveTypes.Float32:
					builders = append(builders, array.NewFloat32Builder(mem))
				case arrow.FixedWidthTypes.Boolean:
					builders = append(builders, array.NewBooleanBuilder(mem))
				default:
					builders = append(builders, array.NewStringBuilder(mem))
				}
			}
		} else {
			// Fallback: use data fields if schema is empty
			fieldMap := make(map[string]bool)
			for fieldName := range firstRow {
				if !fieldMap[fieldName] {
					fieldNames = append(fieldNames, fieldName)
					fieldMap[fieldName] = true
				}
			}

			for _, fieldName := range fieldNames {
				field := arrow.Field{Name: fieldName, Type: arrow.BinaryTypes.String, Nullable: true}
				fields = append(fields, field)
				builders = append(builders, array.NewStringBuilder(mem))
			}
		}
	} else {
		// Fallback: use data fields if schema is not available
		fieldMap := make(map[string]bool)
		for fieldName := range firstRow {
			if !fieldMap[fieldName] {
				fieldNames = append(fieldNames, fieldName)
				fieldMap[fieldName] = true
			}
		}

		for _, fieldName := range fieldNames {
			field := arrow.Field{Name: fieldName, Type: arrow.BinaryTypes.String, Nullable: true}
			fields = append(fields, field)
			builders = append(builders, array.NewStringBuilder(mem))
		}
	}

	schema := arrow.NewSchema(fields, nil)

	// Build arrays from data
	for rowIdx := 0; rowIdx < len(batch); rowIdx++ {
		row := batch[rowIdx]
		for colIdx, fieldName := range fieldNames {
			val, exists := row[fieldName]
			field := fields[colIdx]

			if !exists || val == nil {
				// Append null based on field type
				switch field.Type {
				case arrow.BinaryTypes.String:
					builders[colIdx].(*array.StringBuilder).AppendNull()
				case arrow.PrimitiveTypes.Int64:
					builders[colIdx].(*array.Int64Builder).AppendNull()
				case arrow.PrimitiveTypes.Int32:
					builders[colIdx].(*array.Int32Builder).AppendNull()
				case arrow.PrimitiveTypes.Float64:
					builders[colIdx].(*array.Float64Builder).AppendNull()
				case arrow.PrimitiveTypes.Float32:
					builders[colIdx].(*array.Float32Builder).AppendNull()
				case arrow.FixedWidthTypes.Boolean:
					builders[colIdx].(*array.BooleanBuilder).AppendNull()
				default:
					builders[colIdx].(*array.StringBuilder).AppendNull()
				}
			} else {
				// Append value based on field type
				switch field.Type {
				case arrow.BinaryTypes.String:
					var strVal string
					switch v := val.(type) {
					case string:
						strVal = v
					case []byte:
						strVal = string(v)
					default:
						// Convert to JSON string for complex types
						if jsonBytes, err := json.Marshal(v); err == nil {
							strVal = string(jsonBytes)
						} else {
							strVal = fmt.Sprintf("%v", v)
						}
					}
					builders[colIdx].(*array.StringBuilder).Append(strVal)
				case arrow.PrimitiveTypes.Int64:
					var intVal int64
					switch v := val.(type) {
					case int64:
						intVal = v
					case int:
						intVal = int64(v)
					case float64:
						intVal = int64(v)
					default:
						intVal = 0
					}
					builders[colIdx].(*array.Int64Builder).Append(intVal)
				case arrow.PrimitiveTypes.Int32:
					var intVal int32
					switch v := val.(type) {
					case int32:
						intVal = v
					case int:
						intVal = int32(v)
					case float64:
						intVal = int32(v)
					default:
						intVal = 0
					}
					builders[colIdx].(*array.Int32Builder).Append(intVal)
				case arrow.PrimitiveTypes.Float64:
					var floatVal float64
					switch v := val.(type) {
					case float64:
						floatVal = v
					case float32:
						floatVal = float64(v)
					case int:
						floatVal = float64(v)
					default:
						floatVal = 0.0
					}
					builders[colIdx].(*array.Float64Builder).Append(floatVal)
				case arrow.PrimitiveTypes.Float32:
					var floatVal float32
					switch v := val.(type) {
					case float32:
						floatVal = v
					case float64:
						floatVal = float32(v)
					case int:
						floatVal = float32(v)
					default:
						floatVal = 0.0
					}
					builders[colIdx].(*array.Float32Builder).Append(floatVal)
				case arrow.FixedWidthTypes.Boolean:
					var boolVal bool
					switch v := val.(type) {
					case bool:
						boolVal = v
					default:
						boolVal = false
					}
					builders[colIdx].(*array.BooleanBuilder).Append(boolVal)
				default:
					// Fallback to string
					var strVal string
					switch v := val.(type) {
					case string:
						strVal = v
					case []byte:
						strVal = string(v)
					default:
						if jsonBytes, err := json.Marshal(v); err == nil {
							strVal = string(jsonBytes)
						} else {
							strVal = fmt.Sprintf("%v", v)
						}
					}
					builders[colIdx].(*array.StringBuilder).Append(strVal)
				}
			}
		}
	}

	// Build arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	// Create RecordBatch
	recordBatch := array.NewRecord(schema, arrays, numRows)
	return recordBatch, nil
}

// Close closes the Iceberg connection
func (i *IcebergSinkConnector) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.closed = true
	i.table = nil
	i.catalog = nil
	return nil
}
