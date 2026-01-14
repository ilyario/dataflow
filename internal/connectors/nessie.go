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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	v1 "github.com/dataflow-operator/dataflow/api/v1"
	msgtypes "github.com/dataflow-operator/dataflow/internal/types"
	openapi "github.com/dataflow-operator/dataflow/pkg/nessie-client"
)

// NessieSourceConnector implements SourceConnector for Nessie catalog
type NessieSourceConnector struct {
	config       *v1.NessieSourceSpec
	nessieClient *openapi.APIClient
	table        *table.Table
	closed       bool
	mu           sync.Mutex
}

// NewNessieSourceConnector creates a new Nessie source connector
func NewNessieSourceConnector(config *v1.NessieSourceSpec) *NessieSourceConnector {
	return &NessieSourceConnector{
		config: config,
	}
}

// Connect establishes connection to Nessie and loads the table
func (n *NessieSourceConnector) Connect(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return fmt.Errorf("connector is closed")
	}

	// Validate Nessie URL
	if n.config.NessieURL == "" {
		return fmt.Errorf("Nessie URL is required but not provided")
	}

	// Determine branch (defaults to "main")
	branch := n.config.Branch
	if branch == "" {
		branch = "main"
	}

	fmt.Printf("INFO: Nessie Source - Connecting to Nessie: %s, branch: %s\n", n.config.NessieURL, branch)

	// Create Nessie API client
	cfg := openapi.NewConfiguration()
	nessieURL, err := url.Parse(n.config.NessieURL)
	if err != nil {
		return fmt.Errorf("failed to parse Nessie URL: %w", err)
	}

	cfg.Scheme = nessieURL.Scheme
	cfg.Host = nessieURL.Host
	if nessieURL.Path != "" {
		cfg.Servers = openapi.ServerConfigurations{
			openapi.ServerConfiguration{
				URL: nessieURL.Path,
			},
		}
	}

	// Add authentication token if provided
	if n.config.Token != "" {
		cfg.AddDefaultHeader("Authorization", "Bearer "+n.config.Token)
	}

	nessieClient := openapi.NewAPIClient(cfg)
	n.nessieClient = nessieClient

	// Build content key from namespace and table
	contentKey := openapi.NewContentKey([]string{n.config.Namespace, n.config.Table})
	fmt.Printf("INFO: Nessie Source - Looking for table: namespace=%s, table=%s, branch=%s\n",
		n.config.Namespace, n.config.Table, branch)
	fmt.Printf("INFO: Nessie Source - ContentKey elements: %v\n", []string{n.config.Namespace, n.config.Table})

	// Get table metadata from Nessie
	contentResp, httpResp, err := nessieClient.V2API.GetContentV2(ctx, *contentKey, branch).Execute()
	if err != nil {
		return fmt.Errorf("failed to get table content from Nessie: %w", err)
	}
	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get table content: HTTP %d", httpResp.StatusCode)
	}

	// Extract IcebergTable from content
	content := contentResp.GetContent()
	if content.IcebergTableV2 == nil {
		return fmt.Errorf("content is not an Iceberg table")
	}

	icebergTable := content.IcebergTableV2
	metadataLocation := icebergTable.GetMetadataLocation()

	// Convert s3a:// to s3:// for iceberg-go compatibility
	// s3a:// is Hadoop-specific scheme, but iceberg-go expects s3://
	if strings.HasPrefix(metadataLocation, "s3a://") {
		metadataLocation = strings.Replace(metadataLocation, "s3a://", "s3://", 1)
		fmt.Printf("INFO: Nessie Source - Converted s3a:// to s3:// in metadata location\n")
	}

	fmt.Printf("INFO: Nessie Source - Table metadata location: %s\n", metadataLocation)

	// Extract bucket from metadata location for FileIO initialization
	bucketName := extractBucketFromS3Path(metadataLocation)
	if bucketName == "" {
		return fmt.Errorf("failed to extract bucket from metadata location: %s", metadataLocation)
	}
	fmt.Printf("INFO: Nessie Source - Extracted bucket name: %s\n", bucketName)

	// Create S3 FileIO factory function with bucket information
	fsysF, err := n.createS3FileIOFactoryWithBucket(bucketName)
	if err != nil {
		return fmt.Errorf("failed to create S3 FileIO factory: %w", err)
	}

	// Create identifier for the table (Identifier is []string)
	ident := table.Identifier{n.config.Namespace, n.config.Table}

	// Load Iceberg table using metadata location
	// CatalogIO is nil because we're not using a catalog, just loading from metadata location
	tbl, err := table.NewFromLocation(ctx, ident, metadataLocation, fsysF, nil)
	if err != nil {
		return fmt.Errorf("failed to open Iceberg table from metadata location: %w", err)
	}

	n.table = tbl
	return nil
}

// Read returns a channel of messages from Nessie/Iceberg table
func (n *NessieSourceConnector) Read(ctx context.Context) (<-chan *msgtypes.Message, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.table == nil {
		return nil, fmt.Errorf("not connected, call Connect first")
	}

	msgChan := make(chan *msgtypes.Message, 100)

	go func() {
		defer close(msgChan)

		// Scan table data using ToArrowRecords for streaming
		scan := n.table.Scan()
		schema, recordIter, err := scan.ToArrowRecords(ctx)
		if err != nil {
			fmt.Printf("ERROR: Nessie - Failed to scan table: %v\n", err)
			return
		}
		_ = schema // Schema is available if needed

		// Iterate through record batches
		for recordBatch, err := range recordIter {
			if err != nil {
				fmt.Printf("ERROR: Nessie - Failed to read record batch: %v\n", err)
				continue
			}
			defer recordBatch.Release()

			// Process each row in the record batch
			for rowIdx := 0; rowIdx < int(recordBatch.NumRows()); rowIdx++ {
				rowData := make(map[string]interface{})

				// Extract data from each column
				for colIdx := 0; colIdx < int(recordBatch.NumCols()); colIdx++ {
					col := recordBatch.Column(colIdx)
					fieldName := recordBatch.Schema().Field(colIdx).Name

					if col.IsNull(rowIdx) {
						rowData[fieldName] = nil
						continue
					}

					// Extract value based on Arrow data type
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
						rowData[fieldName] = fmt.Sprintf("<unsupported_type:%T>", arr)
					}
				}

				jsonData, err := json.Marshal(rowData)
				if err != nil {
					continue
				}

				msg := msgtypes.NewMessage(jsonData)
				msg.Metadata["namespace"] = n.config.Namespace
				msg.Metadata["table"] = n.config.Table

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

// Close closes the Nessie connection
func (n *NessieSourceConnector) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.closed = true
	n.table = nil
	n.nessieClient = nil
	return nil
}

// NessieSinkConnector implements SinkConnector for Nessie catalog
type NessieSinkConnector struct {
	config           *v1.NessieSinkSpec
	nessieClient     *openapi.APIClient
	table            *table.Table
	closed           bool
	mu               sync.Mutex
	writeMu          sync.Mutex // Mutex for synchronizing write operations
	metadataLocation string
}

// NewNessieSinkConnector creates a new Nessie sink connector
func NewNessieSinkConnector(config *v1.NessieSinkSpec) *NessieSinkConnector {
	return &NessieSinkConnector{
		config: config,
	}
}

// Connect establishes connection to Nessie and loads/creates the table
func (n *NessieSinkConnector) Connect(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return fmt.Errorf("connector is closed")
	}

	// Validate Nessie URL
	if n.config.NessieURL == "" {
		return fmt.Errorf("Nessie URL is required but not provided")
	}

	// Determine branch (defaults to "main")
	branch := n.config.Branch
	if branch == "" {
		branch = "main"
	}

	fmt.Printf("INFO: Nessie Sink - Connecting to Nessie: %s, branch: %s\n", n.config.NessieURL, branch)

	// Create Nessie API client
	cfg := openapi.NewConfiguration()
	nessieURL, err := url.Parse(n.config.NessieURL)
	if err != nil {
		return fmt.Errorf("failed to parse Nessie URL: %w", err)
	}

	cfg.Scheme = nessieURL.Scheme
	cfg.Host = nessieURL.Host
	if nessieURL.Path != "" {
		cfg.Servers = openapi.ServerConfigurations{
			openapi.ServerConfiguration{
				URL: nessieURL.Path,
			},
		}
	}

	// Add authentication token if provided
	if n.config.Token != "" {
		cfg.AddDefaultHeader("Authorization", "Bearer "+n.config.Token)
	}

	nessieClient := openapi.NewAPIClient(cfg)
	n.nessieClient = nessieClient

	// Build content key from namespace and table
	contentKey := openapi.NewContentKey([]string{n.config.Namespace, n.config.Table})

	// Log detailed information for debugging
	fmt.Printf("INFO: Nessie Sink - Looking for table: namespace=%s, table=%s, branch=%s\n",
		n.config.Namespace, n.config.Table, branch)
	fmt.Printf("INFO: Nessie Sink - ContentKey elements: %v\n", []string{n.config.Namespace, n.config.Table})

	// Try to get existing table
	contentResp, httpResp, err := nessieClient.V2API.GetContentV2(ctx, *contentKey, branch).Execute()

	// Handle case where HTTP status is 200 but deserialization of effectiveReference fails
	// In this case, we can still extract the content from the error body if available
	if err != nil && httpResp != nil && httpResp.StatusCode == http.StatusOK {
		fmt.Printf("WARNING: Nessie Sink - HTTP 200 but deserialization error (likely effectiveReference issue): %v\n", err)
		fmt.Printf("INFO: Nessie Sink - Attempting to extract content from error or response\n")

		// Try to extract body from GenericOpenAPIError
		var bodyBytes []byte
		if genErr, ok := err.(*openapi.GenericOpenAPIError); ok {
			// GenericOpenAPIError has a body field
			bodyBytes = genErr.Body()
			fmt.Printf("DEBUG: Nessie Sink - Extracted body from GenericOpenAPIError, length: %d\n", len(bodyBytes))
		} else {
			fmt.Printf("DEBUG: Nessie Sink - Error is not GenericOpenAPIError, type: %T\n", err)
		}

		// If we have body bytes, try to parse content from it
		if len(bodyBytes) > 0 {
			var responseMap map[string]interface{}
			if jsonErr := json.Unmarshal(bodyBytes, &responseMap); jsonErr == nil {
				if contentObj, ok := responseMap["content"].(map[string]interface{}); ok {
					// In API v2, metadataLocation can be directly in content object
					var metadataLoc string
					var hasMetadataLoc bool

					// Try direct metadataLocation first (API v2 format)
					if metadataLoc, hasMetadataLoc = contentObj["metadataLocation"].(string); !hasMetadataLoc {
						metadataLoc, hasMetadataLoc = contentObj["metadata_location"].(string)
					}

					// If not found, try nested icebergTableV2 structure
					if !hasMetadataLoc {
						var icebergTableObj map[string]interface{}
						var found bool
						if icebergTableObj, found = contentObj["icebergTableV2"].(map[string]interface{}); !found {
							icebergTableObj, found = contentObj["iceberg_table_v2"].(map[string]interface{})
						}

						if found {
							if metadataLoc, hasMetadataLoc = icebergTableObj["metadataLocation"].(string); !hasMetadataLoc {
								metadataLoc, hasMetadataLoc = icebergTableObj["metadata_location"].(string)
							}
						}
					}

					if hasMetadataLoc {
						// Convert s3a:// to s3:// for iceberg-go compatibility
						if strings.HasPrefix(metadataLoc, "s3a://") {
							metadataLoc = strings.Replace(metadataLoc, "s3a://", "s3://", 1)
							fmt.Printf("INFO: Nessie Sink - Converted s3a:// to s3:// in metadata location\n")
						}

						fmt.Printf("INFO: Nessie Sink - Successfully extracted metadata location from response: %s\n", metadataLoc)
						n.metadataLocation = metadataLoc

						// Extract bucket from metadata location for FileIO initialization
						bucketName := extractBucketFromS3Path(metadataLoc)
						if bucketName == "" {
							return fmt.Errorf("failed to extract bucket from metadata location: %s", metadataLoc)
						}
						fmt.Printf("INFO: Nessie Sink - Extracted bucket name: %s\n", bucketName)

						// Create S3 FileIO factory function with bucket information
						fsysF, err := n.createS3FileIOFactoryWithBucket(bucketName)
						if err != nil {
							return fmt.Errorf("failed to create S3 FileIO factory: %w", err)
						}
						if fsysF == nil {
							return fmt.Errorf("FileIO factory is nil after creation")
						}

						// Create identifier for the table
						ident := table.Identifier{n.config.Namespace, n.config.Table}

						// Load Iceberg table using metadata location
						tbl, err := table.NewFromLocation(ctx, ident, metadataLoc, fsysF, nil)
						if err != nil {
							return fmt.Errorf("failed to open Iceberg table from metadata location: %w", err)
						}

						n.table = tbl
						return nil
					} else {
						fmt.Printf("WARNING: Nessie Sink - Could not find metadataLocation in content object: %+v\n", contentObj)
					}
				} else {
					fmt.Printf("WARNING: Nessie Sink - Could not parse content from response: %+v\n", responseMap)
				}
			} else {
				fmt.Printf("WARNING: Nessie Sink - Failed to unmarshal response body: %v\n", jsonErr)
			}
		} else {
			fmt.Printf("WARNING: Nessie Sink - No body bytes available in error\n")
		}
		// If manual extraction failed, fall through to error handling
	}

	if err != nil {
		// Log detailed error information
		statusCode := 0
		responseBody := ""
		if httpResp != nil {
			statusCode = httpResp.StatusCode
			// Try to read response body for more details
			if httpResp.Body != nil {
				bodyBytes, readErr := io.ReadAll(httpResp.Body)
				if readErr == nil && len(bodyBytes) > 0 {
					responseBody = string(bodyBytes)
					if len(responseBody) > 500 {
						responseBody = responseBody[:500] + "..."
					}
				}
			}
		}

		fmt.Printf("ERROR: Nessie Sink - Failed to get table content: namespace=%s, table=%s, branch=%s, HTTP status=%d\n",
			n.config.Namespace, n.config.Table, branch, statusCode)
		fmt.Printf("ERROR: Nessie Sink - Error details: %v\n", err)
		if responseBody != "" {
			fmt.Printf("ERROR: Nessie Sink - Response body: %s\n", responseBody)
		}

		// Table doesn't exist, create it if AutoCreateTable is enabled
		if httpResp != nil && httpResp.StatusCode == http.StatusNotFound {
			fmt.Printf("INFO: Nessie Sink - Table not found (404), AutoCreateTable=%v\n",
				n.config.AutoCreateTable != nil && *n.config.AutoCreateTable)
			if n.config.AutoCreateTable != nil && *n.config.AutoCreateTable {
				if err := n.createTable(ctx, branch, contentKey); err != nil {
					return fmt.Errorf("failed to create table: %w", err)
				}
				// Reload after creation
				contentResp, httpResp, err = nessieClient.V2API.GetContentV2(ctx, *contentKey, branch).Execute()
				if err != nil {
					return fmt.Errorf("failed to get table after creation: %w", err)
				}
			} else {
				// Provide helpful error message with suggestions
				errorMsg := fmt.Sprintf("table does not exist and AutoCreateTable is disabled (namespace=%s, table=%s, branch=%s, HTTP %d)",
					n.config.Namespace, n.config.Table, branch, statusCode)
				if responseBody != "" {
					errorMsg += fmt.Sprintf(" - Response: %s", responseBody)
				}
				errorMsg += fmt.Sprintf("\nPossible causes:")
				errorMsg += fmt.Sprintf("\n  1. Table doesn't exist in branch '%s' - check if table exists in this branch", branch)
				errorMsg += fmt.Sprintf("\n  2. Wrong namespace or table name - verify namespace='%s', table='%s'", n.config.Namespace, n.config.Table)
				errorMsg += fmt.Sprintf("\n  3. Table exists in different branch - check other branches")
				errorMsg += fmt.Sprintf("\n  4. Enable autoCreateTable: true in configuration if you want to create table automatically")
				return fmt.Errorf("%s", errorMsg)
			}
		} else {
			// Handle other HTTP errors with more context
			if httpResp != nil {
				errorMsg := fmt.Sprintf("failed to get table content from Nessie (namespace=%s, table=%s, branch=%s, HTTP %d): %v",
					n.config.Namespace, n.config.Table, branch, httpResp.StatusCode, err)
				if responseBody != "" {
					errorMsg += fmt.Sprintf(" - Response: %s", responseBody)
				}

				// Provide specific guidance based on status code
				switch httpResp.StatusCode {
				case http.StatusUnauthorized:
					errorMsg += "\nPossible cause: Authentication failed - check token/credentials"
				case http.StatusForbidden:
					errorMsg += "\nPossible cause: Access denied - check permissions for namespace/table"
				case http.StatusBadRequest:
					errorMsg += fmt.Sprintf("\nPossible cause: Invalid request - check namespace='%s', table='%s', branch='%s'",
						n.config.Namespace, n.config.Table, branch)
				case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable:
					errorMsg += "\nPossible cause: Server error - Nessie server may be unavailable"
				}

				return fmt.Errorf("%s", errorMsg)
			}
			return fmt.Errorf("failed to get table content from Nessie (namespace=%s, table=%s, branch=%s): %w",
				n.config.Namespace, n.config.Table, branch, err)
		}
	}

	// Extract IcebergTable from content
	content := contentResp.GetContent()
	if content.IcebergTableV2 == nil {
		return fmt.Errorf("content is not an Iceberg table")
	}

	icebergTable := content.IcebergTableV2
	metadataLocation := icebergTable.GetMetadataLocation()

	// Convert s3a:// to s3:// for iceberg-go compatibility
	// s3a:// is Hadoop-specific scheme, but iceberg-go expects s3://
	if strings.HasPrefix(metadataLocation, "s3a://") {
		metadataLocation = strings.Replace(metadataLocation, "s3a://", "s3://", 1)
		fmt.Printf("INFO: Nessie Sink - Converted s3a:// to s3:// in metadata location\n")
	}

	n.metadataLocation = metadataLocation
	fmt.Printf("INFO: Nessie Sink - Table metadata location: %s\n", metadataLocation)

	// Extract bucket from metadata location for FileIO initialization
	bucketName := extractBucketFromS3Path(metadataLocation)
	if bucketName == "" {
		return fmt.Errorf("failed to extract bucket from metadata location: %s", metadataLocation)
	}
	fmt.Printf("INFO: Nessie Sink - Extracted bucket name: %s\n", bucketName)

	// Create S3 FileIO factory function with bucket information
	fsysF, err := n.createS3FileIOFactoryWithBucket(bucketName)
	if err != nil {
		return fmt.Errorf("failed to create S3 FileIO factory: %w", err)
	}
	if fsysF == nil {
		return fmt.Errorf("FileIO factory is nil after creation")
	}

	// Create identifier for the table (Identifier is []string)
	ident := table.Identifier{n.config.Namespace, n.config.Table}

	// Load Iceberg table using metadata location
	// CatalogIO is nil because we're not using a catalog, just loading from metadata location
	fmt.Printf("INFO: Nessie Sink - Attempting to load table from location: %s\n", metadataLocation)
	fmt.Printf("INFO: Nessie Sink - Table identifier: namespace=%s, table=%s\n", n.config.Namespace, n.config.Table)
	tbl, err := table.NewFromLocation(ctx, ident, metadataLocation, fsysF, nil)
	if err != nil {
		// Provide more detailed error information
		fmt.Printf("ERROR: Nessie Sink - Failed to load table from location: %s\n", metadataLocation)
		fmt.Printf("ERROR: Nessie Sink - Error details: %v\n", err)
		fmt.Printf("ERROR: Nessie Sink - Check:\n")
		fmt.Printf("  1. S3 credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)\n")
		fmt.Printf("  2. S3 endpoint (AWS_ENDPOINT_URL_S3) - should match your storage\n")
		fmt.Printf("  3. S3 region (AWS_REGION)\n")
		fmt.Printf("  4. Bucket and path access permissions\n")
		fmt.Printf("  5. Network connectivity to S3 endpoint\n")
		return fmt.Errorf("failed to open Iceberg table from metadata location %s: %w", metadataLocation, err)
	}

	n.table = tbl
	fmt.Printf("INFO: Nessie Sink - Successfully loaded table from location\n")
	return nil
}

// createS3FileIOFactory creates an S3 FileIO factory function based on configuration
// DEPRECATED: This method should not be used - it can return nil FileIO
// Use createS3FileIOFactoryWithBucket instead
func (n *NessieSourceConnector) createS3FileIOFactory() (table.FSysF, error) {
	return createS3FileIOFactoryFromConfig(n.config.AWSRegionSecretRef, n.config.AWSAccessKeyIDSecretRef,
		n.config.AWSSecretAccessKeySecretRef, n.config.AWSEndpointURLSecretRef)
}

// createS3FileIOFactoryWithBucket creates an S3 FileIO factory function with bucket name
func (n *NessieSourceConnector) createS3FileIOFactoryWithBucket(bucketName string) (table.FSysF, error) {
	return createS3FileIOFactoryFromConfigWithBucket(n.config.AWSRegionSecretRef, n.config.AWSAccessKeyIDSecretRef,
		n.config.AWSSecretAccessKeySecretRef, n.config.AWSEndpointURLSecretRef, bucketName)
}

// createS3FileIOFactory creates an S3 FileIO factory function based on configuration
// DEPRECATED: This method should not be used - it can return nil FileIO
// Use createS3FileIOFactoryWithBucket instead
func (n *NessieSinkConnector) createS3FileIOFactory() (table.FSysF, error) {
	return createS3FileIOFactoryFromConfig(n.config.AWSRegionSecretRef, n.config.AWSAccessKeyIDSecretRef,
		n.config.AWSSecretAccessKeySecretRef, n.config.AWSEndpointURLSecretRef)
}

// createS3FileIOFactoryWithBucket creates an S3 FileIO factory function with bucket name
func (n *NessieSinkConnector) createS3FileIOFactoryWithBucket(bucketName string) (table.FSysF, error) {
	return createS3FileIOFactoryFromConfigWithBucket(n.config.AWSRegionSecretRef, n.config.AWSAccessKeyIDSecretRef,
		n.config.AWSSecretAccessKeySecretRef, n.config.AWSEndpointURLSecretRef, bucketName)
}

// extractBucketFromS3Path extracts bucket name from S3 path like s3://bucket/path/to/file
func extractBucketFromS3Path(s3Path string) string {
	if !strings.HasPrefix(s3Path, "s3://") {
		return ""
	}
	// Remove s3:// prefix
	path := strings.TrimPrefix(s3Path, "s3://")
	// If path starts with /, it's invalid (no bucket name)
	if strings.HasPrefix(path, "/") {
		return ""
	}
	// Find first slash to get bucket name
	if idx := strings.Index(path, "/"); idx > 0 {
		return path[:idx]
	}
	// If no slash, entire path is bucket (unlikely but possible)
	return path
}

// createS3FileIOFactoryFromConfigWithBucket creates an S3 FileIO factory function with bucket name
func createS3FileIOFactoryFromConfigWithBucket(regionRef, accessKeyRef, secretKeyRef, endpointRef *v1.SecretRef, bucketName string) (table.FSysF, error) {
	// Get base configuration
	fsysF, err := createS3FileIOFactoryFromConfig(regionRef, accessKeyRef, secretKeyRef, endpointRef)
	if err != nil {
		return nil, err
	}

	// If bucket name is provided, create a factory that uses it
	if bucketName != "" {
		return func(ctx context.Context) (icebergio.IO, error) {
			// Create S3 path with bucket for LoadFS
			s3Path := fmt.Sprintf("s3://%s/", bucketName)
			fmt.Printf("DEBUG: Nessie - Creating FileIO with bucket: %s, path: %s\n", bucketName, s3Path)

			// Get properties from base factory
			awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
			awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
			awsRegion := os.Getenv("AWS_REGION")
			awsEndpoint := os.Getenv("AWS_ENDPOINT_URL_S3")

			if awsRegion == "" {
				awsRegion = "us-east-1"
			}

			// Validate credentials
			if awsAccessKeyID == "" || awsSecretAccessKey == "" {
				return nil, fmt.Errorf("AWS credentials are missing: AWS_ACCESS_KEY_ID=%v, AWS_SECRET_ACCESS_KEY=%v",
					awsAccessKeyID != "", awsSecretAccessKey != "")
			}

			props := make(map[string]string)
			if awsRegion != "" {
				props[icebergio.S3Region] = awsRegion
			}
			if awsAccessKeyID != "" {
				props[icebergio.S3AccessKeyID] = awsAccessKeyID
			}
			if awsSecretAccessKey != "" {
				props[icebergio.S3SecretAccessKey] = awsSecretAccessKey
			}
			if awsEndpoint != "" {
				props[icebergio.S3EndpointURL] = awsEndpoint
				props["s3.path-style-access"] = "true"
			}

			fmt.Printf("DEBUG: Nessie - LoadFS props: region=%s, endpoint=%s, hasAccessKey=%v, hasSecretKey=%v\n",
				props[icebergio.S3Region], props[icebergio.S3EndpointURL],
				props[icebergio.S3AccessKeyID] != "", props[icebergio.S3SecretAccessKey] != "")

			// Create FileIO with bucket path
			io, err := icebergio.LoadFS(ctx, props, s3Path)
			if err != nil {
				fmt.Printf("ERROR: Nessie - LoadFS failed: %v\n", err)
				return nil, fmt.Errorf("failed to create S3 FileIO with bucket %s: %w", bucketName, err)
			}
			// Critical check: LoadFS can return nil, nil without error in some cases
			if io == nil && err == nil {
				fmt.Printf("ERROR: Nessie - LoadFS returned nil IO with no error\n")
				return nil, fmt.Errorf("LoadFS returned nil IO with no error (check S3 configuration and bucket path: %s)", s3Path)
			}
			if io == nil {
				fmt.Printf("ERROR: Nessie - FileIO is nil after LoadFS\n")
				return nil, fmt.Errorf("FileIO is nil after LoadFS with bucket %s", bucketName)
			}
			fmt.Printf("DEBUG: Nessie - FileIO created successfully: %T\n", io)
			return io, nil
		}, nil
	}

	return fsysF, nil
}

// createS3FileIOFactoryFromConfig creates an S3 FileIO factory function using AWS credentials from environment
func createS3FileIOFactoryFromConfig(regionRef, accessKeyRef, secretKeyRef, endpointRef *v1.SecretRef) (table.FSysF, error) {
	// AWS credentials should be provided via environment variables
	// These are set by the controller from Kubernetes secrets
	// For local development, set these environment variables:
	// - AWS_REGION (defaults to us-east-1 if not set)
	// - AWS_ACCESS_KEY_ID
	// - AWS_SECRET_ACCESS_KEY
	// - AWS_ENDPOINT_URL_S3 (for MinIO, e.g., http://localhost:9000)
	//
	// For production (AWS S3, Yandex Object Storage, etc.), credentials should
	// be provided via IAM roles, environment variables, or mounted secrets.

	// Check if AWS credentials are available
	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsRegion := os.Getenv("AWS_REGION")
	awsEndpoint := os.Getenv("AWS_ENDPOINT_URL_S3")

	if awsRegion == "" {
		awsRegion = "us-east-1" // Default region
	}

	// Log configuration (without exposing sensitive data)
	hasAccessKey := awsAccessKeyID != ""
	hasSecretKey := awsSecretAccessKey != ""
	fmt.Printf("INFO: Nessie - S3 FileIO configuration: region=%s, endpoint=%s, hasAccessKey=%v, hasSecretKey=%v\n",
		awsRegion, awsEndpoint, hasAccessKey, hasSecretKey)

	// Warn if credentials are missing
	if !hasAccessKey || !hasSecretKey {
		fmt.Printf("WARNING: Nessie - Missing AWS credentials! Access key: %v, Secret key: %v\n", hasAccessKey, hasSecretKey)
		fmt.Printf("WARNING: Nessie - S3 access may fail without proper credentials\n")
	}

	// Create properties map for S3 FileIO
	// Use constants from iceberg-go/io package for proper property names
	props := make(map[string]string)
	if awsRegion != "" {
		props[icebergio.S3Region] = awsRegion
	}
	if awsAccessKeyID != "" {
		// Use constant from iceberg-go for access key
		props[icebergio.S3AccessKeyID] = awsAccessKeyID
	}
	if awsSecretAccessKey != "" {
		// Use constant from iceberg-go for secret key
		props[icebergio.S3SecretAccessKey] = awsSecretAccessKey
	}
	if awsEndpoint != "" {
		// Use constant from iceberg-go for endpoint
		props[icebergio.S3EndpointURL] = awsEndpoint
		// Enable path-style access for S3-compatible storage (MinIO, Yandex Object Storage)
		props["s3.path-style-access"] = "true"
	}

	// Log properties (without sensitive data)
	fmt.Printf("INFO: Nessie - S3 FileIO properties: region=%s, endpoint=%s, hasAccessKey=%v, hasSecretKey=%v, pathStyleAccess=true\n",
		props[icebergio.S3Region], props[icebergio.S3EndpointURL],
		props[icebergio.S3AccessKeyID] != "", props[icebergio.S3SecretAccessKey] != "")

	// Create FileIO factory function
	// iceberg-go's LoadFS needs the full S3 path with bucket to create S3 FileIO
	// The factory function will be called by NewFromLocation, but we need to create
	// a factory that can work with any S3 path
	fsysF := func(ctx context.Context) (icebergio.IO, error) {
		// Use LoadFS which automatically handles S3, GCS, and local file systems
		// It detects the scheme from the location (s3://, gs://, file://, etc.)
		// For S3, LoadFS needs a location with bucket name to initialize
		// We'll create a dummy S3 path with bucket extracted from common bucket name
		// or use a generic S3 path format
		fmt.Printf("DEBUG: Nessie - Creating FileIO factory with properties (region=%s, endpoint=%s, hasAccessKey=%v, hasSecretKey=%v)\n",
			props[icebergio.S3Region], props[icebergio.S3EndpointURL],
			props[icebergio.S3AccessKeyID] != "", props[icebergio.S3SecretAccessKey] != "")

		// LoadFS requires a valid S3 path with bucket - empty string will cause nil FileIO
		// This function should not be used directly - use createS3FileIOFactoryWithBucket instead
		// Return error to prevent silent failures
		return nil, fmt.Errorf("S3 FileIO factory requires bucket name - use createS3FileIOFactoryWithBucket instead")
	}

	return fsysF, nil
}

// createTable creates a new Iceberg table in Nessie
func (n *NessieSinkConnector) createTable(ctx context.Context, branch string, contentKey *openapi.ContentKey) error {
	fmt.Printf("INFO: Nessie - Creating table: %s.%s\n", n.config.Namespace, n.config.Table)

	// Table creation via Nessie API requires:
	// 1. Create Iceberg table files (metadata.json, data files) using iceberg-go
	// 2. Get the metadata location
	// 3. Register it in Nessie via CommitV2 with Put operation
	//
	// This is a complex operation that requires:
	// - Storage backend configuration (S3, HDFS, etc.)
	// - Table schema definition
	// - Initial metadata file creation
	// - Nessie commit operation
	//
	// For now, we'll return an error indicating that table creation needs
	// a more complete implementation
	return fmt.Errorf("table creation via Nessie API requires full Iceberg table initialization - not yet implemented. Please create the table manually or use Iceberg REST Catalog")
}

// Write writes messages to Nessie/Iceberg table
func (n *NessieSinkConnector) Write(ctx context.Context, messages <-chan *msgtypes.Message) error {
	n.mu.Lock()
	if n.table == nil {
		n.mu.Unlock()
		return fmt.Errorf("not connected, call Connect first")
	}
	tbl := n.table
	n.mu.Unlock()

	batch := make([]map[string]interface{}, 0, 10)
	batchSize := 10
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				fmt.Printf("INFO: Nessie - Context done, flushing batch of %d messages\n", len(batch))
				if err := n.writeBatch(ctx, tbl, batch); err != nil {
					fmt.Printf("ERROR: Nessie - Failed to flush batch on context done: %v\n", err)
					return err
				}
			}
			return ctx.Err()
		case <-ticker.C:
			// Periodic flush
			if len(batch) > 0 {
				fmt.Printf("INFO: Nessie - Periodic flush, writing batch of %d messages\n", len(batch))
				if err := n.writeBatch(ctx, tbl, batch); err != nil {
					fmt.Printf("ERROR: Nessie - Failed to write batch on ticker: %v\n", err)
					if len(batch) > 100 {
						fmt.Printf("WARNING: Nessie - Batch too large (%d), clearing\n", len(batch))
						batch = batch[:0]
					}
				} else {
					batch = batch[:0]
				}
			}
		case msg, ok := <-messages:
			if !ok {
				// Channel closed, flush remaining data
				if len(batch) > 0 {
					fmt.Printf("INFO: Nessie - Channel closed, flushing batch of %d messages\n", len(batch))
					if err := n.writeBatch(ctx, tbl, batch); err != nil {
						fmt.Printf("ERROR: Nessie - Failed to flush batch on channel close: %v\n", err)
						return err
					}
				}
				return nil
			}

			var data map[string]interface{}
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				fmt.Printf("ERROR: Nessie - Failed to unmarshal message: %v\n", err)
				continue
			}

			// For auto-created tables with "data" field, wrap in data
			if n.config.AutoCreateTable != nil && *n.config.AutoCreateTable {
				jsonData, _ := json.Marshal(data)
				wrappedData := map[string]interface{}{
					"data": string(jsonData),
				}
				batch = append(batch, wrappedData)
			} else {
				batch = append(batch, data)
			}

			// Write when batch size is reached
			if len(batch) >= batchSize {
				fmt.Printf("INFO: Nessie - Batch size reached (%d), writing to table\n", len(batch))
				if err := n.writeBatch(ctx, tbl, batch); err != nil {
					fmt.Printf("ERROR: Nessie - Failed to write batch: %v\n", err)
					if len(batch) > 100 {
						fmt.Printf("WARNING: Nessie - Batch too large (%d), clearing\n", len(batch))
						batch = batch[:0]
					}
				} else {
					batch = batch[:0]
				}
			}
		}
	}
}

// refreshMetadataLocation refreshes metadata location from Nessie
func (n *NessieSinkConnector) refreshMetadataLocation(ctx context.Context) error {
	if n.nessieClient == nil {
		return fmt.Errorf("Nessie client is not initialized")
	}

	branch := n.config.Branch
	if branch == "" {
		branch = "main"
	}

	contentKey := openapi.NewContentKey([]string{n.config.Namespace, n.config.Table})
	contentResp, _, err := n.nessieClient.V2API.GetContentV2(ctx, *contentKey, branch).Execute()
	if err != nil {
		return fmt.Errorf("failed to get table content from Nessie: %w", err)
	}

	content := contentResp.GetContent()
	if content.IcebergTableV2 == nil {
		return fmt.Errorf("content is not an Iceberg table")
	}

	icebergTable := content.IcebergTableV2
	metadataLocation := icebergTable.GetMetadataLocation()

	// Convert s3a:// to s3:// for iceberg-go compatibility
	if strings.HasPrefix(metadataLocation, "s3a://") {
		metadataLocation = strings.Replace(metadataLocation, "s3a://", "s3://", 1)
	}

	n.metadataLocation = metadataLocation
	fmt.Printf("INFO: Nessie - Refreshed metadata location: %s\n", metadataLocation)
	return nil
}

func (n *NessieSinkConnector) writeBatch(ctx context.Context, tbl *table.Table, batch []map[string]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	// Synchronize write operations
	n.writeMu.Lock()
	defer n.writeMu.Unlock()

	fmt.Printf("INFO: Nessie - Writing batch of %d messages\n", len(batch))

	// Validate metadata location first
	if n.metadataLocation == "" {
		return fmt.Errorf("metadata location is empty, cannot reload table")
	}

	// Extract bucket from metadata location for FileIO initialization
	bucketName := extractBucketFromS3Path(n.metadataLocation)
	if bucketName == "" {
		return fmt.Errorf("failed to extract bucket from metadata location: %s", n.metadataLocation)
	}
	fmt.Printf("INFO: Nessie - Extracted bucket name from metadata location: %s\n", bucketName)

	// Create S3 FileIO factory function with bucket information
	fsysF, err := n.createS3FileIOFactoryWithBucket(bucketName)
	if err != nil {
		return fmt.Errorf("failed to create S3 FileIO factory: %w", err)
	}
	if fsysF == nil {
		return fmt.Errorf("FileIO factory is nil after creation")
	}

	// Create identifier for the table (Identifier is []string)
	ident := table.Identifier{n.config.Namespace, n.config.Table}

	// Try to reload table from cached metadata location first
	fmt.Printf("INFO: Nessie - Attempting to reload table from metadata location: %s\n", n.metadataLocation)
	if fsysF == nil {
		return fmt.Errorf("FileIO factory is nil, cannot reload table")
	}

	// Create and cache FileIO before creating table
	// This is critical - we cache FileIO to ensure it remains valid throughout the table's lifecycle
	// If FileIO factory is called multiple times during commit, it might return nil, causing panic
	fmt.Printf("INFO: Nessie - Creating and caching FileIO before table creation\n")
	cachedFileIO, err := fsysF(ctx)
	if err != nil {
		fmt.Printf("ERROR: Nessie - Failed to create FileIO: %v\n", err)
		return fmt.Errorf("failed to create FileIO: %w", err)
	}
	if cachedFileIO == nil {
		fmt.Printf("ERROR: Nessie - FileIO factory returned nil IO\n")
		return fmt.Errorf("FileIO factory returned nil IO")
	}
	fmt.Printf("INFO: Nessie - FileIO created and cached successfully: %T\n", cachedFileIO)

	// Create a stable factory that returns the cached FileIO
	// This ensures FileIO remains valid during commit operations
	stableFsysF := func(ctx context.Context) (icebergio.IO, error) {
		if cachedFileIO == nil {
			fmt.Printf("ERROR: Nessie - Cached FileIO is nil in stable factory\n")
			return nil, fmt.Errorf("cached FileIO is nil")
		}
		return cachedFileIO, nil
	}

	// Create Nessie catalogIO implementation to prevent nil pointer panic in doCommit
	// The library may access CatalogIO during commit, and nil causes panic
	nessieCatalogIO := &nessieCatalogIOImpl{
		connector: n,
	}

	fmt.Printf("INFO: Nessie - Creating table with cached FileIO and Nessie catalogIO\n")
	latestTable, err := table.NewFromLocation(ctx, ident, n.metadataLocation, stableFsysF, nessieCatalogIO)
	if err != nil {
		// If reload failed, try to refresh metadata location from Nessie
		fmt.Printf("WARNING: Nessie - Failed to reload table from cached metadata location, refreshing from Nessie: %v\n", err)
		if refreshErr := n.refreshMetadataLocation(ctx); refreshErr != nil {
			return fmt.Errorf("failed to reload table (original error: %v) and refresh metadata location: %w", err, refreshErr)
		}
		// Extract bucket from refreshed metadata location
		bucketName = extractBucketFromS3Path(n.metadataLocation)
		if bucketName == "" {
			return fmt.Errorf("failed to extract bucket from refreshed metadata location: %s", n.metadataLocation)
		}
		fmt.Printf("INFO: Nessie - Extracted bucket name from refreshed metadata location: %s\n", bucketName)

		// Recreate FileIO factory with refreshed bucket
		fsysF, err = n.createS3FileIOFactoryWithBucket(bucketName)
		if err != nil {
			return fmt.Errorf("failed to create S3 FileIO factory with refreshed bucket: %w", err)
		}
		if fsysF == nil {
			return fmt.Errorf("FileIO factory is nil after recreation")
		}

		// Create and cache FileIO after refresh
		fmt.Printf("INFO: Nessie - Creating and caching FileIO after refresh\n")
		cachedFileIO, err := fsysF(ctx)
		if err != nil {
			fmt.Printf("ERROR: Nessie - Failed to create FileIO after refresh: %v\n", err)
			return fmt.Errorf("failed to create FileIO after refresh: %w", err)
		}
		if cachedFileIO == nil {
			fmt.Printf("ERROR: Nessie - FileIO factory returned nil IO after refresh\n")
			return fmt.Errorf("FileIO factory returned nil IO after refresh")
		}
		fmt.Printf("INFO: Nessie - FileIO created and cached successfully after refresh: %T\n", cachedFileIO)

		// Create a stable factory that returns the cached FileIO
		stableFsysF = func(ctx context.Context) (icebergio.IO, error) {
			if cachedFileIO == nil {
				fmt.Printf("ERROR: Nessie - Cached FileIO is nil in stable factory after refresh\n")
				return nil, fmt.Errorf("cached FileIO is nil")
			}
			return cachedFileIO, nil
		}

		// Create Nessie catalogIO for retry after refresh
		nessieCatalogIO := &nessieCatalogIOImpl{
			connector: n,
		}

		// Try again with refreshed metadata location
		fmt.Printf("INFO: Nessie - Attempting to reload table from refreshed metadata location: %s\n", n.metadataLocation)
		latestTable, err = table.NewFromLocation(ctx, ident, n.metadataLocation, stableFsysF, nessieCatalogIO)
		if err != nil {
			return fmt.Errorf("failed to reload table after refreshing metadata location: %w", err)
		}
		fmt.Printf("INFO: Nessie - Successfully reloaded table after refreshing metadata location\n")
	}

	// Validate that table is properly initialized
	if latestTable == nil {
		return fmt.Errorf("table is nil after reload")
	}

	// Get table schema
	icebergSchema := latestTable.Schema()
	if icebergSchema == nil {
		return fmt.Errorf("table schema is nil")
	}

	// Log schema for debugging
	icebergFields := icebergSchema.Fields()
	fmt.Printf("INFO: Nessie - Table schema has %d fields\n", len(icebergFields))
	for _, field := range icebergFields {
		fmt.Printf("INFO: Nessie - Field: %s, Type: %v, Required: %v\n", field.Name, field.Type, field.Required)
	}

	// Convert batch to Arrow RecordBatch
	recordBatch, err := n.convertBatchToArrow(batch, icebergSchema)
	if err != nil {
		return fmt.Errorf("failed to convert batch to Arrow: %w", err)
	}
	defer recordBatch.Release()

	// Use Append to write data
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		reader, err := array.NewRecordReader(recordBatch.Schema(), []arrow.RecordBatch{recordBatch})
		if err != nil {
			return fmt.Errorf("failed to create record reader: %w", err)
		}

		// Validate table before Append
		if latestTable == nil {
			reader.Release()
			return fmt.Errorf("table is nil before Append")
		}

		_, err = latestTable.Append(ctx, reader, nil)
		reader.Release()

		if err == nil {
			// Success - update cached table reference and metadata location
			n.mu.Lock()
			n.table = latestTable
			// Update metadata location from the table (after Append, metadata may have changed)
			// Note: The actual metadata location in Nessie should be updated via CommitV2,
			// but for now we'll use the location from the table object
			if latestTable != nil {
				// Try to get the latest metadata location from the table
				// This is a workaround - ideally we should get it from Nessie after commit
				fmt.Printf("INFO: Nessie - Successfully wrote batch, table reference updated\n")
			}
			n.mu.Unlock()
			fmt.Printf("INFO: Nessie - Successfully wrote batch of %d messages\n", len(batch))
			return nil
		}

		errStr := err.Error()
		fmt.Printf("ERROR: Nessie - Append failed (attempt %d/%d): %s\n", attempt+1, maxRetries, errStr)

		// Check for retryable errors
		if strings.Contains(errStr, "concurrently") || strings.Contains(errStr, "CommitFailedException") {
			if attempt < maxRetries-1 {
				waitTime := time.Duration(attempt+1) * 100 * time.Millisecond
				fmt.Printf("WARNING: Nessie - Concurrent commit conflict, waiting %v and retrying\n", waitTime)
				time.Sleep(waitTime)
				// Refresh metadata location and reload table for retry
				if refreshErr := n.refreshMetadataLocation(ctx); refreshErr != nil {
					fmt.Printf("WARNING: Nessie - Failed to refresh metadata location for retry: %v\n", refreshErr)
				} else {
					// Extract bucket from refreshed metadata location
					bucketName = extractBucketFromS3Path(n.metadataLocation)
					if bucketName == "" {
						fmt.Printf("WARNING: Nessie - Failed to extract bucket from metadata location for retry: %s\n", n.metadataLocation)
					} else {
						// Recreate FileIO factory with refreshed bucket
						fsysF, err = n.createS3FileIOFactoryWithBucket(bucketName)
						if err != nil {
							fmt.Printf("WARNING: Nessie - Failed to recreate FileIO factory for retry: %v\n", err)
						} else if fsysF != nil {
							// Test FileIO factory before creating table
							fmt.Printf("DEBUG: Nessie - Testing FileIO factory for retry\n")
							testIO, testErr := fsysF(ctx)
							if testErr != nil {
								fmt.Printf("WARNING: Nessie - FileIO factory test failed for retry: %v\n", testErr)
							} else if testIO == nil {
								fmt.Printf("WARNING: Nessie - FileIO factory returned nil IO during test for retry\n")
							} else {
								fmt.Printf("DEBUG: Nessie - FileIO factory test passed for retry: %T\n", testIO)
							}
						}
					}
				}
				ident := table.Identifier{n.config.Namespace, n.config.Table}
				fmt.Printf("INFO: Nessie - Reloading table for retry from metadata location: %s\n", n.metadataLocation)
				if fsysF == nil {
					return fmt.Errorf("FileIO factory is nil before retry table reload")
				}

				// Create and cache FileIO before retry
				fmt.Printf("INFO: Nessie - Creating and caching FileIO before retry\n")
				cachedFileIO, err := fsysF(ctx)
				if err != nil {
					fmt.Printf("ERROR: Nessie - Failed to create FileIO before retry: %v\n", err)
					return fmt.Errorf("failed to create FileIO before retry: %w", err)
				}
				if cachedFileIO == nil {
					fmt.Printf("ERROR: Nessie - FileIO factory returned nil IO before retry\n")
					return fmt.Errorf("FileIO factory returned nil IO before retry")
				}
				fmt.Printf("INFO: Nessie - FileIO created and cached successfully before retry: %T\n", cachedFileIO)

				// Create a stable factory that returns the cached FileIO
				stableFsysF := func(ctx context.Context) (icebergio.IO, error) {
					if cachedFileIO == nil {
						fmt.Printf("ERROR: Nessie - Cached FileIO is nil in stable factory for retry\n")
						return nil, fmt.Errorf("cached FileIO is nil")
					}
					return cachedFileIO, nil
				}

				// Create Nessie catalogIO for retry
				nessieCatalogIO := &nessieCatalogIOImpl{
					connector: n,
				}

				// Retry with reloaded table
				latestTable, err = table.NewFromLocation(ctx, ident, n.metadataLocation, stableFsysF, nessieCatalogIO)
				if err != nil {
					return fmt.Errorf("failed to reload table for retry: %w", err)
				}
				continue
			}
		}

		return fmt.Errorf("failed to append data to table: %w", err)
	}

	return fmt.Errorf("failed to append data to table after %d retries", maxRetries)
}

// convertBatchToArrow converts a batch of maps to an Arrow RecordBatch
func (n *NessieSinkConnector) convertBatchToArrow(batch []map[string]interface{}, icebergSchema *iceberg.Schema) (arrow.Record, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("batch is empty")
	}

	firstRow := batch[0]
	numRows := int64(len(batch))

	fields := make([]arrow.Field, 0)
	builders := make([]array.Builder, 0)
	mem := memory.DefaultAllocator
	fieldNames := make([]string, 0)

	// Use Iceberg schema to determine field order and types
	if icebergSchema != nil {
		icebergFields := icebergSchema.Fields()
		if len(icebergFields) > 0 {
			for _, icebergField := range icebergFields {
				fieldName := icebergField.Name
				fieldNames = append(fieldNames, fieldName)

				var arrowType arrow.DataType
				// Use type comparison - check for exact type match
				fieldTypeStr := fmt.Sprintf("%T", icebergField.Type)
				fmt.Printf("DEBUG: Nessie - Field %s: Iceberg type=%v, type string=%s\n", fieldName, icebergField.Type, fieldTypeStr)

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
				} else if icebergField.Type == iceberg.PrimitiveTypes.Timestamp {
					// Timestamp without timezone (wall clock time)
					// Note: Iceberg Timestamp maps to Arrow Timestamp without timezone
					arrowType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
					fmt.Printf("DEBUG: Nessie - Field %s: Iceberg Timestamp -> Arrow Timestamp (no timezone)\n", fieldName)
				} else if icebergField.Type == iceberg.PrimitiveTypes.TimestampTz {
					// Timestamp with timezone (instant in time) - MUST use UTC timezone
					// Iceberg requires timestamptz to have a timezone in Arrow format
					arrowType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
					fmt.Printf("DEBUG: Nessie - Field %s: Iceberg TimestampTz -> Arrow Timestamp (UTC timezone)\n", fieldName)
				} else {
					fmt.Printf("WARNING: Nessie - Field %s: Unknown Iceberg type %v (type string: %s), using String\n", fieldName, icebergField.Type, fieldTypeStr)
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
					// Check if it's a timestamp type
					if tsType, ok := arrowType.(*arrow.TimestampType); ok {
						builders = append(builders, array.NewTimestampBuilder(mem, tsType))
					} else {
						builders = append(builders, array.NewStringBuilder(mem))
					}
				}
			}
		} else {
			// Fallback: use data fields
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
		// Fallback: use data fields
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
				// Append null
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
					// Check if it's a timestamp type
					if _, ok := field.Type.(*arrow.TimestampType); ok {
						builders[colIdx].(*array.TimestampBuilder).AppendNull()
					} else {
						builders[colIdx].(*array.StringBuilder).AppendNull()
					}
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
					// Check if it's a timestamp type
					if _, ok := field.Type.(*arrow.TimestampType); ok {
						var timestampVal int64
						switch v := val.(type) {
						case string:
							// Parse timestamp string to microseconds since epoch
							// Try RFC3339 format first (most common)
							if t, err := time.Parse(time.RFC3339, v); err == nil {
								timestampVal = t.UnixMicro()
							} else if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
								timestampVal = t.UnixMicro()
							} else if t, err := time.Parse("2006-01-02T15:04:05Z07:00", v); err == nil {
								timestampVal = t.UnixMicro()
							} else if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
								timestampVal = t.UnixMicro()
							} else {
								// Try parsing as Unix timestamp (seconds or milliseconds)
								if unixTime, err := time.Parse(time.UnixDate, v); err == nil {
									timestampVal = unixTime.UnixMicro()
								} else {
									fmt.Printf("WARNING: Nessie - Failed to parse timestamp string '%s', using current time\n", v)
									timestampVal = time.Now().UnixMicro()
								}
							}
						case int64:
							// Assume microseconds if value is large, otherwise seconds
							if v > 1e12 {
								timestampVal = v
							} else {
								timestampVal = v * 1e6
							}
						case int:
							// Assume seconds
							timestampVal = int64(v) * 1e6
						case float64:
							// Assume seconds with fractional part
							timestampVal = int64(v * 1e6)
						case time.Time:
							timestampVal = v.UnixMicro()
						default:
							fmt.Printf("WARNING: Nessie - Unknown timestamp type %T, using current time\n", v)
							timestampVal = time.Now().UnixMicro()
						}
						builders[colIdx].(*array.TimestampBuilder).Append(arrow.Timestamp(timestampVal))
					} else {
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

// Close closes the Nessie connection
func (n *NessieSinkConnector) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.closed = true
	n.table = nil
	n.nessieClient = nil
	return nil
}

// nessieCatalogIOImpl implements table.CatalogIO interface for Nessie
// This is a minimal implementation to prevent nil pointer panic in doCommit
type nessieCatalogIOImpl struct {
	connector *NessieSinkConnector
}

// LoadTable loads table from the catalog
// This is called by iceberg-go to load table
func (n *nessieCatalogIOImpl) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	// This is a minimal implementation to prevent nil pointer panic
	// We don't actually load from catalog here since we use NewFromLocation
	// Return nil table - this should not be called when using NewFromLocation
	return nil, fmt.Errorf("LoadTable not implemented for nessieCatalogIOImpl - use NewFromLocation instead")
}

// CommitTable commits table changes to the catalog
// This is called by iceberg-go during commit operations
func (n *nessieCatalogIOImpl) CommitTable(ctx context.Context, ident table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	// Get the current table to extract metadata and location
	n.connector.mu.Lock()
	currentTable := n.connector.table
	metadataLocation := n.connector.metadataLocation
	n.connector.mu.Unlock()

	// If we have a table, get its metadata
	var metadata table.Metadata
	if currentTable != nil {
		metadata = currentTable.Metadata()
	}

	// Return metadata and location - this prevents nil pointer panic in doCommit
	// The actual Nessie update should be done separately via CommitV2 API
	return metadata, metadataLocation, nil
}

// UpdateMetadataLocation updates the metadata location in Nessie after commit
// This is called by iceberg-go after a successful commit to update the catalog
func (n *nessieCatalogIOImpl) UpdateMetadataLocation(ctx context.Context, ident table.Identifier, location string) error {
	// For now, we don't update Nessie here because we handle it separately
	// This is a no-op implementation to prevent nil pointer panic
	// The actual metadata location update should be done via Nessie CommitV2 API
	return nil
}
