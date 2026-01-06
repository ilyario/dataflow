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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DataFlowSpec defines the desired state of DataFlow
type DataFlowSpec struct {
	// Source defines the source of data
	Source SourceSpec `json:"source"`

	// Sink defines the destination of data
	Sink SinkSpec `json:"sink"`

	// Transformations is a list of transformations to apply to messages
	// +optional
	Transformations []TransformationSpec `json:"transformations,omitempty"`
}

// SourceSpec defines the source configuration
type SourceSpec struct {
	// Type of source: kafka, postgresql, iceberg, rabbitmq
	Type string `json:"type"`

	// Kafka source configuration
	// +optional
	Kafka *KafkaSourceSpec `json:"kafka,omitempty"`

	// PostgreSQL source configuration
	// +optional
	PostgreSQL *PostgreSQLSourceSpec `json:"postgresql,omitempty"`

	// Iceberg source configuration
	// +optional
	Iceberg *IcebergSourceSpec `json:"iceberg,omitempty"`

	// RabbitMQ source configuration
	// +optional
	RabbitMQ *RabbitMQSourceSpec `json:"rabbitmq,omitempty"`
}

// KafkaSourceSpec defines Kafka source configuration
type KafkaSourceSpec struct {
	// Brokers is a list of Kafka broker addresses
	Brokers []string `json:"brokers"`

	// Topic to read from
	Topic string `json:"topic"`

	// ConsumerGroup for Kafka consumer
	// +optional
	ConsumerGroup string `json:"consumerGroup,omitempty"`

	// TLS configuration
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// SASL configuration
	// +optional
	SASL *SASLConfig `json:"sasl,omitempty"`

	// BrokersSecretRef references a Kubernetes secret for brokers (comma-separated)
	// +optional
	BrokersSecretRef *SecretRef `json:"brokersSecretRef,omitempty"`

	// TopicSecretRef references a Kubernetes secret for topic
	// +optional
	TopicSecretRef *SecretRef `json:"topicSecretRef,omitempty"`

	// ConsumerGroupSecretRef references a Kubernetes secret for consumer group
	// +optional
	ConsumerGroupSecretRef *SecretRef `json:"consumerGroupSecretRef,omitempty"`
}

// PostgreSQLSourceSpec defines PostgreSQL source configuration
type PostgreSQLSourceSpec struct {
	// ConnectionString for PostgreSQL database
	ConnectionString string `json:"connectionString"`

	// Table to read from
	Table string `json:"table"`

	// Query for custom SQL query (optional, if not provided, reads from table)
	// +optional
	Query string `json:"query,omitempty"`

	// PollInterval in seconds for polling mode
	// +optional
	PollInterval *int32 `json:"pollInterval,omitempty"`

	// ConnectionStringSecretRef references a Kubernetes secret for connection string
	// +optional
	ConnectionStringSecretRef *SecretRef `json:"connectionStringSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`
}

// IcebergSourceSpec defines Iceberg source configuration
type IcebergSourceSpec struct {
	// RESTCatalogURL is the URL of the Iceberg REST Catalog
	RESTCatalogURL string `json:"restCatalogUrl"`

	// Namespace in the catalog
	Namespace string `json:"namespace"`

	// Table name
	Table string `json:"table"`

	// Authentication token
	// +optional
	Token string `json:"token,omitempty"`

	// RESTCatalogURLSecretRef references a Kubernetes secret for REST catalog URL
	// +optional
	RESTCatalogURLSecretRef *SecretRef `json:"restCatalogUrlSecretRef,omitempty"`

	// NamespaceSecretRef references a Kubernetes secret for namespace
	// +optional
	NamespaceSecretRef *SecretRef `json:"namespaceSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`

	// TokenSecretRef references a Kubernetes secret for authentication token
	// +optional
	TokenSecretRef *SecretRef `json:"tokenSecretRef,omitempty"`
}

// RabbitMQSourceSpec defines RabbitMQ source configuration
type RabbitMQSourceSpec struct {
	// URL connection string for RabbitMQ
	URL string `json:"url"`

	// Queue name to consume from
	Queue string `json:"queue"`

	// Exchange name (optional)
	// +optional
	Exchange string `json:"exchange,omitempty"`

	// RoutingKey (optional)
	// +optional
	RoutingKey string `json:"routingKey,omitempty"`

	// URLSecretRef references a Kubernetes secret for connection URL
	// +optional
	URLSecretRef *SecretRef `json:"urlSecretRef,omitempty"`

	// QueueSecretRef references a Kubernetes secret for queue name
	// +optional
	QueueSecretRef *SecretRef `json:"queueSecretRef,omitempty"`

	// ExchangeSecretRef references a Kubernetes secret for exchange name
	// +optional
	ExchangeSecretRef *SecretRef `json:"exchangeSecretRef,omitempty"`

	// RoutingKeySecretRef references a Kubernetes secret for routing key
	// +optional
	RoutingKeySecretRef *SecretRef `json:"routingKeySecretRef,omitempty"`
}

// SinkSpec defines the sink configuration
type SinkSpec struct {
	// Type of sink: kafka, postgresql, iceberg, rabbitmq
	Type string `json:"type"`

	// Kafka sink configuration
	// +optional
	Kafka *KafkaSinkSpec `json:"kafka,omitempty"`

	// PostgreSQL sink configuration
	// +optional
	PostgreSQL *PostgreSQLSinkSpec `json:"postgresql,omitempty"`

	// Iceberg sink configuration
	// +optional
	Iceberg *IcebergSinkSpec `json:"iceberg,omitempty"`

	// RabbitMQ sink configuration
	// +optional
	RabbitMQ *RabbitMQSinkSpec `json:"rabbitmq,omitempty"`
}

// KafkaSinkSpec defines Kafka sink configuration
type KafkaSinkSpec struct {
	// Brokers is a list of Kafka broker addresses
	Brokers []string `json:"brokers"`

	// Topic to write to
	Topic string `json:"topic"`

	// TLS configuration
	// +optional
	TLS *TLSConfig `json:"tls,omitempty"`

	// SASL configuration
	// +optional
	SASL *SASLConfig `json:"sasl,omitempty"`

	// BrokersSecretRef references a Kubernetes secret for brokers (comma-separated)
	// +optional
	BrokersSecretRef *SecretRef `json:"brokersSecretRef,omitempty"`

	// TopicSecretRef references a Kubernetes secret for topic
	// +optional
	TopicSecretRef *SecretRef `json:"topicSecretRef,omitempty"`
}

// PostgreSQLSinkSpec defines PostgreSQL sink configuration
type PostgreSQLSinkSpec struct {
	// ConnectionString for PostgreSQL database
	ConnectionString string `json:"connectionString"`

	// Table to write to
	Table string `json:"table"`

	// BatchSize for batch inserts
	// +optional
	BatchSize *int32 `json:"batchSize,omitempty"`

	// AutoCreateTable automatically creates the table if it doesn't exist
	// +optional
	AutoCreateTable *bool `json:"autoCreateTable,omitempty"`

	// ConnectionStringSecretRef references a Kubernetes secret for connection string
	// +optional
	ConnectionStringSecretRef *SecretRef `json:"connectionStringSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`
}

// IcebergSinkSpec defines Iceberg sink configuration
type IcebergSinkSpec struct {
	// RESTCatalogURL is the URL of the Iceberg REST Catalog
	RESTCatalogURL string `json:"restCatalogUrl"`

	// Namespace in the catalog
	Namespace string `json:"namespace"`

	// Table name
	Table string `json:"table"`

	// Authentication token
	// +optional
	Token string `json:"token,omitempty"`

	// AutoCreateNamespace automatically creates the namespace if it doesn't exist
	// Defaults to true if not specified
	// +optional
	AutoCreateNamespace *bool `json:"autoCreateNamespace,omitempty"`

	// AutoCreateTable automatically creates the table if it doesn't exist
	// +optional
	AutoCreateTable *bool `json:"autoCreateTable,omitempty"`

	// RESTCatalogURLSecretRef references a Kubernetes secret for REST catalog URL
	// +optional
	RESTCatalogURLSecretRef *SecretRef `json:"restCatalogUrlSecretRef,omitempty"`

	// NamespaceSecretRef references a Kubernetes secret for namespace
	// +optional
	NamespaceSecretRef *SecretRef `json:"namespaceSecretRef,omitempty"`

	// TableSecretRef references a Kubernetes secret for table name
	// +optional
	TableSecretRef *SecretRef `json:"tableSecretRef,omitempty"`

	// TokenSecretRef references a Kubernetes secret for authentication token
	// +optional
	TokenSecretRef *SecretRef `json:"tokenSecretRef,omitempty"`
}

// RabbitMQSinkSpec defines RabbitMQ sink configuration
type RabbitMQSinkSpec struct {
	// URL connection string for RabbitMQ
	URL string `json:"url"`

	// Exchange name
	Exchange string `json:"exchange"`

	// RoutingKey
	RoutingKey string `json:"routingKey"`

	// Queue name (optional, for direct queue publishing)
	// +optional
	Queue string `json:"queue,omitempty"`

	// URLSecretRef references a Kubernetes secret for connection URL
	// +optional
	URLSecretRef *SecretRef `json:"urlSecretRef,omitempty"`

	// ExchangeSecretRef references a Kubernetes secret for exchange name
	// +optional
	ExchangeSecretRef *SecretRef `json:"exchangeSecretRef,omitempty"`

	// RoutingKeySecretRef references a Kubernetes secret for routing key
	// +optional
	RoutingKeySecretRef *SecretRef `json:"routingKeySecretRef,omitempty"`

	// QueueSecretRef references a Kubernetes secret for queue name
	// +optional
	QueueSecretRef *SecretRef `json:"queueSecretRef,omitempty"`
}

// SecretRef references a Kubernetes secret
type SecretRef struct {
	// Name of the secret
	Name string `json:"name"`

	// Namespace of the secret (optional, defaults to the same namespace as DataFlow)
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Key in the secret to read the value from
	Key string `json:"key"`
}

// TLSConfig defines TLS configuration
type TLSConfig struct {
	// InsecureSkipVerify skips certificate verification
	// +optional
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// CertFile path to certificate file
	// +optional
	CertFile string `json:"certFile,omitempty"`

	// KeyFile path to key file
	// +optional
	KeyFile string `json:"keyFile,omitempty"`

	// CAFile path to CA certificate file
	// +optional
	CAFile string `json:"caFile,omitempty"`

	// CertSecretRef references a Kubernetes secret for certificate
	// +optional
	CertSecretRef *SecretRef `json:"certSecretRef,omitempty"`

	// KeySecretRef references a Kubernetes secret for key
	// +optional
	KeySecretRef *SecretRef `json:"keySecretRef,omitempty"`

	// CASecretRef references a Kubernetes secret for CA certificate
	// +optional
	CASecretRef *SecretRef `json:"caSecretRef,omitempty"`
}

// SASLConfig defines SASL configuration
type SASLConfig struct {
	// Mechanism: plain, scram-sha-256, scram-sha-512
	Mechanism string `json:"mechanism"`

	// Username
	Username string `json:"username"`

	// Password
	Password string `json:"password"`

	// UsernameSecretRef references a Kubernetes secret for username
	// +optional
	UsernameSecretRef *SecretRef `json:"usernameSecretRef,omitempty"`

	// PasswordSecretRef references a Kubernetes secret for password
	// +optional
	PasswordSecretRef *SecretRef `json:"passwordSecretRef,omitempty"`
}

// TransformationSpec defines a transformation to apply
type TransformationSpec struct {
	// Type of transformation: timestamp, flatten, filter, mask, router, select, remove
	Type string `json:"type"`

	// Timestamp transformation configuration
	// +optional
	Timestamp *TimestampTransformation `json:"timestamp,omitempty"`

	// Flatten transformation configuration
	// +optional
	Flatten *FlattenTransformation `json:"flatten,omitempty"`

	// Filter transformation configuration
	// +optional
	Filter *FilterTransformation `json:"filter,omitempty"`

	// Mask transformation configuration
	// +optional
	Mask *MaskTransformation `json:"mask,omitempty"`

	// Router transformation configuration
	// +optional
	Router *RouterTransformation `json:"router,omitempty"`

	// Select transformation configuration
	// +optional
	Select *SelectTransformation `json:"select,omitempty"`

	// Remove transformation configuration
	// +optional
	Remove *RemoveTransformation `json:"remove,omitempty"`
}

// TimestampTransformation adds a timestamp field
type TimestampTransformation struct {
	// FieldName is the name of the timestamp field (default: created_at)
	// +optional
	FieldName string `json:"fieldName,omitempty"`

	// Format is the timestamp format (default: RFC3339)
	// +optional
	Format string `json:"format,omitempty"`
}

// FlattenTransformation flattens an array field
type FlattenTransformation struct {
	// Field is the JSONPath to the array field to flatten
	Field string `json:"field"`
}

// FilterTransformation filters messages based on conditions
type FilterTransformation struct {
	// Condition is a JSONPath expression that must evaluate to true
	Condition string `json:"condition"`
}

// MaskTransformation masks sensitive data
type MaskTransformation struct {
	// Fields is a list of JSONPath expressions to mask
	Fields []string `json:"fields"`

	// MaskChar is the character to use for masking (default: *)
	// +optional
	MaskChar string `json:"maskChar,omitempty"`

	// KeepLength keeps the original length of the value
	// +optional
	KeepLength bool `json:"keepLength,omitempty"`
}

// RouterTransformation routes messages to different sinks
type RouterTransformation struct {
	// Routes is a list of routing rules
	Routes []RouteRule `json:"routes"`
}

// RouteRule defines a routing rule
type RouteRule struct {
	// Condition is a JSONPath expression that must evaluate to true
	Condition string `json:"condition"`

	// Sink is the sink configuration for this route
	Sink SinkSpec `json:"sink"`
}

// SelectTransformation selects specific fields
type SelectTransformation struct {
	// Fields is a list of JSONPath expressions to select
	Fields []string `json:"fields"`
}

// RemoveTransformation removes specific fields
type RemoveTransformation struct {
	// Fields is a list of JSONPath expressions to remove
	Fields []string `json:"fields"`
}

// DataFlowStatus defines the observed state of DataFlow
type DataFlowStatus struct {
	// Phase represents the current phase of the data flow
	// +optional
	Phase string `json:"phase,omitempty"`

	// Message provides additional information about the status
	// +optional
	Message string `json:"message,omitempty"`

	// LastProcessedTime is the timestamp of the last processed message
	// +optional
	LastProcessedTime *metav1.Time `json:"lastProcessedTime,omitempty"`

	// ProcessedCount is the number of processed messages
	// +optional
	ProcessedCount int64 `json:"processedCount,omitempty"`

	// ErrorCount is the number of errors encountered
	// +optional
	ErrorCount int64 `json:"errorCount,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// DataFlow is the Schema for the dataflows API
type DataFlow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataFlowSpec   `json:"spec,omitempty"`
	Status DataFlowStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// DataFlowList contains a list of DataFlow
type DataFlowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DataFlow `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DataFlow{}, &DataFlowList{})
}
