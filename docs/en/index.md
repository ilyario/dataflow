# DataFlow Operator

DataFlow Operator is a Kubernetes operator for streaming data between different data sources with support for message transformations.

## Overview

DataFlow Operator allows you to declaratively define data flows between different sources and sinks through Kubernetes Custom Resource Definitions (CRD). The operator automatically manages the lifecycle of data flows, processes messages, and applies necessary transformations.

## Key Features

### Multiple Data Source Support

- **Kafka** - read and write messages from/to Kafka topics with TLS and SASL authentication support
- **PostgreSQL** - read from tables and write to PostgreSQL tables with support for custom SQL queries and batch inserts
- **RabbitMQ** - read from queues and publish to RabbitMQ queues with support for exchanges and routing keys
- **Iceberg** - work with Apache Iceberg through REST API for big data processing

### Rich Transformation Set

- **Timestamp** - add timestamp to each message
- **Flatten** - expand arrays into separate messages while preserving parent fields
- **Filter** - filter messages based on JSONPath conditions
- **Mask** - mask sensitive data with or without preserving length
- **Router** - route messages to different sinks based on conditions
- **Select** - select specific fields from messages
- **Remove** - remove specified fields from messages

### Flexible Routing

The operator supports conditional routing of messages to different sinks based on JSONPath expressions, enabling complex data processing scenarios.

### Simple Management

Declarative configuration through Kubernetes CRD allows easy management of data flows, versioning configurations, and integration with CI/CD systems.

### Secure Configuration

Support for configuring connectors from Kubernetes Secrets through `SecretRef` allows secure storage of credentials, tokens, and connection strings without explicitly specifying them in the DataFlow specification.

## Quick Start

### Installing the Operator

```bash
# Install operator via Helm
helm install dataflow-operator ./helm/dataflow-operator

# Verify installation
kubectl get pods -l app.kubernetes.io/name=dataflow-operator

# Verify CRD
kubectl get crd dataflows.dataflow.dataflow.io
```

### Creating Your First Data Flow

Create a simple data flow from Kafka to PostgreSQL:

```bash
kubectl apply -f config/samples/kafka-to-postgres.yaml
```

Check status:

```bash
kubectl get dataflow kafka-to-postgres
kubectl describe dataflow kafka-to-postgres
```

### Local Development

For local development and testing:

```bash
# Start dependencies (Kafka, PostgreSQL, RabbitMQ, Iceberg)
docker-compose up -d

# Run operator locally
make run
```

## Architecture

The operator consists of the following components:

### CRD (Custom Resource Definitions)

Defines the schema for the `DataFlow` resource, which describes the data flow configuration, including source, sink, and list of transformations.

### Controller

Kubernetes controller that monitors changes to `DataFlow` resources and manages their lifecycle. The controller creates and manages processors for each active data flow.

### Connectors

Modular connector system for various data sources and sinks. Each connector implements a standard interface for reading or writing data.

### Transformers

Message transformation modules that are applied sequentially to each message in the order specified in the configuration.

### Processor

Message processing orchestrator that coordinates the work of source, transformations, and sink. Handles errors, maintains statistics, and manages the data flow lifecycle.

## Supported Sources and Sinks

### Kafka

- Consumer groups support for scaling
- TLS and SASL authentication
- Initial read position configuration
- Message key support

### PostgreSQL

- Custom SQL queries for sources
- Periodic polling with configurable interval
- Batch inserts for improved performance
- Automatic table creation
- JSONB support for flexible schema

### RabbitMQ

- Exchange and routing key support
- Direct queue publishing
- Reliable message delivery

### Iceberg

- REST API integration
- Automatic namespace and table creation
- Authentication token support
- Optimized for big data processing

## Security

### Kubernetes Secrets

All connectors support configuration from Kubernetes Secrets through `*SecretRef` fields:

- **Kafka**: brokers, topic, consumerGroup, SASL credentials, TLS certificates
- **PostgreSQL**: connectionString, table
- **Iceberg**: restCatalogUrl, namespace, table, token
- **RabbitMQ**: url, queue, exchange, routingKey

This enables:
- Secure storage of sensitive data
- Centralized credential management
- Secret rotation without changing DataFlow resources
- Access control through Kubernetes RBAC

For more details, see the [Using Kubernetes Secrets](connectors.md#using-kubernetes-secrets) section in the connectors documentation.

## Transformations

All transformations support JSONPath for working with nested data structures.

### Timestamp

Adds a field with timestamp in RFC3339 or custom format.

### Flatten

Expands arrays into separate messages, preserving all parent fields. Useful for processing nested structures.

### Filter

Filters messages based on JSONPath conditions. Supports complex logical expressions.

### Mask

Masks sensitive data, supporting length preservation or full character replacement.

### Router

Routes messages to different sinks based on conditions. Enables complex processing scenarios.

### Select

Selects only specified fields from messages, reducing data size and improving performance.

### Remove

Removes specified fields from messages, useful for data cleanup before sending.

## Monitoring and Status

Each `DataFlow` resource has a status that includes:

- **Phase** - current phase of the data flow (Running, Error, etc.)
- **Message** - additional status information
- **LastProcessedTime** - time of the last processed message
- **ProcessedCount** - number of processed messages
- **ErrorCount** - number of errors

## Documentation

- [Getting Started](getting-started.md) - detailed getting started guide
- [Connectors](connectors.md) - detailed description of all connectors
- [Transformations](transformations.md) - detailed transformation descriptions with examples
- [Examples](examples.md) - practical usage examples
- [Development](development.md) - developer guide

## License

Apache License 2.0

