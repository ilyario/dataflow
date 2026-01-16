# DataFlow Operator

Kubernetes operator for streaming data between different data sources with support for message transformations.

## Overview

DataFlow Operator allows you to declaratively define data flows between different sources and sinks through Kubernetes Custom Resource Definitions (CRD). The operator automatically manages the lifecycle of data flows, processes messages, and applies necessary transformations.

## Quick Start

### Installing the Operator

```bash
# Install operator via Helm from OCI registry
helm install dataflow-operator oci://ghcr.io/ilyario/helm-charts/dataflow-operator

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

## Documentation

Documentation is available in two languages:

- **[English Documentation](en/index.md)** - Complete English documentation
- **[Русская Документация](ru/index.md)** - Полная русская документация

## Key Features

### Multiple Data Source Support

- **Kafka** - read and write messages from/to Kafka topics with TLS and SASL authentication support
- **PostgreSQL** - read from tables and write to PostgreSQL tables with support for custom SQL queries and batch inserts

### Rich Transformation Set

- **Timestamp** - add timestamp to each message
- **Flatten** - expand arrays into separate messages while preserving parent fields
- **Filter** - filter messages based on JSONPath conditions
- **Mask** - mask sensitive data with or without preserving length
- **Router** - route messages to different sinks based on conditions
- **Select** - select specific fields from messages
- **Remove** - remove specified fields from messages

### Secure Configuration

Support for configuring connectors from Kubernetes Secrets through `SecretRef` allows secure storage of credentials, tokens, and connection strings without explicitly specifying them in the DataFlow specification.

## License

Apache License 2.0
