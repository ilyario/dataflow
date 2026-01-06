# Connectors

DataFlow Operator supports various connectors for data sources and sinks. Each connector implements a standard interface and can be used as a data source (source) or sink.

## Connector Overview

| Connector | Source | Sink | Features |
|-----------|--------|------|----------|
| Kafka | ✅ | ✅ | Consumer groups, TLS, SASL |
| PostgreSQL | ✅ | ✅ | SQL queries, batch inserts, auto-create tables |
| RabbitMQ | ✅ | ✅ | Exchanges, routing keys, queues |
| Iceberg | ✅ | ✅ | REST API, auto-create namespace/tables |

> **Note**: This is a simplified English version. For complete documentation, see the [Russian version](../ru/connectors.md) or refer to the code examples in `config/samples/`.

## Using Kubernetes Secrets

DataFlow Operator supports configuring connectors from Kubernetes Secrets. This allows secure storage of sensitive data (passwords, tokens, connection strings) without explicitly specifying them in the DataFlow specification.

### Overview

Instead of specifying values directly in the configuration, you can use references to Kubernetes Secrets through `*SecretRef` fields. The operator automatically reads values from secrets and substitutes them into connector configuration.

### SecretRef Structure

Each secret reference has the following structure:

```yaml
secretRef:
  name: my-secret          # Secret name (required)
  namespace: my-namespace  # Secret namespace (optional, defaults to DataFlow namespace)
  key: my-key              # Key in secret (required)
```

### Supported Fields

All connectors support secret references for the following fields:

#### Kafka
- `brokersSecretRef` - broker list (comma-separated)
- `topicSecretRef` - topic name
- `consumerGroupSecretRef` - consumer group
- `sasl.usernameSecretRef` - SASL username
- `sasl.passwordSecretRef` - SASL password
- `tls.certSecretRef` - client certificate
- `tls.keySecretRef` - private key
- `tls.caSecretRef` - CA certificate

#### PostgreSQL
- `connectionStringSecretRef` - connection string
- `tableSecretRef` - table name

#### Iceberg
- `restCatalogUrlSecretRef` - REST Catalog URL
- `namespaceSecretRef` - catalog namespace
- `tableSecretRef` - table name
- `tokenSecretRef` - authentication token

#### RabbitMQ
- `urlSecretRef` - connection URL
- `queueSecretRef` - queue name
- `exchangeSecretRef` - exchange name
- `routingKeySecretRef` - routing key

### Usage Examples

#### Example 1: Kafka with SASL Authentication

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: default
type: Opaque
stringData:
  brokers: "kafka1:9092,kafka2:9092,kafka3:9092"
  topic: "input-topic"
  consumerGroup: "dataflow-group"
  username: "kafka-user"
  password: "kafka-password"
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-example
spec:
  source:
    type: kafka
    kafka:
      brokersSecretRef:
        name: kafka-credentials
        key: brokers
      topicSecretRef:
        name: kafka-credentials
        key: topic
      consumerGroupSecretRef:
        name: kafka-credentials
        key: consumerGroup
      sasl:
        mechanism: "scram-sha-256"
        usernameSecretRef:
          name: kafka-credentials
          key: username
        passwordSecretRef:
          name: kafka-credentials
          key: password
```

#### Example 2: PostgreSQL with Connection String from Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: default
type: Opaque
stringData:
  connectionString: "postgres://user:password@postgres:5432/dbname?sslmode=disable"
  table: "output_table"
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: postgres-example
spec:
  source:
    type: postgresql
    postgresql:
      connectionStringSecretRef:
        name: postgres-credentials
        key: connectionString
      tableSecretRef:
        name: postgres-credentials
        key: table
```

### Priority of Values

If both a direct value and `SecretRef` are specified, `SecretRef` takes priority. The value from the secret will be used instead of the direct value.

### Secrets in Different Namespaces

By default, the operator looks for secrets in the same namespace where the DataFlow resource is located. You can specify a different namespace:

```yaml
connectionStringSecretRef:
  name: postgres-credentials
  namespace: shared-secrets
  key: connectionString
```

### Security

- **RBAC**: The operator requires permissions to read secrets (`get`, `list`, `watch`)
- **Isolation**: Secrets should be in the same namespace or in a namespace the operator has access to
- **Temporary Files**: For TLS certificates, the operator creates temporary files that are automatically deleted

### Troubleshooting

#### Secret Not Found

```bash
# Check if secret exists
kubectl get secret <secret-name> -n <namespace>

# Check operator permissions
kubectl auth can-i get secrets --as=system:serviceaccount:default:dataflow-operator
```

#### Key Not Found in Secret

```bash
# Check keys in secret
kubectl get secret <secret-name> -n <namespace> -o jsonpath='{.data}' | jq 'keys'
```

#### Secret Resolution Error

Check operator logs:

```bash
kubectl logs -l app.kubernetes.io/name=dataflow-operator | grep -i secret
```

Ensure:
1. Secret exists in the specified namespace
2. The specified key exists in the secret
3. The operator has permissions to read secrets

For complete connector documentation, see the [Russian version](../ru/connectors.md).

