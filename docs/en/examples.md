# Examples

Practical examples of using DataFlow Operator for various data processing scenarios.

> **Note**: This is a simplified English version. For complete documentation, see the [Russian version](../ru/examples.md).

## Simple Kafka → PostgreSQL Flow

Basic example of transferring data from a Kafka topic to a PostgreSQL table.

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-postgres
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - localhost:9092
      topic: input-topic
      consumerGroup: dataflow-group
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
      table: output_table
      autoCreateTable: true
```

**Apply:**
```bash
kubectl apply -f config/samples/kafka-to-postgres.yaml
```

## Using Secrets for Credentials

DataFlow Operator supports configuring connectors from Kubernetes Secrets through `*SecretRef` fields. This allows secure storage of credentials without explicitly specifying them in the DataFlow specification.

### Example: Kafka → PostgreSQL with Secrets

#### Step 1: Create Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: default
type: Opaque
stringData:
  brokers: "kafka1:9092,kafka2:9092"
  topic: "input-topic"
  consumerGroup: "dataflow-group"
  username: "kafka-user"
  password: "kafka-password"
---
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: default
type: Opaque
stringData:
  connectionString: "postgres://user:password@postgres:5432/dbname?sslmode=disable"
  table: "output_table"
```

#### Step 2: DataFlow with SecretRef

```yaml
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: secure-dataflow
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
        mechanism: scram-sha-256
        usernameSecretRef:
          name: kafka-credentials
          key: username
        passwordSecretRef:
          name: kafka-credentials
          key: password
  sink:
    type: postgresql
    postgresql:
      connectionStringSecretRef:
        name: postgres-credentials
        key: connectionString
      tableSecretRef:
        name: postgres-credentials
        key: table
      autoCreateTable: true
```

**Apply:**
```bash
kubectl apply -f config/samples/kafka-to-postgres-secrets.yaml
```

### Example: TLS Certificates from Secrets

For TLS configuration, the operator automatically determines whether the value from the secret is a file path or certificate content.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-tls-certs
type: Opaque
stringData:
  ca.crt: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-tls-secure
spec:
  source:
    type: kafka
    kafka:
      brokers:
        - secure-kafka:9093
      topic: secure-topic
      tls:
        caSecretRef:
          name: kafka-tls-certs
          key: ca.crt
```

### Benefits of Using SecretRef

- **Security**: Credentials are not stored in the DataFlow specification
- **Management**: Centralized credential management through Kubernetes
- **Rotation**: Update secrets without changing DataFlow resources
- **RBAC**: Access control through Kubernetes RBAC

For more details, see the [Using Kubernetes Secrets](../ru/connectors.md#использование-secrets-в-kubernetes) section in the connectors documentation.

For more examples, see the [Russian version](../ru/examples.md) or check `config/samples/` directory.

