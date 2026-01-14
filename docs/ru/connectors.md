# Connectors

DataFlow Operator поддерживает различные коннекторы для источников и приемников данных. Каждый коннектор реализует стандартный интерфейс и может использоваться как источник (source) или приемник (sink) данных.

## Обзор коннекторов

| Коннектор | Источник | Приемник | Особенности |
|-----------|----------|----------|-------------|
| Kafka | ✅ | ✅ | Consumer groups, TLS, SASL, Avro, Schema Registry |
| PostgreSQL | ✅ | ✅ | SQL запросы, батч-вставки, автосоздание таблиц |
| Iceberg | ✅ | ✅ | REST API, автосоздание namespace/таблиц |
| Nessie | ✅ | ✅ | Транзакционный каталог, S3 backend, автосоздание таблиц |

## Kafka

Kafka коннектор поддерживает чтение и запись сообщений из/в топики Apache Kafka. Поддерживает consumer groups для масштабирования, TLS и SASL аутентификацию, а также работу с Avro форматом через Confluent Schema Registry или статическую схему.

### Источник (Source)

Конфигурация Kafka источника:

```yaml
source:
  type: kafka
  kafka:
    # Список брокеров Kafka (обязательно)
    brokers:
      - kafka1:9092
      - kafka2:9092
      - kafka3:9092

    # Топик для чтения (обязательно)
    topic: input-topic

    # Consumer group (опционально, по умолчанию: dataflow-operator)
    consumerGroup: my-group

    # TLS конфигурация (опционально)
    tls:
      # Пропустить проверку сертификата (не рекомендуется для production)
      insecureSkipVerify: false
      # Путь к CA сертификату
      caFile: /path/to/ca.crt
      # Путь к клиентскому сертификату
      certFile: /path/to/client.crt
      # Путь к приватному ключу
      keyFile: /path/to/client.key

    # SASL аутентификация (опционально)
    sasl:
      # Механизм: plain, scram-sha-256, scram-sha-512
      mechanism: scram-sha-256
      username: kafka-user
      password: kafka-password

    # Формат сообщений (опционально, по умолчанию: "json")
    # Поддерживаемые форматы: "json", "avro"
    format: json

    # Конфигурация Avro (требуется если format: "avro")
    # Вариант 1: Использование Confluent Schema Registry (рекомендуется)
    schemaRegistry:
      # URL Schema Registry (обязательно)
      url: https://schema-registry:8081
      # Basic Auth для Schema Registry (опционально)
      basicAuth:
        username: schema-user
        password: schema-password
      # TLS конфигурация для Schema Registry (опционально)
      tls:
        insecureSkipVerify: false
        caFile: /path/to/schema-registry-ca.crt

    # Вариант 2: Статическая Avro схема (альтернатива Schema Registry)
    # avroSchema: |
    #   {
    #     "type": "record",
    #     "name": "MyRecord",
    #     "fields": [
    #       {"name": "id", "type": "long"},
    #       {"name": "name", "type": "string"}
    #     ]
    #   }
    # Или путь к файлу со схемой:
    # avroSchemaFile: /path/to/schema.avsc
```

#### Особенности Kafka источника

- **Consumer Groups**: Используйте разные consumer groups для масштабирования обработки
- **Начальная позиция**: По умолчанию читает с самого старого сообщения (`OffsetOldest`)
- **Форматы данных**: Поддерживает JSON (по умолчанию) и Avro форматы
- **Avro поддержка**:
  - **Confluent Schema Registry**: Автоматическое получение схем по ID из сообщений (формат: magic byte + schema ID + data)
  - **Статическая схема**: Использование предопределенной схемы из конфигурации или файла
  - **Кэширование схем**: Схемы из Schema Registry кэшируются для повышения производительности
- **Метаданные**: Каждое сообщение содержит метаданные:
  - `topic` - название топика
  - `partition` - номер партиции
  - `offset` - смещение сообщения
  - `key` - ключ сообщения (если есть)

#### Пример с TLS и SASL

```yaml
source:
  type: kafka
  kafka:
    brokers:
      - secure-kafka:9093
    topic: secure-topic
    consumerGroup: secure-group
    tls:
      caFile: /etc/kafka/ca.crt
      certFile: /etc/kafka/client.crt
      keyFile: /etc/kafka/client.key
    sasl:
      mechanism: scram-sha-512
      username: kafka-user
      password: ${KAFKA_PASSWORD}  # Используйте Secrets в Kubernetes
```

#### Пример с Avro и Schema Registry

```yaml
source:
  type: kafka
  kafka:
    brokers:
      - kafka:9092
    topic: avro-topic
    consumerGroup: avro-group
    format: avro
    schemaRegistry:
      url: https://schema-registry:8081
      basicAuth:
        username: schema-user
        password: schema-password
      tls:
        insecureSkipVerify: true  # Для self-signed сертификатов
        # caFile: /path/to/ca.crt  # Или укажите CA сертификат
```

#### Пример с Avro и статической схемой

```yaml
source:
  type: kafka
  kafka:
    brokers:
      - kafka:9092
    topic: avro-topic
    consumerGroup: avro-group
    format: avro
    avroSchema: |
      {
        "type": "record",
        "name": "Stock",
        "namespace": "com.example",
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "symbol", "type": "string"},
          {"name": "price", "type": "double"},
          {"name": "quantity", "type": "int"}
        ]
      }
    # Или используйте файл:
    # avroSchemaFile: /path/to/schema.avsc
```

### Приемник (Sink)

Конфигурация Kafka приемника:

```yaml
sink:
  type: kafka
  kafka:
    # Список брокеров Kafka (обязательно)
    brokers:
      - kafka1:9092

    # Топик для записи (обязательно)
    topic: output-topic

    # TLS конфигурация (опционально, аналогично источнику)
    tls:
      caFile: /path/to/ca.crt
      certFile: /path/to/client.crt
      keyFile: /path/to/client.key

    # SASL конфигурация (опционально, аналогично источнику)
    sasl:
      mechanism: scram-sha-256
      username: kafka-user
      password: kafka-password
```

#### Особенности Kafka приемника

- **Синхронная запись**: Использует `SyncProducer` для гарантированной доставки
- **Ключи сообщений**: Сохраняет ключи из метаданных сообщения
- **Метаданные**: После записи обновляет метаданные с `partition` и `offset`

## PostgreSQL

PostgreSQL коннектор поддерживает чтение из таблиц и запись в таблицы PostgreSQL. Поддерживает кастомные SQL запросы, периодический опрос и батч-вставки.

### Источник (Source)

Конфигурация PostgreSQL источника:

```yaml
source:
  type: postgresql
  postgresql:
    # Connection string (обязательно)
    # Формат: postgres://user:password@host:port/dbname?sslmode=mode
    connectionString: "postgres://user:password@localhost:5432/dbname?sslmode=disable"

    # Таблица для чтения (обязательно, если не указан query)
    table: source_table

    # Кастомный SQL запрос (опционально)
    # Если указан, используется вместо чтения всей таблицы
    query: "SELECT * FROM source_table WHERE updated_at > NOW() - INTERVAL '1 hour'"

    # Интервал опроса в секундах (опционально, по умолчанию: 5)
    # Используется для периодического чтения новых данных
    pollInterval: 60
```

#### Особенности PostgreSQL источника

- **Периодический опрос**: Регулярно опрашивает таблицу на наличие новых данных
- **Кастомные запросы**: Поддержка сложных SQL запросов с JOIN, WHERE, и т.д.
- **Метаданные**: Каждое сообщение содержит метаданные:
  - `table` - название таблицы
- **Формат данных**: Данные преобразуются в JSON формат

#### Пример с кастомным запросом

```yaml
source:
  type: postgresql
  postgresql:
    connectionString: "postgres://user:password@localhost:5432/analytics"
    query: |
      SELECT
        u.id,
        u.email,
        o.order_id,
        o.total
      FROM users u
      JOIN orders o ON u.id = o.user_id
      WHERE o.created_at > NOW() - INTERVAL '1 day'
    pollInterval: 300  # Опрос каждые 5 минут
```

### Приемник (Sink)

Конфигурация PostgreSQL приемника:

```yaml
sink:
  type: postgresql
  postgresql:
    # Connection string (обязательно)
    connectionString: "postgres://user:password@localhost:5432/dbname?sslmode=disable"

    # Таблица для записи (обязательно)
    table: target_table

    # Размер батча для вставки (опционально, по умолчанию: 1)
    # Увеличьте для повышения производительности
    batchSize: 100

    # Автоматическое создание таблицы (опционально, по умолчанию: false)
    # Если true, создает таблицу с JSONB полем для гибкой схемы
    autoCreateTable: true
```

#### Особенности PostgreSQL приемника

- **Батч-вставки**: Группирует сообщения для эффективной записи
- **Автосоздание таблиц**: Автоматически создает таблицы с JSONB полем
- **Гибкая схема**: Поддерживает как JSONB (для автосозданных таблиц), так и колоночный формат
- **Индексы**: Автоматически создает GIN индекс на JSONB поле для быстрого поиска

#### Пример с автосозданием таблицы

```yaml
sink:
  type: postgresql
  postgresql:
    connectionString: "postgres://user:password@localhost:5432/analytics"
    table: events
    autoCreateTable: true  # Создаст таблицу с JSONB полем
    batchSize: 50
```

Автоматически созданная таблица будет иметь структуру:

```sql
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_events_data ON events USING GIN (data);
```

## Iceberg

Iceberg коннектор работает с Apache Iceberg через REST API. Оптимизирован для работы с большими данными и поддерживает автоматическое создание namespace и таблиц. Имя каталога можно настроить через параметр `catalogName` (по умолчанию: "iceberg"). Это позволяет использовать несколько каталогов в одном кластере и обеспечивает совместимость с Trino и другими системами.

### Источник (Source)

Конфигурация Iceberg источника:

```yaml
source:
  type: iceberg
  iceberg:
    # URL REST Catalog (обязательно)
    restCatalogUrl: "http://iceberg-rest:8181"

    # Имя каталога (опционально, по умолчанию: "iceberg")
    # Используется для идентификации каталога в Trino и других системах
    # Формат идентификатора таблицы: catalogName.namespace.table
    catalogName: "iceberg"

    # Namespace в каталоге (обязательно)
    namespace: analytics

    # Название таблицы (обязательно)
    table: source_table

    # Токен аутентификации (опционально)
    token: "your-auth-token"
```

#### Особенности Iceberg источника

- **Стриминг чтение**: Использует Arrow RecordBatches для эффективного чтения
- **Поддержка типов**: Автоматически обрабатывает различные типы данных Iceberg
- **Метаданные**: Каждое сообщение содержит:
  - `namespace` - namespace таблицы
  - `table` - название таблицы

#### Требования для работы

Для работы Iceberg коннектора необходимо настроить доступ к объектному хранилищу (S3/MinIO). Это можно сделать двумя способами:

**1. Через секреты Kubernetes (рекомендуется):**

```yaml
sink:
  type: iceberg
  iceberg:
    restCatalogUrl: "http://iceberg-rest:8181"
    namespace: analytics
    table: target_table
    awsRegionSecretRef:
      name: s3-credentials
      namespace: dataflow
      key: AWS_REGION
    awsAccessKeyIDSecretRef:
      name: s3-credentials
      namespace: dataflow
      key: AWS_ACCESS_KEY_ID
    awsSecretAccessKeySecretRef:
      name: s3-credentials
      namespace: dataflow
      key: AWS_SECRET_ACCESS_KEY
    awsEndpointURLSecretRef:
      name: s3-credentials
      namespace: dataflow
      key: AWS_ENDPOINT_URL_S3
```

**2. Через переменные окружения в поде:**

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_ENDPOINT_URL_S3=http://localhost:9000  # Для MinIO
```

> **Примечание:** REST catalog управляет только метаданными (схемы, партиции). Файлы данных записываются напрямую в S3/MinIO через AWS SDK, поэтому необходимы AWS credentials.

### Приемник (Sink)

Конфигурация Iceberg приемника:

```yaml
sink:
  type: iceberg
  iceberg:
    # URL REST Catalog (обязательно)
    restCatalogUrl: "http://iceberg-rest:8181"

    # Имя каталога (опционально, по умолчанию: "iceberg")
    # Используется для идентификации каталога в Trino и других системах
    # Формат идентификатора таблицы: catalogName.namespace.table
    catalogName: "iceberg"

    # Namespace в каталоге (обязательно)
    namespace: analytics

    # Название таблицы (обязательно)
    table: target_table

    # Токен аутентификации (опционально)
    token: "your-auth-token"

    # Автоматическое создание namespace (опционально, по умолчанию: true)
    autoCreateNamespace: true

    # Автоматическое создание таблицы (опционально, по умолчанию: false)
    # Если true, создает таблицу с полем "data" типа String
    autoCreateTable: true
```

#### Особенности Iceberg приемника

- **Батч-запись**: Группирует сообщения в батчи для эффективной записи
- **Автосоздание**: Автоматически создает namespace и таблицы при необходимости
- **Обработка конфликтов**: Автоматически обрабатывает конфликты при параллельных записях
- **Arrow формат**: Использует Apache Arrow для эффективной сериализации данных

#### Пример с автосозданием и секретами

```yaml
sink:
  type: iceberg
  iceberg:
    restCatalogUrl: "http://iceberg-rest:8181"
    # Имя каталога (опционально, по умолчанию: "iceberg")
    # Используется для идентификации каталога в Trino и других системах
    # Формат идентификатора таблицы: catalogName.namespace.table
    catalogName: "iceberg"
    namespace: analytics
    table: events
    tokenSecretRef:
      name: iceberg-credentials
      namespace: dataflow
      key: token
    # AWS credentials для записи данных в S3
    awsRegionSecretRef:
      name: s3-credentials
      namespace: dataflow
      key: aws-region
    awsAccessKeyIDSecretRef:
      name: s3-credentials
      namespace: dataflow
      key: aws-access-key-id
    awsSecretAccessKeySecretRef:
      name: s3-credentials
      namespace: dataflow
      key: aws-secret-access-key
    autoCreateNamespace: true
    autoCreateTable: true
```

Автоматически созданная таблица будет иметь схему с полем `data` типа `String`, в котором хранится JSON представление сообщения.

> **Важно:** REST catalog управляет только метаданными. Файлы данных записываются напрямую в S3, поэтому необходимы AWS credentials через секреты или переменные окружения.

## Использование Secrets в Kubernetes

DataFlow Operator поддерживает конфигурацию коннекторов из Kubernetes Secrets. Это позволяет безопасно хранить чувствительные данные (пароли, токены, connection strings) без их явного указания в спецификации DataFlow.

### Обзор

Вместо указания значений напрямую в конфигурации, вы можете использовать ссылки на Kubernetes Secrets через поля `*SecretRef`. Оператор автоматически читает значения из secrets и подставляет их в конфигурацию коннекторов.

### Структура SecretRef

Каждая ссылка на secret имеет следующую структуру:

```yaml
secretRef:
  name: my-secret          # Имя secret (обязательно)
  namespace: my-namespace  # Namespace secret (опционально, по умолчанию - namespace DataFlow)
  key: my-key              # Ключ в secret (обязательно)
```

### Поддерживаемые поля

Все коннекторы поддерживают ссылки на secrets для следующих полей:

#### Kafka
- `brokersSecretRef` - список брокеров (через запятую)
- `topicSecretRef` - название топика
- `consumerGroupSecretRef` - consumer group
- `sasl.usernameSecretRef` - имя пользователя SASL
- `sasl.passwordSecretRef` - пароль SASL
- `tls.certSecretRef` - сертификат клиента
- `tls.keySecretRef` - приватный ключ
- `tls.caSecretRef` - CA сертификат
- `schemaRegistry.urlSecretRef` - URL Schema Registry
- `schemaRegistry.basicAuth.usernameSecretRef` - имя пользователя для Schema Registry
- `schemaRegistry.basicAuth.passwordSecretRef` - пароль для Schema Registry
- `avroSchemaSecretRef` - Avro схема из секрета (для статической схемы)

#### PostgreSQL
- `connectionStringSecretRef` - строка подключения
- `tableSecretRef` - название таблицы

#### Iceberg
- `restCatalogUrlSecretRef` - URL REST Catalog
- `namespaceSecretRef` - namespace в каталоге
- `tableSecretRef` - название таблицы
- `tokenSecretRef` - токен аутентификации
- `awsRegionSecretRef` - AWS регион
- `awsAccessKeyIDSecretRef` - AWS access key ID
- `awsSecretAccessKeySecretRef` - AWS secret access key
- `awsEndpointURLSecretRef` - URL endpoint для S3 (опционально, только для MinIO)

#### Nessie
- `nessieUrlSecretRef` - URL сервера Nessie
- `branchSecretRef` - название ветки
- `namespaceSecretRef` - namespace в каталоге
- `tableSecretRef` - название таблицы
- `tokenSecretRef` - токен аутентификации
- `awsRegionSecretRef` - AWS регион
- `awsAccessKeyIDSecretRef` - AWS access key ID
- `awsSecretAccessKeySecretRef` - AWS secret access key
- `awsEndpointURLSecretRef` - URL endpoint для S3 (опционально, только для MinIO)

### Примеры использования

#### Пример 1: Kafka с SASL аутентификацией

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

#### Пример 2: PostgreSQL с connection string из secret

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

#### Пример 3: Kafka → PostgreSQL с secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
type: Opaque
stringData:
  brokers: "localhost:9092"
  topic: "input-topic"
  consumerGroup: "dataflow-group"
---
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
type: Opaque
stringData:
  connectionString: "postgres://dataflow:dataflow@postgres:5432/dataflow?sslmode=disable"
  table: "output_table"
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-to-postgres-secrets
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
  sink:
    type: postgresql
    postgresql:
      connectionStringSecretRef:
        name: postgres-credentials
        key: connectionString
      tableSecretRef:
        name: postgres-credentials
        key: table
```

#### Пример 4: TLS сертификаты из secrets

Для TLS конфигурации оператор автоматически определяет, является ли значение из secret путем к файлу или содержимым сертификата.

**Определение типа значения:**
- Если значение начинается с `-----BEGIN` (например, `-----BEGIN CERTIFICATE-----` или `-----BEGIN PRIVATE KEY-----`), оператор считает его содержимым сертификата и создает временный файл
- Если значение не начинается с `-----BEGIN` и существует как файл в файловой системе, оно используется как путь к файлу
- Если значение не начинается с `-----BEGIN` и файл не существует, оно также обрабатывается как содержимое сертификата

**Поддерживаемые форматы:**
1. **Содержимое сертификата** (PEM формат):
   ```yaml
   ca.crt: |
     -----BEGIN CERTIFICATE-----
     MIIDXTCCAkWgAwIBAgIJAK...
     -----END CERTIFICATE-----
   ```

2. **Base64-кодированное содержимое** (в поле `data` секрета):
   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: kafka-tls-certs
   type: Opaque
   data:
     ca.crt: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t...  # base64
   ```
   Kubernetes автоматически декодирует base64 при чтении из секрета.

3. **Путь к файлу**:
   ```yaml
   ca.crt: /etc/kafka/ca.crt
   ```

**Пример с содержимым сертификата:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-tls-certs
type: Opaque
stringData:
  # Содержимое CA сертификата
  ca.crt: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
  # Содержимое клиентского сертификата
  client.crt: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
  # Содержимое приватного ключа
  client.key: |
    -----BEGIN PRIVATE KEY-----
    ...
    -----END PRIVATE KEY-----
---
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: kafka-tls-example
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
        certSecretRef:
          name: kafka-tls-certs
          key: client.crt
        keySecretRef:
          name: kafka-tls-certs
          key: client.key
```

**Важно:**
- Временные файлы создаются автоматически для содержимого сертификатов и удаляются после использования
- При использовании base64-кодированных значений в поле `data`, Kubernetes автоматически декодирует их при чтении
- Убедитесь, что сертификаты в правильном PEM формате с заголовками `-----BEGIN` и `-----END`

### Приоритет значений

Если указаны и прямое значение, и `SecretRef`, приоритет имеет `SecretRef`. Значение из secret будет использовано вместо прямого значения.

### Secrets в разных namespace

По умолчанию оператор ищет secrets в том же namespace, где находится DataFlow ресурс. Вы можете указать другой namespace:

```yaml
connectionStringSecretRef:
  name: postgres-credentials
  namespace: shared-secrets
  key: connectionString
```

### Безопасность

- **RBAC**: Оператору требуются права на чтение secrets (`get`, `list`, `watch`)
- **Изоляция**: Secrets должны быть в том же namespace или в namespace, к которому у оператора есть доступ
- **Временные файлы**: Для TLS сертификатов оператор создает временные файлы, которые автоматически удаляются после использования
  - Файлы создаются только для содержимого сертификатов (начинающихся с `-----BEGIN`)
  - Файлы удаляются автоматически при завершении работы оператора
  - Временные файлы создаются в системной временной директории с уникальными именами

### Устранение неполадок

#### Secret не найден

```bash
# Проверьте существование secret
kubectl get secret <secret-name> -n <namespace>

# Проверьте права оператора
kubectl auth can-i get secrets --as=system:serviceaccount:default:dataflow-operator
```

#### Ключ не найден в secret

```bash
# Проверьте ключи в secret
kubectl get secret <secret-name> -n <namespace> -o jsonpath='{.data}' | jq 'keys'
```

#### Ошибка разрешения secrets

Проверьте логи оператора:

```bash
kubectl logs -l app.kubernetes.io/name=dataflow-operator | grep -i secret
```

Убедитесь, что:
1. Secret существует в указанном namespace
2. Указанный ключ существует в secret
3. У оператора есть права на чтение secrets

#### Проблемы с TLS сертификатами

Если возникают ошибки при работе с TLS сертификатами:

1. **Ошибка "file name too long"**: Убедитесь, что сертификат в секрете хранится правильно:
   - Если используете `stringData`, сертификат должен быть в PEM формате с заголовками `-----BEGIN` и `-----END`
   - Если используете `data` (base64), убедитесь, что значение корректно закодировано
   - Оператор автоматически определяет содержимое сертификата по префиксу `-----BEGIN`

2. **Ошибка "failed to read CA file"**: Проверьте формат сертификата:
   ```bash
   # Проверьте содержимое секрета
   kubectl get secret <secret-name> -n <namespace> -o jsonpath='{.data.ca\.crt}' | base64 -d
   ```
   Убедитесь, что сертификат начинается с `-----BEGIN CERTIFICATE-----`

3. **Ошибка создания временного файла**: Проверьте права оператора на создание файлов в временной директории

## Рекомендации по производительности

### Kafka

- Используйте несколько брокеров для отказоустойчивости
- Настройте правильный размер consumer group для параллельной обработки
- Используйте батчи при записи для повышения throughput

### PostgreSQL

- Увеличьте `batchSize` для приемника (рекомендуется 50-100)
- Используйте индексы на часто запрашиваемых полях
- Настройте `pollInterval` в зависимости от частоты обновления данных

### Iceberg

- Используйте батчи для записи (автоматически, размер 10 сообщений)
- Настройте правильный размер партиций для ваших данных
- Используйте compaction для оптимизации чтения

## Nessie

Nessie коннектор поддерживает чтение и запись данных из/в Iceberg таблицы через Nessie транзакционный каталог. Nessie предоставляет Git-подобную модель версионирования для метаданных таблиц, а данные хранятся в S3-совместимом хранилище. Коннектор автоматически обрабатывает метаданные через Nessie API и использует AWS SDK для доступа к данным в S3.

### Источник (Source)

Конфигурация Nessie источника:

```yaml
source:
  type: nessie
  nessie:
    # URL сервера Nessie (обязательно)
    nessieUrl: "http://nessie:19120/api/v2"

    # Ветка для чтения (опционально, по умолчанию: "main")
    branch: "main"

    # Namespace в каталоге (обязательно)
    namespace: analytics

    # Название таблицы (обязательно)
    table: source_table

    # Токен аутентификации (опционально)
    token: "your-auth-token"
```

#### Особенности Nessie источника

- **Чтение из Iceberg таблиц**: Использует metadata location из Nessie для загрузки таблиц
- **S3 backend**: Автоматически работает с S3-совместимым хранилищем (AWS S3, MinIO, Yandex Object Storage)
- **Поддержка s3a://**: Автоматически конвертирует s3a:// в s3:// для совместимости с iceberg-go
- **Arrow формат**: Использует Apache Arrow для эффективного чтения данных
- **Ветки**: Поддержка чтения из разных веток Nessie (по умолчанию: "main")
- **Метаданные**: Каждое сообщение содержит:
  - `namespace` - namespace таблицы
  - `table` - название таблицы

#### Требования для работы

Для работы Nessie коннектора необходимо настроить доступ к объектному хранилищу (S3/MinIO). Это можно сделать двумя способами:

**1. Через секреты Kubernetes (рекомендуется):**

```yaml
source:
  type: nessie
  nessie:
    nessieUrl: "http://nessie:19120/api/v2"
    namespace: analytics
    table: source_table
    awsRegionSecretRef:
      name: s3-credentials
      namespace: dataflow
      key: AWS_REGION
    awsAccessKeyIDSecretRef:
      name: s3-credentials
      namespace: dataflow
      key: AWS_ACCESS_KEY_ID
    awsSecretAccessKeySecretRef:
      name: s3-credentials
      namespace: dataflow
      key: AWS_SECRET_ACCESS_KEY
    awsEndpointURLSecretRef:
      name: s3-credentials
      namespace: dataflow
      key: AWS_ENDPOINT_URL_S3
```

**2. Через переменные окружения в поде:**

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_ENDPOINT_URL_S3=http://localhost:9000  # Для MinIO
```

> **Примечание:** Nessie управляет метаданными таблиц (metadata location), а файлы данных хранятся в S3/MinIO. Коннектор использует AWS SDK для доступа к данным, поэтому необходимы AWS credentials.

### Приемник (Sink)

Конфигурация Nessie приемника:

```yaml
sink:
  type: nessie
  nessie:
    # URL сервера Nessie (обязательно)
    nessieUrl: "http://nessie:19120/api/v2"

    # Ветка для записи (опционально, по умолчанию: "main")
    branch: "main"

    # Namespace в каталоге (обязательно)
    namespace: analytics

    # Название таблицы (обязательно)
    table: target_table

    # Токен аутентификации (опционально)
    token: "your-auth-token"

    # Автоматическое создание таблицы (опционально, по умолчанию: false)
    # Если true, создает таблицу с полем "data" типа String
    autoCreateTable: true
```

#### Особенности Nessie приемника

- **Батч-запись**: Группирует сообщения в батчи для эффективной записи
- **Автосоздание**: Автоматически создает таблицы при необходимости
- **Обработка конфликтов**: Автоматически обрабатывает конфликты при параллельных записях с retry механизмом
- **Arrow формат**: Использует Apache Arrow для эффективной сериализации данных
- **S3 backend**: Автоматически работает с S3-совместимым хранилищем
- **Ветки**: Поддержка записи в разные ветки Nessie (по умолчанию: "main")
- **Транзакции**: Использует транзакционную модель Nessie для атомарных операций

#### Пример с автосозданием и секретами

```yaml
sink:
  type: nessie
  nessie:
    nessieUrl: "http://nessie:19120/api/v2"
    namespace: analytics
    table: target_table
    autoCreateTable: true
    # Использование секретов для конфиденциальных данных
    tokenSecretRef:
      name: nessie-credentials
      key: token
    awsRegionSecretRef:
      name: s3-credentials
      key: AWS_REGION
    awsAccessKeyIDSecretRef:
      name: s3-credentials
      key: AWS_ACCESS_KEY_ID
    awsSecretAccessKeySecretRef:
      name: s3-credentials
      key: AWS_SECRET_ACCESS_KEY
    awsEndpointURLSecretRef:
      name: s3-credentials
      key: AWS_ENDPOINT_URL_S3
```

#### Поддерживаемые секреты

Nessie коннектор поддерживает следующие секреты:

- `nessieUrlSecretRef` - URL сервера Nessie
- `branchSecretRef` - название ветки
- `namespaceSecretRef` - namespace в каталоге
- `tableSecretRef` - название таблицы
- `tokenSecretRef` - токен аутентификации
- `awsRegionSecretRef` - AWS регион
- `awsAccessKeyIDSecretRef` - AWS access key ID
- `awsSecretAccessKeySecretRef` - AWS secret access key
- `awsEndpointURLSecretRef` - AWS S3 endpoint URL (для MinIO)

#### Отличия от Iceberg REST Catalog

Nessie коннектор отличается от Iceberg REST Catalog коннектора:

- **Версионирование**: Nessie предоставляет Git-подобную модель версионирования метаданных
- **Транзакции**: Поддержка транзакционных операций с метаданными
- **Ветки**: Работа с ветками и тегами для управления версиями
- **Метаданные**: Метаданные хранятся в Nessie, а не в REST Catalog

## Устранение неполадок

### Проблемы с подключением

1. Проверьте доступность источника данных из кластера
2. Убедитесь, что credentials корректны
3. Проверьте сетевые политики Kubernetes
4. Для TLS проверьте сертификаты

### Проблемы с производительностью

1. Увеличьте размер батчей для приемников
2. Настройте правильный `pollInterval` для источников
3. Используйте несколько инстансов оператора для масштабирования
4. Мониторьте метрики обработки сообщений

### Логирование

Включите детальное логирование для отладки:

```bash
kubectl logs -l app.kubernetes.io/name=dataflow-operator -f --tail=100
```

Проверьте статус DataFlow ресурса:

```bash
kubectl describe dataflow <name>
```
