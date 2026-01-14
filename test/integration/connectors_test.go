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

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/dataflow-operator/dataflow/internal/connectors"
	"github.com/dataflow-operator/dataflow/internal/types"
)

// TestKafkaConnectorIntegration тестирует Kafka source и sink коннекторы
func TestKafkaConnectorIntegration(t *testing.T) {
	ctx := context.Background()

	// Запускаем Kafka контейнер с более надежной стратегией ожидания
	// Используем ожидание порта и затем проверяем готовность через подключение
	kafkaContainer, err := kafka.RunContainer(ctx,
		kafka.WithClusterID("test-cluster"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9093/tcp").WithStartupTimeout(120*time.Second),
		),
	)
	require.NoError(t, err)
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate kafka container: %v", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, brokers)

	// Ждем полной готовности Kafka - проверяем доступность через подключение
	maxRetries := 15
	retryDelay := 2 * time.Second
	var kafkaReady bool
	for i := 0; i < maxRetries; i++ {
		adminConfig := sarama.NewConfig()
		adminConfig.Version = sarama.V2_8_0_0
		adminConfig.Net.DialTimeout = 5 * time.Second
		adminConfig.Net.ReadTimeout = 5 * time.Second
		admin, testErr := sarama.NewClusterAdmin(brokers, adminConfig)
		if testErr == nil {
			// Проверяем, что можем получить метаданные
			_, testErr = admin.DescribeTopics([]string{})
			if testErr == nil {
				admin.Close()
				kafkaReady = true
				break
			}
			admin.Close()
		}
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}
	require.True(t, kafkaReady, "Kafka is not ready after %d retries", maxRetries)

	topic := "test-topic"
	consumerGroup := "test-group"

	// Создаем топик с retry логикой
	adminConfig2 := sarama.NewConfig()
	adminConfig2.Version = sarama.V2_8_0_0
	adminConfig2.Net.DialTimeout = 10 * time.Second
	admin2, err := sarama.NewClusterAdmin(brokers, adminConfig2)
	require.NoError(t, err)
	defer admin2.Close()

	// Пробуем создать топик несколько раз
	for i := 0; i < 5; i++ {
		err = admin2.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err == nil {
			break
		}
		// Проверяем, если топик уже существует - это нормально
		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "already exists") || strings.Contains(errStr, "TopicExistsException") {
				// Топик уже существует - это нормально
				err = nil
				break
			}
		}
		if i < 4 {
			time.Sleep(2 * time.Second)
		}
	}
	// Игнорируем ошибку, если топик уже существует
	if err != nil {
		t.Logf("Note: topic creation returned error (may already exist): %v", err)
	}

	t.Run("Kafka Source Connector", func(t *testing.T) {
		// Создаем source коннектор
		sourceSpec := &v1.KafkaSourceSpec{
			Brokers:       brokers,
			Topic:         topic,
			ConsumerGroup: consumerGroup,
		}
		sourceConnector := connectors.NewKafkaSourceConnector(sourceSpec)

		// Подключаемся
		err := sourceConnector.Connect(ctx)
		require.NoError(t, err)
		defer sourceConnector.Close()

		// Отправляем тестовое сообщение в Kafka
		producerConfig := sarama.NewConfig()
		producerConfig.Producer.Return.Successes = true
		producerConfig.Version = sarama.V2_8_0_0
		producer, err := sarama.NewSyncProducer(brokers, producerConfig)
		require.NoError(t, err)
		defer producer.Close()

		testMessage := map[string]interface{}{
			"id":   1,
			"name": "test",
			"data": "test data",
		}
		messageBytes, err := json.Marshal(testMessage)
		require.NoError(t, err)

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(messageBytes),
		})
		require.NoError(t, err)

		// Читаем сообщение через source коннектор
		msgChan, err := sourceConnector.Read(ctx)
		require.NoError(t, err)

		// Ждем сообщение с таймаутом
		select {
		case msg := <-msgChan:
			require.NotNil(t, msg)
			var receivedData map[string]interface{}
			err = json.Unmarshal(msg.Data, &receivedData)
			require.NoError(t, err)
			assert.Equal(t, float64(1), receivedData["id"])
			assert.Equal(t, "test", receivedData["name"])
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})

	t.Run("Kafka Sink Connector", func(t *testing.T) {
		sinkTopic := "sink-topic"
		err := admin2.CreateTopic(sinkTopic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		// Игнорируем ошибку, если топик уже существует
		if err != nil {
			errStr := err.Error()
			if !strings.Contains(errStr, "already exists") && !strings.Contains(errStr, "TopicExistsException") {
				require.NoError(t, err)
			}
		}

		// Создаем sink коннектор
		sinkSpec := &v1.KafkaSinkSpec{
			Brokers: brokers,
			Topic:   sinkTopic,
		}
		sinkConnector := connectors.NewKafkaSinkConnector(sinkSpec)

		// Подключаемся
		err = sinkConnector.Connect(ctx)
		require.NoError(t, err)
		defer sinkConnector.Close()

		// Создаем сообщение для записи
		testMessage := map[string]interface{}{
			"id":   2,
			"name": "sink test",
		}
		messageBytes, err := json.Marshal(testMessage)
		require.NoError(t, err)
		msg := types.NewMessage(messageBytes)

		// Записываем сообщение
		msgChan := make(chan *types.Message, 1)
		msgChan <- msg
		close(msgChan)

		err = sinkConnector.Write(ctx, msgChan)
		require.NoError(t, err)

		// Проверяем, что сообщение записалось
		consumerConfig := sarama.NewConfig()
		consumerConfig.Version = sarama.V2_8_0_0
		consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		consumer, err := sarama.NewConsumer(brokers, consumerConfig)
		require.NoError(t, err)
		defer consumer.Close()

		partitionConsumer, err := consumer.ConsumePartition(sinkTopic, 0, sarama.OffsetOldest)
		require.NoError(t, err)
		defer partitionConsumer.Close()

		select {
		case kafkaMsg := <-partitionConsumer.Messages():
			var receivedData map[string]interface{}
			err = json.Unmarshal(kafkaMsg.Value, &receivedData)
			require.NoError(t, err)
			assert.Equal(t, float64(2), receivedData["id"])
			assert.Equal(t, "sink test", receivedData["name"])
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})
}

// TestPostgreSQLConnectorIntegration тестирует PostgreSQL source и sink коннекторы
func TestPostgreSQLConnectorIntegration(t *testing.T) {
	ctx := context.Background()

	// Запускаем PostgreSQL контейнер
	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	// Получаем connection string
	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	tableName := "test_table"

	// Создаем таблицу и вставляем тестовые данные
	// Пробуем подключиться несколько раз с retry, так как контейнер может быть еще не полностью готов
	var conn *pgx.Conn
	maxRetries := 10
	retryDelay := 500 * time.Millisecond
	for i := 0; i < maxRetries; i++ {
		conn, err = pgx.Connect(ctx, connStr)
		if err == nil {
			// Проверяем подключение через ping
			if pingErr := conn.Ping(ctx); pingErr == nil {
				break
			}
			conn.Close(ctx)
			conn = nil
		}
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
			retryDelay *= 2 // exponential backoff
		}
	}
	require.NoError(t, err, "failed to connect to PostgreSQL after %d retries", maxRetries)
	require.NotNil(t, conn, "connection is nil")
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			value INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName))
	require.NoError(t, err)

	_, err = conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (name, value) VALUES
		('test1', 100),
		('test2', 200),
		('test3', 300)
	`, tableName))
	require.NoError(t, err)

	t.Run("PostgreSQL Source Connector", func(t *testing.T) {
		pollInterval := int32(1)
		sourceSpec := &v1.PostgreSQLSourceSpec{
			ConnectionString: connStr,
			Table:            tableName,
			PollInterval:     &pollInterval,
		}
		sourceConnector := connectors.NewPostgreSQLSourceConnector(sourceSpec)

		err := sourceConnector.Connect(ctx)
		require.NoError(t, err)
		defer sourceConnector.Close()

		msgChan, err := sourceConnector.Read(ctx)
		require.NoError(t, err)

		// Читаем сообщения
		messages := make([]*types.Message, 0)
		timeout := time.After(5 * time.Second)
		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					goto done
				}
				messages = append(messages, msg)
			case <-timeout:
				goto done
			}
		}
	done:
		require.GreaterOrEqual(t, len(messages), 3, "должно быть прочитано минимум 3 сообщения")

		// Проверяем содержимое первого сообщения
		var data map[string]interface{}
		err = json.Unmarshal(messages[0].Data, &data)
		require.NoError(t, err)
		assert.Contains(t, data, "name")
		assert.Contains(t, data, "value")
	})

	t.Run("PostgreSQL Sink Connector", func(t *testing.T) {
		sinkTable := "sink_table"
		_, err = conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id INTEGER,
				name VARCHAR(100),
				value INTEGER
			)
		`, sinkTable))
		require.NoError(t, err)

		sinkSpec := &v1.PostgreSQLSinkSpec{
			ConnectionString: connStr,
			Table:            sinkTable,
		}
		sinkConnector := connectors.NewPostgreSQLSinkConnector(sinkSpec)

		err = sinkConnector.Connect(ctx)
		require.NoError(t, err)
		defer sinkConnector.Close()

		// Создаем сообщения для записи
		testMessages := []map[string]interface{}{
			{"id": 1, "name": "sink1", "value": 10},
			{"id": 2, "name": "sink2", "value": 20},
		}

		msgChan := make(chan *types.Message, len(testMessages))
		for _, testMsg := range testMessages {
			msgBytes, err := json.Marshal(testMsg)
			require.NoError(t, err)
			msgChan <- types.NewMessage(msgBytes)
		}
		close(msgChan)

		err = sinkConnector.Write(ctx, msgChan)
		require.NoError(t, err)

		// Проверяем, что данные записались
		var count int
		err = conn.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", sinkTable)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}

// TestNessieConnectorIntegration тестирует Nessie source и sink коннекторы
// ВАЖНО: Этот тест требует наличия реальных сервисов:
// - Nessie сервер (например, http://localhost:19120)
// - S3-совместимое хранилище (например, MinIO)
// - Правильно настроенные переменные окружения для S3 (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, etc.)
//
// Тест будет пропущен, если сервисы недоступны.
// Для запуска теста необходимо:
// 1. Запустить Nessie сервер
// 2. Настроить S3 хранилище (MinIO или AWS S3)
// 3. Установить переменные окружения для S3
// 4. Запустить тест: go test ./test/integration/... -v -run TestNessieConnectorIntegration
func TestNessieConnectorIntegration(t *testing.T) {
	// Проверяем наличие необходимых переменных окружения
	nessieURL := os.Getenv("NESSIE_URL")
	if nessieURL == "" {
		nessieURL = "http://localhost:19120" // Значение по умолчанию
	}

	s3Endpoint := os.Getenv("AWS_ENDPOINT_URL_S3")
	s3AccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	s3SecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	// Пропускаем тест, если нет S3 credentials
	if s3AccessKey == "" || s3SecretKey == "" {
		t.Skip("Skipping Nessie integration test: S3 credentials not provided (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
	}

	ctx := context.Background()

	// TODO: Добавить проверку доступности Nessie сервера
	// TODO: Добавить проверку доступности S3 хранилища

	t.Run("Nessie Sink Connector - FileIO validation", func(t *testing.T) {
		// Этот тест проверяет, что writeBatch корректно обрабатывает ситуацию,
		// когда FileIO factory может вернуть nil при коммите.
		// Это критический тест для проверки исправления паники "nil pointer dereference"
		// в table.doCommit.

		sinkSpec := &v1.NessieSinkSpec{
			NessieURL: nessieURL,
			Namespace: "test_namespace",
			Table:     "test_table_fileio_validation",
			// S3 настройки будут взяты из переменных окружения
		}

		// Если указан endpoint, добавляем его в конфигурацию
		if s3Endpoint != "" {
			// Note: В текущей реализации S3 настройки берутся из переменных окружения
			// Если в будущем добавится поддержка конфигурации через spec, можно будет использовать:
			// sinkSpec.S3Endpoint = s3Endpoint
		}

		sinkConnector := connectors.NewNessieSinkConnector(sinkSpec)

		// Подключаемся
		err := sinkConnector.Connect(ctx)
		if err != nil {
			t.Skipf("Skipping test: failed to connect to Nessie: %v", err)
		}
		defer sinkConnector.Close()

		// Создаем сообщения для записи
		testMessages := []map[string]interface{}{
			{"id": 1, "name": "test1", "value": 10},
			{"id": 2, "name": "test2", "value": 20},
		}

		msgChan := make(chan *types.Message, len(testMessages))
		for _, testMsg := range testMessages {
			msgBytes, err := json.Marshal(testMsg)
			require.NoError(t, err)
			msgChan <- types.NewMessage(msgBytes)
		}
		close(msgChan)

		// Записываем сообщения
		// Этот вызов должен пройти без паники, даже если FileIO factory
		// потенциально может вернуть nil при коммите
		err = sinkConnector.Write(ctx, msgChan)

		// Проверяем, что не было паники
		// Если была паника, тест упадет с panic
		// Если была ошибка (но не паника), это нормально для интеграционного теста
		if err != nil {
			// Проверяем, что ошибка не связана с nil pointer
			errMsg := err.Error()
			assert.NotContains(t, errMsg, "nil pointer",
				"Ошибка не должна быть связана с nil pointer - это указывает на проблему с FileIO")
			assert.NotContains(t, errMsg, "invalid memory address",
				"Ошибка не должна быть связана с invalid memory address - это указывает на nil pointer dereference")

			// Логируем ошибку для отладки, но не падаем тест
			t.Logf("Write returned error (expected in some cases): %v", err)
		} else {
			t.Log("Successfully wrote messages to Nessie table")
		}
	})

	// TODO: Добавить тест для Nessie Source Connector
}
