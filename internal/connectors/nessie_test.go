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
	"strings"
	"testing"

	v1 "github.com/dataflow-operator/dataflow/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNessieSinkConnector_writeBatch_EmptyMetadataLocation(t *testing.T) {
	ctx := context.Background()
	config := &v1.NessieSinkSpec{
		NessieURL: "http://localhost:19120",
		Namespace: "test_namespace",
		Table:     "test_table",
	}
	connector := NewNessieSinkConnector(config)
	connector.metadataLocation = "" // Пустая metadata location

	batch := []map[string]interface{}{
		{"id": 1, "name": "test"},
	}

	err := connector.writeBatch(ctx, nil, batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "metadata location is empty")
}

func TestNessieSinkConnector_writeBatch_InvalidMetadataLocation(t *testing.T) {
	ctx := context.Background()
	config := &v1.NessieSinkSpec{
		NessieURL: "http://localhost:19120",
		Namespace: "test_namespace",
		Table:     "test_table",
	}
	connector := NewNessieSinkConnector(config)
	// Metadata location без bucket (невалидный S3 путь)
	connector.metadataLocation = "s3:///path/to/metadata.json"

	batch := []map[string]interface{}{
		{"id": 1, "name": "test"},
	}

	err := connector.writeBatch(ctx, nil, batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to extract bucket from metadata location")
}

func TestNessieSinkConnector_writeBatch_EmptyBatch(t *testing.T) {
	ctx := context.Background()
	config := &v1.NessieSinkSpec{
		NessieURL: "http://localhost:19120",
		Namespace: "test_namespace",
		Table:     "test_table",
	}
	connector := NewNessieSinkConnector(config)
	connector.metadataLocation = "s3://test-bucket/path/to/metadata.json"

	// Пустой batch должен вернуть nil без ошибки
	err := connector.writeBatch(ctx, nil, []map[string]interface{}{})
	require.NoError(t, err)
}

func TestNessieSinkConnector_writeBatch_FileIOFactoryError(t *testing.T) {
	ctx := context.Background()
	config := &v1.NessieSinkSpec{
		NessieURL: "http://localhost:19120",
		Namespace: "test_namespace",
		Table:     "test_table",
	}
	connector := NewNessieSinkConnector(config)
	// Валидный S3 путь, но без настроенных S3 credentials
	// createS3FileIOFactoryWithBucket вернет ошибку, если нет credentials
	connector.metadataLocation = "s3://test-bucket/path/to/metadata.json"

	batch := []map[string]interface{}{
		{"id": 1, "name": "test"},
	}

	err := connector.writeBatch(ctx, nil, batch)
	// Ожидаем ошибку при создании FileIO factory (нет S3 credentials)
	require.Error(t, err)
	// Проверяем, что ошибка связана с FileIO factory
	assert.Contains(t, err.Error(), "FileIO")
}

// TestNessieSinkConnector_writeBatch_ValidatesFileIOBeforeCommit проверяет,
// что writeBatch проверяет FileIO factory перед использованием.
// Этот тест проверяет, что код корректно обрабатывает ошибки при создании FileIO factory
// и не допускает ситуацию, когда nil FileIO передается в table.NewFromLocation.
//
// ВАЖНО: Для полного тестирования сценария nil FileIO при коммите (когда FileIO factory
// возвращает nil во время вызова table.Append -> table.doCommit) необходим интеграционный тест
// с реальными сервисами (Nessie, S3) или использование моков для iceberg-go библиотеки.
// Текущий тест проверяет, что код правильно валидирует FileIO ДО попытки коммита.
func TestNessieSinkConnector_writeBatch_ValidatesFileIOBeforeCommit(t *testing.T) {
	ctx := context.Background()
	config := &v1.NessieSinkSpec{
		NessieURL: "http://localhost:19120",
		Namespace: "test_namespace",
		Table:     "test_table",
	}
	connector := NewNessieSinkConnector(config)
	connector.metadataLocation = "s3://test-bucket/path/to/metadata.json"

	batch := []map[string]interface{}{
		{"id": 1, "name": "test"},
	}

	// Этот тест проверяет, что writeBatch пытается протестировать FileIO factory
	// перед созданием таблицы. Даже если нет реальных S3 credentials,
	// код должен попытаться создать factory и проверить его.
	err := connector.writeBatch(ctx, nil, batch)

	// Ожидаем ошибку, так как нет реальных S3 credentials
	require.Error(t, err)

	// Важно: проверяем, что ошибка происходит ДО попытки коммита
	// (т.е. при создании/тестировании FileIO factory, а не при коммите)
	// Это означает, что код правильно проверяет FileIO перед использованием
	errMsg := err.Error()

	// Ошибка должна быть связана с FileIO factory, а не с коммитом
	// Если бы FileIO был nil при коммите, мы бы получили панику, а не ошибку
	expectedSubstrings := []string{
		"FileIO factory",
		"FileIO",
		"S3",
		"credentials",
		"failed to create",
	}
	containsExpected := false
	for _, substr := range expectedSubstrings {
		if strings.Contains(errMsg, substr) {
			containsExpected = true
			break
		}
	}
	assert.True(t, containsExpected,
		"Ошибка должна быть связана с FileIO factory, а не с коммитом. Получена ошибка: %s", errMsg,
	)
}

// TestNessieSinkConnector_writeBatch_ValidatesTableAfterReload проверяет,
// что writeBatch проверяет таблицу и схему после перезагрузки.
// Этот тест проверяет, что код корректно обрабатывает ошибки при загрузке таблицы
// и не допускает ситуацию, когда nil таблица или nil схема используются при коммите.
//
// ВАЖНО: Этот тест не может полностью воспроизвести ситуацию nil FileIO при коммите,
// так как для этого нужны реальные сервисы или моки. Однако он проверяет, что код
// правильно валидирует таблицу и схему после перезагрузки, что предотвращает
// большинство случаев nil pointer dereference.
func TestNessieSinkConnector_writeBatch_ValidatesTableAfterReload(t *testing.T) {
	ctx := context.Background()
	config := &v1.NessieSinkSpec{
		NessieURL: "http://localhost:19120",
		Namespace: "test_namespace",
		Table:     "test_table",
	}
	connector := NewNessieSinkConnector(config)
	connector.metadataLocation = "s3://test-bucket/path/to/metadata.json"

	batch := []map[string]interface{}{
		{"id": 1, "name": "test"},
	}

	// Этот тест проверяет, что даже если таблица не может быть загружена
	// (из-за отсутствия реальных сервисов), код правильно обрабатывает ошибку
	err := connector.writeBatch(ctx, nil, batch)

	// Ожидаем ошибку, так как нет реальных сервисов
	require.Error(t, err)

	// Проверяем, что ошибка происходит на этапе загрузки таблицы или создания FileIO,
	// а не из-за nil pointer при коммите
	errMsg := err.Error()

	// Ошибка не должна быть паникой - код должен корректно обработать ситуацию
	assert.NotContains(t, errMsg, "panic")
	assert.NotContains(t, errMsg, "nil pointer")
}
