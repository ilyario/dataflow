#!/bin/bash

# Скрипт для создания namespace и таблицы в Iceberg REST Catalog

REST_URL="${1:-http://localhost:8081}"
NAMESPACE="${2:-default}"
TABLE="${3:-stock_items}"

echo "Создание namespace: $NAMESPACE"
curl -s -X POST "$REST_URL/v1/namespaces" \
  -H "Content-Type: application/json" \
  -d "{\"namespace\":[\"$NAMESPACE\"]}" | jq . || echo "Namespace уже существует или ошибка"

echo ""
echo "Создание таблицы: $TABLE"

# Создаем таблицу с схемой для данных после Flatten трансформации
# partition-spec должен быть объектом, а не массивом
curl -s -X POST "$REST_URL/v1/namespaces/$NAMESPACE/tables" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "stock_items",
    "schema": {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "type", "type": "string", "required": false},
        {"id": 2, "name": "version", "type": "long", "required": false},
        {"id": 3, "name": "sku", "type": "long", "required": false},
        {"id": 4, "name": "section", "type": "string", "required": false},
        {"id": 5, "name": "created_at", "type": "string", "required": false}
      ]
    },
    "partition-spec": {
      "spec-id": 0,
      "fields": []
    }
  }' | jq . || echo "Таблица уже существует или ошибка"

echo ""
echo "Проверка таблицы:"
curl -s "$REST_URL/v1/namespaces/$NAMESPACE/tables" | jq .

