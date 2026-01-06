#!/bin/bash

# Скрипт для просмотра данных из Iceberg таблицы

REST_URL="${1:-http://localhost:8081}"
NAMESPACE="${2:-default}"
TABLE="${3:-stock_items}"

echo "Просмотр данных из таблицы: $NAMESPACE.$TABLE"
echo "REST Catalog URL: $REST_URL"
echo ""

# Проверка доступности REST API
if ! curl -s -f "$REST_URL/v1/config" > /dev/null 2>&1; then
    echo "ОШИБКА: Iceberg REST API недоступен по адресу $REST_URL"
    echo "Убедитесь, что контейнер iceberg-rest запущен:"
    echo "  docker-compose ps"
    echo "  docker logs iceberg-rest"
    exit 1
fi

# Список namespaces
echo "=== Доступные namespaces ==="
curl -s "$REST_URL/v1/namespaces" | jq '.' || echo "Ошибка получения namespaces"
echo ""

# Список таблиц в namespace
echo "=== Таблицы в namespace '$NAMESPACE' ==="
curl -s "$REST_URL/v1/namespaces/$NAMESPACE/tables" | jq '.' || echo "Ошибка получения списка таблиц"
echo ""

# Метаданные таблицы
echo "=== Метаданные таблицы '$TABLE' ==="
TABLE_META=$(curl -s "$REST_URL/v1/namespaces/$NAMESPACE/tables/$TABLE")
echo "$TABLE_META" | jq '.' || echo "Ошибка получения метаданных таблицы"

# Проверка наличия данных
SNAPSHOT_ID=$(echo "$TABLE_META" | jq -r '.metadata."current-snapshot-id" // empty')
SNAPSHOT_COUNT=$(echo "$TABLE_META" | jq -r '.metadata.snapshots | length // 0')

echo ""
if [ "$SNAPSHOT_ID" = "-1" ] || [ "$SNAPSHOT_COUNT" = "0" ]; then
    echo "⚠️  ВНИМАНИЕ: Таблица пуста (нет snapshots)"
    echo "   current-snapshot-id: $SNAPSHOT_ID"
    echo "   snapshots count: $SNAPSHOT_COUNT"
    echo ""
    echo "   Для записи данных:"
    echo "   1. Убедитесь, что DataFlow запущен: kubectl get dataflows"
    echo "   2. Отправьте тестовое сообщение: ./scripts/send-test-message.sh"
    echo ""
else
    echo "✓ Таблица содержит данные"
    echo "   current-snapshot-id: $SNAPSHOT_ID"
    echo "   snapshots count: $SNAPSHOT_COUNT"
    echo ""
fi
echo ""

# Данные таблицы
echo "=== Попытка чтения данных через REST API ==="
DATA_RESPONSE=$(curl -s "$REST_URL/v1/namespaces/$NAMESPACE/tables/$TABLE/data")

if echo "$DATA_RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
    echo "⚠️  REST API не поддерживает прямой endpoint /data для чтения"
    echo "Ответ: $(echo "$DATA_RESPONSE" | jq -r '.error.message // .error')"
    echo ""
    echo "=== Альтернативные способы просмотра данных ==="
    echo ""
    echo "1. Через MinIO Console (просмотр Parquet файлов):"
    echo "   - Откройте: http://localhost:9001"
    echo "   - Логин: admin, Пароль: password"
    echo "   - Перейдите в bucket 'warehouse' -> '$NAMESPACE' -> '$TABLE'"
    echo "   - Ищите .parquet файлы (могут быть в корне или подпапках)"
    echo "   - Используйте скрипт: ./scripts/view-iceberg-minio.sh $NAMESPACE $TABLE"
    echo ""
    echo "2. Использование Spark (если установлен):"
    echo "   spark-sql --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \\"
    echo "            --conf spark.sql.catalog.iceberg.type=rest \\"
    echo "            --conf spark.sql.catalog.iceberg.uri=$REST_URL \\"
    echo "            -e \"SELECT * FROM iceberg.default.$TABLE\""
    echo ""
    echo "3. Использование Python с pyiceberg:"
    echo "   pip install pyiceberg"
    echo "   python3 <<EOF"
    echo "   from pyiceberg.catalog import load_catalog"
    echo "   catalog = load_catalog("
    echo "       name='iceberg',"
    echo "       uri='$REST_URL',"
    echo "       type='rest'"
    echo "   )"
    echo "   table = catalog.load_table('$NAMESPACE.$TABLE')"
    echo "   for row in table.scan().to_arrow().to_pylist():"
    echo "       print(row)"
    echo "   EOF"
    echo ""
    echo "4. Прямое чтение через MinIO CLI (mc):"
    echo "   docker exec -it mc /tmp/mc ls -r minio/warehouse/default/$TABLE/data/"
    echo ""
    echo "5. Проверка метаданных и snapshots:"
    echo "   curl -s '$REST_URL/v1/namespaces/$NAMESPACE/tables/$TABLE' | jq '.metadata-location'"
    echo "   curl -s '$REST_URL/v1/namespaces/$NAMESPACE/tables/$TABLE/metadata' | jq '.snapshots'"
else
    # Если ответ успешный (нестандартный API)
    if echo "$DATA_RESPONSE" | jq -e '. | type == "array"' > /dev/null 2>&1; then
        ROW_COUNT=$(echo "$DATA_RESPONSE" | jq '. | length')
        echo "✓ Найдено записей: $ROW_COUNT"
        echo ""
        if [ "$ROW_COUNT" -gt 0 ]; then
            echo "$DATA_RESPONSE" | jq '.'
        else
            echo "Таблица пуста"
        fi
    else
        echo "$DATA_RESPONSE" | jq '.' || echo "$DATA_RESPONSE"
    fi
fi

echo ""
echo "=== Полезные команды ==="
echo "Проверка snapshots таблицы:"
curl -s "$REST_URL/v1/namespaces/$NAMESPACE/tables/$TABLE/metadata" 2>/dev/null | jq -r '.snapshots[]? | "Snapshot ID: \(.snapshot-id), Timestamp: \(.timestamp-ms)"' || echo "Не удалось получить snapshots"

