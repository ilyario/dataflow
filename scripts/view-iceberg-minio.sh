#!/bin/bash

# Скрипт для просмотра данных Iceberg через MinIO
# Показывает структуру файлов и позволяет скачать Parquet файлы

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ALIAS="${MINIO_ALIAS:-minio}"
BUCKET="${BUCKET:-warehouse}"
NAMESPACE="${1:-default}"
TABLE="${2:-stock_items}"

echo "Просмотр данных Iceberg через MinIO"
echo "Namespace: $NAMESPACE"
echo "Table: $TABLE"
echo ""

# Проверка доступности MinIO
if ! docker exec -it mc /tmp/mc alias list 2>/dev/null | grep -q "$MINIO_ALIAS"; then
    echo "⚠️  MinIO alias не настроен. Настройка..."
    docker exec -it mc /tmp/mc alias set $MINIO_ALIAS $MINIO_ENDPOINT admin password 2>/dev/null || {
        echo "ОШИБКА: Не удалось подключиться к MinIO"
        echo "Убедитесь, что контейнер 'mc' запущен: docker-compose ps"
        exit 1
    }
fi

TABLE_PATH="$BUCKET/$NAMESPACE/$TABLE"
REST_URL="${REST_URL:-http://localhost:8081}"

echo "=== Проверка наличия данных ==="
# Проверяем snapshots через REST API
SNAPSHOTS=$(curl -s "$REST_URL/v1/namespaces/$NAMESPACE/tables/$TABLE" 2>/dev/null | jq -r '.metadata."current-snapshot-id" // empty')

if [ "$SNAPSHOTS" = "-1" ] || [ -z "$SNAPSHOTS" ]; then
    echo "⚠️  Таблица пуста (нет snapshots)"
    echo "   Отправьте данные через: ./scripts/send-test-message.sh"
    echo ""
    echo "Проверка структуры таблицы:"
else
    echo "✓ Найдены snapshots (ID: $SNAPSHOTS)"
    echo ""
fi

echo "=== Структура файлов таблицы ==="
echo "Базовый путь: s3://$TABLE_PATH"
echo ""

# Iceberg хранит данные в разных местах:
# - metadata/ - метаданные
# - data/ - данные (если есть)
# - или прямо в корне таблицы

echo "Все файлы в таблице:"
docker exec -it mc /tmp/mc find $MINIO_ALIAS/$TABLE_PATH -type f 2>/dev/null | head -30 || {
    echo "⚠️  Не удалось получить список файлов"
    echo "   Убедитесь, что контейнер 'mc' запущен: docker-compose ps"
}

echo ""
echo "=== Поиск Parquet файлов ==="
# Ищем Parquet файлы в разных возможных местах
echo "В корне таблицы:"
docker exec -it mc /tmp/mc find $MINIO_ALIAS/$TABLE_PATH -maxdepth 1 -name "*.parquet" 2>/dev/null | head -10 || echo "  Не найдено"

echo ""
echo "В подпапках:"
docker exec -it mc /tmp/mc find $MINIO_ALIAS/$TABLE_PATH -name "*.parquet" 2>/dev/null | head -20 || echo "  Не найдено"

echo ""
echo "=== Метаданные ==="
echo "Metadata файлы:"
docker exec -it mc /tmp/mc find $MINIO_ALIAS/$TABLE_PATH -name "*.metadata.json" 2>/dev/null | head -5 || echo "  Не найдено"

echo ""
echo "=== Manifest файлы ==="
docker exec -it mc /tmp/mc find $MINIO_ALIAS/$TABLE_PATH -name "*.avro" 2>/dev/null | head -5 || echo "  Не найдено"

echo ""
echo "=== Инструкции ==="
echo "1. Для просмотра через MinIO Console:"
echo "   http://localhost:9001 (логин: admin, пароль: password)"
echo "   Перейдите в bucket: $BUCKET -> $NAMESPACE -> $TABLE"
echo "   Ищите файлы .parquet в корне или подпапках"
echo ""
echo "2. Если таблица пуста, отправьте тестовые данные:"
echo "   ./scripts/send-test-message.sh"
echo ""
echo "3. Для скачивания Parquet файла (если найден):"
echo "   docker exec -it mc /tmp/mc cp $MINIO_ALIAS/$TABLE_PATH/xxx.parquet /tmp/"
echo ""
echo "4. Для чтения Parquet файла (требует Python):"
echo "   python3 <<EOF"
echo "   import pyarrow.parquet as pq"
echo "   table = pq.read_table('downloaded.parquet')"
echo "   print(table.to_pandas())"
echo "   EOF"
echo ""
echo "5. Проверка через REST API:"
echo "   curl -s $REST_URL/v1/namespaces/$NAMESPACE/tables/$TABLE | jq '.metadata.\"current-snapshot-id\"'"

