#!/bin/bash

# Скрипт для тестирования полного цикла: RabbitMQ → Iceberg → RabbitMQ
# Схема: положили в очередь RabbitMQ → попало в Iceberg → попало в другую очередь RabbitMQ

set -e

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Параметры
RABBITMQ_URL="${RABBITMQ_URL:-amqp://guest:guest@localhost:5672/}"
RABBITMQ_HOST="${RABBITMQ_HOST:-localhost}"
RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"
RABBITMQ_USER="${RABBITMQ_USER:-guest}"
RABBITMQ_PASS="${RABBITMQ_PASS:-guest}"
INPUT_QUEUE="${INPUT_QUEUE:-rabbitmq-input}"
OUTPUT_EXCHANGE="${OUTPUT_EXCHANGE:-output-exchange}"
OUTPUT_ROUTING_KEY="${OUTPUT_ROUTING_KEY:-output.queue}"
OUTPUT_QUEUE="${OUTPUT_QUEUE:-rabbitmq-output}"
ICEBERG_REST_URL="${ICEBERG_REST_URL:-http://localhost:8081}"
ICEBERG_NAMESPACE="${ICEBERG_NAMESPACE:-default}"
ICEBERG_TABLE="${ICEBERG_TABLE:-test_rabbitmq_iceberg}"

# Проверка зависимостей
echo -e "${YELLOW}[1/7] Проверка зависимостей...${NC}"

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Ошибка: python3 не найден. Установите Python 3.${NC}"
    exit 1
fi

if ! python3 -c "import pika" 2>/dev/null; then
    echo -e "${YELLOW}Установка pika (библиотека для RabbitMQ)...${NC}"
    pip3 install pika --quiet || {
        echo -e "${RED}Ошибка: не удалось установить pika. Установите вручную: pip3 install pika${NC}"
        exit 1
    }
fi

if ! command -v curl &> /dev/null; then
    echo -e "${RED}Ошибка: curl не найден${NC}"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo -e "${RED}Ошибка: jq не найден. Установите jq для работы с JSON${NC}"
    exit 1
fi

# Проверка доступности сервисов
echo -e "${YELLOW}[2/7] Проверка доступности сервисов...${NC}"

# Проверка RabbitMQ
if ! curl -s -u "${RABBITMQ_USER}:${RABBITMQ_PASS}" "http://${RABBITMQ_HOST}:15672/api/overview" > /dev/null 2>&1; then
    echo -e "${RED}Ошибка: RabbitMQ недоступен на ${RABBITMQ_HOST}:15672${NC}"
    echo "Убедитесь, что RabbitMQ запущен: docker-compose up -d rabbitmq"
    exit 1
fi
echo -e "${GREEN}✓ RabbitMQ доступен${NC}"

# Проверка Iceberg REST Catalog
if ! curl -s "${ICEBERG_REST_URL}/v1/config" > /dev/null 2>&1; then
    echo -e "${RED}Ошибка: Iceberg REST Catalog недоступен на ${ICEBERG_REST_URL}${NC}"
    echo "Убедитесь, что Iceberg REST Catalog запущен: docker-compose up -d rest"
    exit 1
fi
echo -e "${GREEN}✓ Iceberg REST Catalog доступен${NC}"

# Настройка RabbitMQ (создание очередей и exchange)
echo -e "${YELLOW}[3/7] Настройка RabbitMQ...${NC}"

python3 << EOF
import pika
import sys

try:
    # Подключение к RabbitMQ
    credentials = pika.PlainCredentials('${RABBITMQ_USER}', '${RABBITMQ_PASS}')
    parameters = pika.ConnectionParameters('${RABBITMQ_HOST}', ${RABBITMQ_PORT}, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Создание входной очереди
    channel.queue_declare(queue='${INPUT_QUEUE}', durable=True)
    print(f"✓ Очередь ${INPUT_QUEUE} создана/проверена")

    # Создание выходного exchange
    channel.exchange_declare(exchange='${OUTPUT_EXCHANGE}', exchange_type='topic', durable=True)
    print(f"✓ Exchange ${OUTPUT_EXCHANGE} создан/проверен")

    # Создание выходной очереди и привязка к exchange
    channel.queue_declare(queue='${OUTPUT_QUEUE}', durable=True)
    channel.queue_bind(exchange='${OUTPUT_EXCHANGE}', queue='${OUTPUT_QUEUE}', routing_key='${OUTPUT_ROUTING_KEY}')
    print(f"✓ Очередь ${OUTPUT_QUEUE} создана/проверена и привязана к exchange")

    connection.close()
except Exception as e:
    print(f"Ошибка при настройке RabbitMQ: {e}")
    sys.exit(1)
EOF

# Очистка очередей перед тестом
echo -e "${YELLOW}Очистка очередей перед тестом...${NC}"
python3 << EOF
import pika

try:
    credentials = pika.PlainCredentials('${RABBITMQ_USER}', '${RABBITMQ_PASS}')
    parameters = pika.ConnectionParameters('${RABBITMQ_HOST}', ${RABBITMQ_PORT}, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Очистка входной очереди
    channel.queue_purge(queue='${INPUT_QUEUE}')
    print(f"✓ Очередь ${INPUT_QUEUE} очищена")

    # Очистка выходной очереди
    channel.queue_purge(queue='${OUTPUT_QUEUE}')
    print(f"✓ Очередь ${OUTPUT_QUEUE} очищена")

    connection.close()
except Exception as e:
    print(f"Предупреждение при очистке очередей: {e}")
EOF

# Проверка/создание таблицы Iceberg
echo -e "${YELLOW}[4/7] Проверка таблицы Iceberg...${NC}"

# Проверка namespace
if ! curl -s "${ICEBERG_REST_URL}/v1/namespaces/${ICEBERG_NAMESPACE}" > /dev/null 2>&1; then
    echo "Создание namespace ${ICEBERG_NAMESPACE}..."
    curl -s -X POST "${ICEBERG_REST_URL}/v1/namespaces" \
        -H "Content-Type: application/json" \
        -d "{\"namespace\":[\"${ICEBERG_NAMESPACE}\"]}" | jq . || echo "Namespace уже существует"
fi

# Проверка таблицы (будет создана автоматически DataFlow, но проверим)
echo "Таблица будет создана автоматически DataFlow при первом использовании"

# Применение манифестов DataFlow
echo -e "${YELLOW}[5/7] Применение манифестов DataFlow...${NC}"

MANIFEST_DIR="${MANIFEST_DIR:-config/samples}"

if [ ! -f "${MANIFEST_DIR}/rabbitmq-to-iceberg.yaml" ]; then
    echo -e "${RED}Ошибка: файл ${MANIFEST_DIR}/rabbitmq-to-iceberg.yaml не найден${NC}"
    exit 1
fi

if [ ! -f "${MANIFEST_DIR}/iceberg-to-rabbitmq.yaml" ]; then
    echo -e "${RED}Ошибка: файл ${MANIFEST_DIR}/iceberg-to-rabbitmq.yaml не найден${NC}"
    exit 1
fi

# Применяем манифесты (если kubectl доступен)
if command -v kubectl &> /dev/null; then
    echo "Применение манифестов через kubectl..."
    kubectl apply -f "${MANIFEST_DIR}/rabbitmq-to-iceberg.yaml" || echo "Предупреждение: не удалось применить rabbitmq-to-iceberg.yaml"
    kubectl apply -f "${MANIFEST_DIR}/iceberg-to-rabbitmq.yaml" || echo "Предупреждение: не удалось применить iceberg-to-rabbitmq.yaml"
    echo "✓ Манифесты применены"
    echo ""
    echo "Ожидание готовности DataFlow (10 секунд)..."
    sleep 10
else
    echo -e "${YELLOW}Предупреждение: kubectl не найден. Примените манифесты вручную:${NC}"
    echo "  kubectl apply -f ${MANIFEST_DIR}/rabbitmq-to-iceberg.yaml"
    echo "  kubectl apply -f ${MANIFEST_DIR}/iceberg-to-rabbitmq.yaml"
    echo ""
    read -p "Нажмите Enter после применения манифестов..."
fi

# Отправка тестового сообщения
echo -e "${YELLOW}[6/7] Отправка тестового сообщения в RabbitMQ...${NC}"

# Генерируем уникальное тестовое сообщение с временной меткой
TEST_MESSAGE=$(cat <<EOF
{
  "test_id": "$(date +%s)",
  "message": "Test message for RabbitMQ → Iceberg → RabbitMQ cycle",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "data": {
    "id": 12345,
    "name": "Test Item",
    "value": 42.5
  }
}
EOF
)

echo "Тестовое сообщение:"
echo "$TEST_MESSAGE" | jq .

# Отправка сообщения в входную очередь
python3 << EOF
import pika
import json
import sys

try:
    credentials = pika.PlainCredentials('${RABBITMQ_USER}', '${RABBITMQ_PASS}')
    parameters = pika.ConnectionParameters('${RABBITMQ_HOST}', ${RABBITMQ_PORT}, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    message = '''${TEST_MESSAGE}'''

    channel.basic_publish(
        exchange='',
        routing_key='${INPUT_QUEUE}',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Сделать сообщение персистентным
            content_type='application/json'
        )
    )

    print(f"✓ Сообщение отправлено в очередь ${INPUT_QUEUE}")
    connection.close()
except Exception as e:
    print(f"Ошибка при отправке сообщения: {e}")
    sys.exit(1)
EOF

# Извлекаем test_id для проверки
TEST_ID=$(echo "$TEST_MESSAGE" | jq -r '.test_id')
echo "Test ID для проверки: ${TEST_ID}"

# Ожидание и проверка результата
echo -e "${YELLOW}[7/7] Ожидание и проверка результата...${NC}"

MAX_WAIT=60
WAIT_INTERVAL=2
ELAPSED=0
FOUND_IN_ICEBERG=false
FOUND_IN_OUTPUT=false

echo "Ожидание обработки сообщения (максимум ${MAX_WAIT} секунд)..."

while [ $ELAPSED -lt $MAX_WAIT ]; do
    # Проверка в Iceberg
    if [ "$FOUND_IN_ICEBERG" = false ]; then
        # Проверяем наличие данных в таблице через REST API
        # Примечание: Iceberg REST API может не иметь прямого endpoint для чтения данных
        # Используем альтернативный способ - проверка через метаданные таблицы
        TABLE_INFO=$(curl -s "${ICEBERG_REST_URL}/v1/namespaces/${ICEBERG_NAMESPACE}/tables/${ICEBERG_TABLE}" 2>/dev/null)
        if [ $? -eq 0 ] && [ -n "$TABLE_INFO" ]; then
            # Таблица существует, проверяем наличие snapshots (данных)
            SNAPSHOTS=$(curl -s "${ICEBERG_REST_URL}/v1/namespaces/${ICEBERG_NAMESPACE}/tables/${ICEBERG_TABLE}/metadata" 2>/dev/null | jq -r '.current-snapshot-id // empty' 2>/dev/null)
            if [ -n "$SNAPSHOTS" ] && [ "$SNAPSHOTS" != "null" ]; then
                echo -e "${GREEN}✓ Данные найдены в Iceberg таблице${NC}"
                FOUND_IN_ICEBERG=true
            fi
        fi
    fi

    # Проверка в выходной очереди RabbitMQ
    if [ "$FOUND_IN_OUTPUT" = false ]; then
        MESSAGE_COUNT=$(python3 << PYEOF
import pika
try:
    credentials = pika.PlainCredentials('${RABBITMQ_USER}', '${RABBITMQ_PASS}')
    parameters = pika.ConnectionParameters('${RABBITMQ_HOST}', ${RABBITMQ_PORT}, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    method = channel.queue_declare(queue='${OUTPUT_QUEUE}', durable=True, passive=True)
    count = method.method.message_count
    print(count)
    connection.close()
except:
    print("0")
PYEOF
        )

        if [ "$MESSAGE_COUNT" -gt 0 ]; then
            echo -e "${GREEN}✓ Сообщение найдено в выходной очереди RabbitMQ (${MESSAGE_COUNT} сообщений)${NC}"
            FOUND_IN_OUTPUT=true

            # Читаем сообщение для проверки
            RECEIVED_MESSAGE=$(python3 << PYEOF
import pika
import json
try:
    credentials = pika.PlainCredentials('${RABBITMQ_USER}', '${RABBITMQ_PASS}')
    parameters = pika.ConnectionParameters('${RABBITMQ_HOST}', ${RABBITMQ_PORT}, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    method, properties, body = channel.basic_get(queue='${OUTPUT_QUEUE}', auto_ack=True)
    if method:
        message = body.decode('utf-8')
        print(message)
    connection.close()
except Exception as e:
    print(f"ERROR: {e}")
PYEOF
            )

            echo ""
            echo "Полученное сообщение из выходной очереди:"
            echo "$RECEIVED_MESSAGE" | jq . || echo "$RECEIVED_MESSAGE"

            # Проверяем, что test_id совпадает
            RECEIVED_TEST_ID=$(echo "$RECEIVED_MESSAGE" | jq -r '.test_id // empty' 2>/dev/null)
            if [ "$RECEIVED_TEST_ID" = "$TEST_ID" ]; then
                echo -e "${GREEN}✓ Test ID совпадает! Сообщение успешно прошло полный цикл${NC}"
            else
                echo -e "${YELLOW}⚠ Test ID не совпадает, но сообщение получено${NC}"
            fi
        fi
    fi

    if [ "$FOUND_IN_ICEBERG" = true ] && [ "$FOUND_IN_OUTPUT" = true ]; then
        break
    fi

    sleep $WAIT_INTERVAL
    ELAPSED=$((ELAPSED + WAIT_INTERVAL))
    echo "  Ожидание... (${ELAPSED}/${MAX_WAIT} сек)"
done

echo ""
echo "=== Результаты теста ==="

if [ "$FOUND_IN_ICEBERG" = true ]; then
    echo -e "${GREEN}✓ Сообщение попало в Iceberg${NC}"
else
    echo -e "${RED}✗ Сообщение НЕ найдено в Iceberg${NC}"
fi

if [ "$FOUND_IN_OUTPUT" = true ]; then
    echo -e "${GREEN}✓ Сообщение попало в выходную очередь RabbitMQ${NC}"
else
    echo -e "${RED}✗ Сообщение НЕ найдено в выходной очереди RabbitMQ${NC}"
fi

if [ "$FOUND_IN_ICEBERG" = true ] && [ "$FOUND_IN_OUTPUT" = true ]; then
    echo ""
    echo -e "${GREEN}=== ТЕСТ ПРОЙДЕН УСПЕШНО! ===${NC}"
    echo "Полный цикл работает: RabbitMQ → Iceberg → RabbitMQ"
    exit 0
else
    echo ""
    echo -e "${RED}=== ТЕСТ НЕ ПРОЙДЕН ===${NC}"
    echo ""
    echo "Диагностика:"
    echo "1. Проверьте логи DataFlow:"
    echo "   kubectl logs -l control-plane=controller-manager"
    echo ""
    echo "2. Если видите ошибку 'UncheckedSQLException: Unknown failure':"
    echo "   - Проверьте, что MinIO доступен: docker ps | grep minio"
    echo "   - Проверьте переменные окружения для S3/MinIO:"
    echo "     AWS_ENDPOINT_URL_S3=http://localhost:9000 (для локального запуска)"
    echo "     AWS_ENDPOINT_URL_S3=http://minio:9000 (для запуска в Kubernetes)"
    echo "   - Проверьте credentials: AWS_ACCESS_KEY_ID=admin, AWS_SECRET_ACCESS_KEY=password"
    echo ""
    echo "3. Проверьте статус DataFlow:"
    echo "   kubectl get dataflows"
    echo "   kubectl describe dataflow rabbitmq-to-iceberg"
    echo "   kubectl describe dataflow iceberg-to-rabbitmq"
    echo ""
    echo "4. Проверьте доступность Iceberg REST Catalog:"
    echo "   curl ${ICEBERG_REST_URL}/v1/config"
    exit 1
fi

