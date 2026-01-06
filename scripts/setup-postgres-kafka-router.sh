#!/bin/bash

# Скрипт для подготовки базы данных PostgreSQL и топиков Kafka
# для тестирования PostgreSQL -> Kafka с router трансформером

set -e

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Подготовка PostgreSQL и Kafka для router тестирования ===${NC}"
echo ""

# Параметры подключения
# По умолчанию используем localhost для локального запуска
# Для Docker окружения установите PG_HOST=postgres
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-dataflow}"
PG_PASSWORD="${PG_PASSWORD:-dataflow}"
PG_DB="${PG_DB:-dataflow}"

KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
KAFKA_DOCKER="${KAFKA_DOCKER:-kafka}"

# Функция для выполнения SQL команд
execute_sql() {
    local sql="$1"
    # Проверяем, является ли PG_HOST именем Docker контейнера
    if [ "$PG_HOST" != "localhost" ] && [ "$PG_HOST" != "127.0.0.1" ] && docker ps --format '{{.Names}}' | grep -q "^${PG_HOST}$"; then
        # Используем docker exec для контейнера
        docker exec -i "$PG_HOST" psql -U "$PG_USER" -d "$PG_DB" <<EOF
$sql
EOF
    else
        # Используем прямое подключение через psql
        PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" <<EOF
$sql
EOF
    fi
}

# Функция для создания топика Kafka
create_kafka_topic() {
    local topic="$1"
    echo -e "${YELLOW}Создание топика: $topic${NC}"

    if command -v kafka-topics &> /dev/null; then
        kafka-topics --create \
            --bootstrap-server "$KAFKA_BROKER" \
            --topic "$topic" \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists 2>/dev/null || true
    elif docker ps | grep -q "$KAFKA_DOCKER"; then
        docker exec "$KAFKA_DOCKER" kafka-topics --create \
            --bootstrap-server localhost:29092 \
            --topic "$topic" \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists 2>/dev/null || true
    else
        echo -e "${RED}Ошибка: не найден kafka-topics и контейнер Kafka не запущен${NC}"
        return 1
    fi
}

# Шаг 1: Создание таблицы events в PostgreSQL
echo -e "${GREEN}[1/4] Создание таблицы events в PostgreSQL...${NC}"
execute_sql "
DROP TABLE IF EXISTS events CASCADE;

CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индекса для поля type для лучшей производительности
CREATE INDEX idx_events_type ON events(type);
CREATE INDEX idx_events_created_at ON events(created_at);
"

echo -e "${GREEN}✓ Таблица events создана${NC}"
echo ""

# Шаг 2: Вставка тестовых данных
echo -e "${GREEN}[2/4] Вставка тестовых данных...${NC}"
execute_sql "
INSERT INTO events (type, data) VALUES
    ('order', '{\"order_id\": 1001, \"customer_id\": 123, \"amount\": 99.99, \"items\": [{\"product_id\": 1, \"quantity\": 2}]}'),
    ('user', '{\"user_id\": 2001, \"name\": \"Иван Иванов\", \"email\": \"ivan@example.com\", \"status\": \"active\"}'),
    ('payment', '{\"payment_id\": 3001, \"order_id\": 1001, \"amount\": 99.99, \"method\": \"card\", \"status\": \"completed\"}'),
    ('order', '{\"order_id\": 1002, \"customer_id\": 124, \"amount\": 149.50, \"items\": [{\"product_id\": 2, \"quantity\": 1}]}'),
    ('user', '{\"user_id\": 2002, \"name\": \"Петр Петров\", \"email\": \"petr@example.com\", \"status\": \"active\"}'),
    ('payment', '{\"payment_id\": 3002, \"order_id\": 1002, \"amount\": 149.50, \"method\": \"paypal\", \"status\": \"pending\"}'),
    ('order', '{\"order_id\": 1003, \"customer_id\": 125, \"amount\": 49.99, \"items\": [{\"product_id\": 3, \"quantity\": 3}]}'),
    ('user', '{\"user_id\": 2003, \"name\": \"Мария Сидорова\", \"email\": \"maria@example.com\", \"status\": \"inactive\"}'),
    ('payment', '{\"payment_id\": 3003, \"order_id\": 1003, \"amount\": 49.99, \"method\": \"card\", \"status\": \"completed\"}');
"

echo -e "${GREEN}✓ Тестовые данные вставлены${NC}"
echo ""

# Шаг 3: Создание топиков Kafka
echo -e "${GREEN}[3/4] Создание топиков Kafka...${NC}"

# Ждем, пока Kafka будет готов
echo "Ожидание готовности Kafka..."
if docker ps | grep -q "$KAFKA_DOCKER"; then
    for i in {1..30}; do
        if docker exec "$KAFKA_DOCKER" kafka-broker-api-versions --bootstrap-server localhost:29092 &>/dev/null; then
            break
        fi
        if [ $i -eq 30 ]; then
            echo -e "${RED}Ошибка: Kafka не готов после 30 попыток${NC}"
            exit 1
        fi
        sleep 1
    done
fi

# Создаем топики
create_kafka_topic "orders"
create_kafka_topic "users"
create_kafka_topic "payments"
create_kafka_topic "default-events"

echo -e "${GREEN}✓ Топики Kafka созданы${NC}"
echo ""

# Шаг 4: Проверка результатов
echo -e "${GREEN}[4/4] Проверка результатов...${NC}"

# Проверяем данные в PostgreSQL
echo "Данные в таблице events:"
execute_sql "SELECT id, type, created_at FROM events ORDER BY id;"

echo ""
echo -e "${GREEN}=== Подготовка завершена успешно! ===${NC}"
echo ""
echo "Следующие шаги:"
echo "1. Примените DataFlow конфигурацию:"
echo "   kubectl apply -f config/samples/postgres-to-kafka-router.yaml"
echo ""
echo "2. Проверьте сообщения в топиках Kafka:"
echo "   # Для топика orders:"
echo "   docker exec -it $KAFKA_DOCKER kafka-console-consumer --bootstrap-server localhost:29092 --topic orders --from-beginning"
echo ""
echo "   # Для топика users:"
echo "   docker exec -it $KAFKA_DOCKER kafka-console-consumer --bootstrap-server localhost:29092 --topic users --from-beginning"
echo ""
echo "   # Для топика payments:"
echo "   docker exec -it $KAFKA_DOCKER kafka-console-consumer --bootstrap-server localhost:29092 --topic payments --from-beginning"
echo ""
echo "3. Добавьте новые данные для тестирования:"
echo "   docker exec -i $PG_HOST psql -U $PG_USER -d $PG_DB -c \"INSERT INTO events (type, data) VALUES ('order', '{\\\"order_id\\\": 1004, \\\"customer_id\\\": 126, \\\"amount\\\": 199.99}');\""
echo ""

