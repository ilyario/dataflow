#!/bin/bash

# Скрипт для отправки тестового сообщения в Kafka топик
# Используется для тестирования Kafka -> Iceberg с Flatten трансформером

TOPIC="${1:-stock-topic}"
BROKER="${2:-localhost:9092}"

# Тестовое сообщение из примера документации
MESSAGE='{
  "type": "stock",
  "version": 32476984,
  "rowsStock": [
    {"sku": 400125868, "section": "A015"},
    {"sku": 400125868, "section": "A001"}
  ]
}'

echo "Отправка сообщения в топик: $TOPIC"
echo "Брокер: $BROKER"
echo ""
echo "Сообщение:"
echo "$MESSAGE" | jq .
echo ""

# Преобразуем многострочный JSON в одну строку для корректной отправки
MESSAGE_ONELINE=$(echo "$MESSAGE" | jq -c .)

# Проверяем наличие kafka-console-producer
if command -v kafka-console-producer &> /dev/null; then
    echo "$MESSAGE_ONELINE" | kafka-console-producer --bootstrap-server "$BROKER" --topic "$TOPIC"
    echo "Сообщение отправлено через kafka-console-producer"
elif docker ps | grep -q kafka; then
    # Используем docker exec для отправки через контейнер Kafka
    # Важно: используем -c для compact (одна строка) и --property parse.key=false для избежания разбивки
    echo "$MESSAGE_ONELINE" | docker exec -i kafka kafka-console-producer \
        --bootstrap-server localhost:29092 \
        --topic "$TOPIC" \
        --property "parse.key=false"
    echo "Сообщение отправлено через docker exec"
else
    echo "Ошибка: не найден kafka-console-producer и контейнер Kafka не запущен"
    echo ""
    echo "Установите Kafka CLI или запустите docker-compose:"
    echo "  docker-compose up -d"
    exit 1
fi

echo ""
echo "Проверьте результат в Iceberg таблице или через Kafka UI: http://localhost:8080"

