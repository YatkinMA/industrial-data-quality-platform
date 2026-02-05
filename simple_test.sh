#!/bin/bash
echo "=== SIMPLE TEST ==="
echo "1. Services:"
docker-compose ps --format "table {{.Names}}\t{{.Status}}"

echo -e "\n2. Kafka test:"
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "Kafka not ready"

echo -e "\n3. ClickHouse test:"
docker-compose exec clickhouse clickhouse-client \
  --user admin --password admin123 \
  --query "SELECT 'ClickHouse OK'" 2>/dev/null && echo "ClickHouse OK" || echo "ClickHouse error"

echo -e "\n4. Messages in Kafka:"
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor-data \
  --max-messages 2 \
  --timeout-ms 5000 2>/dev/null | jq . 2>/dev/null || echo "No messages yet"

echo -e "\n=== DONE ==="
