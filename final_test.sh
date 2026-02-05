#!/bin/bash
echo "=== FINAL PIPELINE TEST ==="

echo "1. All services status:"
docker-compose ps --format "table {{.Names}}\t{{.Status}}"

echo -e "\n2. Kafka topics:"
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo -e "\n3. Generator working? (last 3 lines):"
docker-compose logs --tail=3 generator

echo -e "\n4. Try to consume messages:"
timeout 5 docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning 2>&1 | head -5

echo -e "\n=== TEST COMPLETE ==="
