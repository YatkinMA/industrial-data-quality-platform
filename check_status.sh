#!/bin/bash
echo "=== SYSTEM STATUS CHECK ==="
echo

echo "1. Все сервисы:"
docker-compose ps

echo -e "\n2. Kafka сообщения (ждем 5 секунд...):"
sleep 5
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor-data \
  --max-messages 3 \
  --timeout-ms 10000 2>/dev/null | wc -l | awk '{print "Got", $1, "messages"}'

echo -e "\n3. ClickHouse:"
docker-compose exec clickhouse clickhouse-client \
  --user admin \
  --password admin123 \
  --query "SELECT 'Tables count:', count() FROM system.tables WHERE database = 'production'" 2>/dev/null

echo -e "\n4. Generator логи:"
docker-compose logs --tail=3 data-generator | grep -E "(INFO|ERROR|Sent)" | tail -3

echo -e "\n5. Processor логи:"
docker-compose logs --tail=3 data-processor | tail -3

echo -e "\n=== CHECK DONE ==="
