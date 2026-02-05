#!/bin/bash
echo "=== TESTING FULL DATA PIPELINE ==="
echo

# 1. Проверяем генератор
echo "1. Data Generator status:"
docker-compose ps data-generator | grep Up && echo "✅ Running" || echo "❌ Stopped"

# 2. Проверяем Kafka
echo -e "\n2. Kafka status:"
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep sensor-data && echo "✅ Topic exists" || echo "❌ No topic"
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic sensor-data --time -1 2>/dev/null | awk -F: '{sum+=$3} END {print "📊 Messages:", sum}'

# 3. Проверяем ClickHouse
echo -e "\n3. ClickHouse status:"
docker-compose exec clickhouse clickhouse-client --user admin --password admin123 --query "SELECT 'Connected'" 2>/dev/null && echo "✅ Connected" || echo "❌ Connection failed"

# 4. Проверяем данные
echo -e "\n4. Data in ClickHouse:"
docker-compose exec clickhouse clickhouse-client --user admin --password admin123 --query "
SELECT 
    table,
    count() as rows,
    formatReadableSize(sum(bytes)) as size
FROM system.tables 
LEFT JOIN system.parts USING (database, table)
WHERE database = 'production' 
GROUP BY table
ORDER BY rows DESC" 2>/dev/null || echo "No data yet"

# 5. Проверяем processor
echo -e "\n5. Data Processor:"
docker ps | grep data-processor && echo "✅ Running" || echo "⚠️  Not running (run manually: docker run ...)"

echo -e "\n=== TEST COMPLETE ==="
