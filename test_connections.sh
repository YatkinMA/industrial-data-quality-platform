# test_connections.sh
#!/bin/bash

echo "Testing connections between services..."

echo "1. Testing Zookeeper..."
docker-compose exec zookeeper bash -c "echo ruok | nc localhost 2181" && echo "✓ Zookeeper is OK" || echo "✗ Zookeeper not ready"

echo "2. Testing Kafka..."
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list && echo "✓ Kafka is OK" || echo "✗ Kafka not ready"

echo "3. Testing ClickHouse..."
docker-compose exec clickhouse clickhouse-client --user admin --password admin123 --query "SHOW DATABASES" && echo "✓ ClickHouse is OK" || echo "✗ ClickHouse not ready"

echo "4. Checking if sensor-data topic exists..."
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -q sensor-data && echo "✓ sensor-data topic exists" || echo "✗ sensor-data topic missing"

echo "5. Checking data generator..."
docker-compose ps data-generator | grep -q Up && echo "✓ Data generator is running" || echo "✗ Data generator not running"
