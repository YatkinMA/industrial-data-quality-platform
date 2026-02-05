# Makefile
.PHONY: up down logs kafka-produce kafka-consume clickhouse-query clean

up:
	docker-compose up -d
	@echo "Services starting..."
	@echo "ClickHouse UI: http://localhost:8123 (admin:admin123)"
	@echo "Kafdrop UI: http://localhost:9001"
	@echo "Run 'make logs' to see logs"

down:
	docker-compose down -v

logs:
	docker-compose logs -f

kafka-topics:
	docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-consume:
	docker-compose exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic sensor-data \
		--from-beginning

clickhouse-shell:
	docker-compose exec clickhouse clickhouse-client \
		--user admin \
		--password admin123 \
		--database production

clickhouse-query:
	@echo "Пример запроса: SELECT count() FROM sensor_data_raw"
	docker-compose exec clickhouse clickhouse-client \
		--user admin \
		--password admin123 \
		--database production \
		--query "SELECT count() FROM sensor_data_raw"

test-all:
	@echo "Тестируем подключения..."
	@echo "1. Проверяем Kafka..."
	@docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -q sensor-data && echo "✓ Kafka topic exists" || echo "✗ Kafka topic missing"
	@echo "2. Проверяем ClickHouse..."
	@docker-compose exec clickhouse clickhouse-client --user admin --password admin123 --query "SHOW DATABASES" | grep -q production && echo "✓ ClickHouse database exists" || echo "✗ ClickHouse database missing"

clean:
	docker-compose down -v
	sudo rm -rf clickhouse_data
