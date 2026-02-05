#!/usr/bin/env python3
"""Тестовый скрипт для проверки подключений"""
import os
from kafka import KafkaProducer
import json
from clickhouse_driver import Client

print("Testing connections...")

# Тест Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BROKERS', 'kafka:9092').split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send('test-topic', {'test': True})
    producer.flush(timeout=5)
    print("✅ Kafka connection: OK")
    producer.close()
except Exception as e:
    print(f"❌ Kafka connection: ERROR - {e}")

# Тест ClickHouse
try:
    client = Client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.getenv('CLICKHOUSE_PORT', '9000')),
        user=os.getenv('CLICKHOUSE_USER', 'admin'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'admin123'),
        database='production'
    )
    result = client.execute('SELECT 1')
    print("✅ ClickHouse connection: OK")
    
    # Проверяем таблицу
    tables = client.execute("SHOW TABLES FROM production")
    print(f"✅ Tables in production: {[t[0] for t in tables]}")
    
    # Считаем записи
    count = client.execute("SELECT count() FROM production.sensor_data_raw")[0][0]
    print(f"✅ Records in sensor_data_raw: {count}")
    
except Exception as e:
    print(f"❌ ClickHouse connection: ERROR - {e}")

print("\nAll tests completed!")
