"""
Простой потребитель Kafka для записи в ClickHouse
"""
from kafka import KafkaConsumer
import json
from clickhouse_driver import Client
import os
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Kafka to ClickHouse consumer...")
    
    # Подключение к Kafka
    consumer = KafkaConsumer(
        'sensor-data',
        bootstrap_servers=os.getenv('KAFKA_BROKERS', 'kafka:9092').split(','),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='clickhouse-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Подключение к ClickHouse
    ch_client = Client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.getenv('CLICKHOUSE_PORT', '9000')),
        user=os.getenv('CLICKHOUSE_USER', 'admin'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'admin123'),
        database='production'
    )
    
    batch = []
    batch_size = 50
    processed = 0
    
    logger.info("Consumer started. Waiting for messages...")
    
    try:
        for message in consumer:
            try:
                data = message.value
                
                # Преобразуем timestamp
                try:
                    timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                except:
                    timestamp = datetime.now()
                
                # Подготовка данных для ClickHouse
                insert_data = (
                    timestamp,
                    data['sensor_id'],
                    data['sensor_type'],
                    data['value'],
                    data['unit'],
                    data['status'],
                    data['metadata']['factory'],
                    data['metadata']['line'],
                    data['metadata']['machine_id'],
                    datetime.now()  # ingestion_time
                )
                
                batch.append(insert_data)
                
                # Вставляем батчем
                if len(batch) >= batch_size:
                    ch_client.execute(
                        'INSERT INTO production.sensor_data_raw (timestamp, sensor_id, sensor_type, value, unit, status, factory, line, machine_id, ingestion_time) VALUES',
                        batch
                    )
                    processed += len(batch)
                    logger.info(f"Inserted batch of {len(batch)} records. Total: {processed}")
                    batch = []
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Вставляем оставшиеся данные
        if batch:
            ch_client.execute(
                'INSERT INTO production.sensor_data_raw (timestamp, sensor_id, sensor_type, value, unit, status, factory, line, machine_id, ingestion_time) VALUES',
                batch
            )
            processed += len(batch)
            logger.info(f"Inserted final batch of {len(batch)} records. Total processed: {processed}")
        
        consumer.close()
        logger.info("Consumer stopped")

if __name__ == "__main__":
    main()
