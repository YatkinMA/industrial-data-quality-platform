# processor/simple_processor.py
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from clickhouse_driver import Client
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Подключаемся к Kafka
    consumer = KafkaConsumer(
        'sensor-data',
        bootstrap_servers=os.getenv('KAFKA_BROKERS', 'kafka:9092').split(','),
        auto_offset_reset='earliest',  # Читаем с начала
        enable_auto_commit=True,
        group_id='clickhouse-processor',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Подключаемся к ClickHouse
    client = Client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.getenv('CLICKHOUSE_PORT', '9000')),
        user=os.getenv('CLICKHOUSE_USER', 'admin'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'admin123'),
        database='production'
    )
    
    logger.info("Starting simple processor...")
    batch = []
    processed = 0
    batch_size = 100
    
    try:
        for message in consumer:
            try:
                data = message.value
                
                # Подготавливаем данные
                record = (
                    datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00')),
                    data['sensor_id'],
                    data['sensor_type'],
                    float(data['value']) if data['value'] is not None else 0.0,
                    data['unit'],
                    data['status'],
                    data['metadata']['factory'],
                    data['metadata']['line'],
                    data['metadata']['machine_id'],
                    datetime.now()
                )
                
                batch.append(record)
                
                # Вставляем батчем
                if len(batch) >= batch_size:
                    client.execute(
                        '''
                        INSERT INTO production.sensor_data_raw 
                        (timestamp, sensor_id, sensor_type, value, unit, status, factory, line, machine_id, ingestion_time) 
                        VALUES
                        ''',
                        batch
                    )
                    processed += len(batch)
                    logger.info(f"Inserted {len(batch)} records. Total: {processed}")
                    batch = []
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Stopping processor...")
    finally:
        # Вставляем остатки
        if batch:
            client.execute(
                'INSERT INTO production.sensor_data_raw VALUES',
                batch
            )
            processed += len(batch)
            logger.info(f"Final insert: {len(batch)} records")
        
        logger.info(f"Total processed: {processed}")
        consumer.close()

if __name__ == "__main__":
    main()
    