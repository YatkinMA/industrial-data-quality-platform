"""
Простой консьюмер Kafka -> ClickHouse с обработкой данных
"""
import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
from clickhouse_driver import Client
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', 'kafka:9092').split(',')
        self.clickhouse_config = {
            'host': os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
            'port': int(os.getenv('CLICKHOUSE_PORT', '9000')),
            'user': os.getenv('CLICKHOUSE_USER', 'admin'),
            'password': os.getenv('CLICKHOUSE_PASSWORD', 'admin123'),
            'database': 'production'
        }
        
        self.batch_size = 100
        self.batch = []
        self.processed_count = 0
        
    def connect_to_kafka(self):
        """Подключение к Kafka с повторными попытками"""
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Connecting to Kafka (attempt {attempt + 1}/{max_retries})...")
                consumer = KafkaConsumer(
                    'sensor-data',
                    bootstrap_servers=self.kafka_brokers,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='data-processor',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info("Connected to Kafka successfully")
                return consumer
            except Exception as e:
                logger.warning(f"Failed to connect to Kafka: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
        
        raise Exception("Could not connect to Kafka after multiple attempts")
    
    def connect_to_clickhouse(self):
        """Подключение к ClickHouse"""
        try:
            client = Client(**self.clickhouse_config)
            # Проверяем подключение
            client.execute('SELECT 1')
            logger.info("Connected to ClickHouse successfully")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def process_message(self, message):
        """Обработка одного сообщения"""
        try:
            data = message.value
            
            # Проверяем качество данных
            if data.get('value') is None:
                logger.warning(f"Missing value in message: {data.get('sensor_id')}")
                return None
            
            # Преобразуем timestamp
            try:
                timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            except:
                timestamp = datetime.now()
            
            # Подготавливаем данные для ClickHouse
            processed_data = (
                timestamp,
                data['sensor_id'],
                data['sensor_type'],
                float(data['value']),
                data['unit'],
                data['status'],
                data['metadata']['factory'],
                data['metadata']['line'],
                data['metadata']['machine_id'],
                datetime.now(),  # ingestion_time
                self.calculate_anomaly_score(data)  # Дополнительное поле для анализа
            )
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return None
    
    def calculate_anomaly_score(self, data):
        """Простая эвристика для определения аномалий"""
        if data['status'] != 'NORMAL':
            return 1.0
        
        # Простая проверка на выбросы
        typical_ranges = {
            'temperature': (70, 80),
            'vibration': (2.0, 3.0),
            'pressure': (95, 105),
            'current': (14, 16)
        }
        
        sensor_type = data['sensor_type']
        value = data['value']
        
        if sensor_type in typical_ranges:
            min_val, max_val = typical_ranges[sensor_type]
            if value < min_val or value > max_val:
                return 0.8  # Потенциальная аномалия
        
        return 0.0
    
    def insert_batch(self, client):
        """Вставка батча данных в ClickHouse"""
        if not self.batch:
            return
        
        try:
            client.execute(
                '''
                INSERT INTO production.sensor_data_raw 
                (timestamp, sensor_id, sensor_type, value, unit, status, factory, line, machine_id, ingestion_time, anomaly_score) 
                VALUES
                ''',
                self.batch
            )
            
            self.processed_count += len(self.batch)
            logger.info(f"Inserted batch of {len(self.batch)} records. Total: {self.processed_count}")
            self.batch = []
            
        except Exception as e:
            logger.error(f"Error inserting batch to ClickHouse: {e}")
    
    def run(self):
        """Основной цикл обработки"""
        logger.info("Starting Data Processor...")
        
        # Подключаемся к сервисам
        consumer = self.connect_to_kafka()
        clickhouse_client = self.connect_to_clickhouse()
        
        logger.info("Data Processor started. Waiting for messages...")
        
        try:
            for message in consumer:
                # Обрабатываем сообщение
                processed_data = self.process_message(message)
                
                if processed_data:
                    self.batch.append(processed_data)
                    
                    # Вставляем батчем
                    if len(self.batch) >= self.batch_size:
                        self.insert_batch(clickhouse_client)
                
                # Также вставляем каждые 30 секунд, даже если батч не полный
                if time.time() % 30 < 1 and self.batch:
                    self.insert_batch(clickhouse_client)
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            # Вставляем оставшиеся данные
            if self.batch:
                self.insert_batch(clickhouse_client)
            
            consumer.close()
            logger.info(f"Data Processor stopped. Total processed: {self.processed_count}")

def main():
    processor = DataProcessor()
    processor.run()

if __name__ == "__main__":
    main()
