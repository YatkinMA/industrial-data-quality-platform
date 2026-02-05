# generator/app.py (исправленная версия)
import json
import time
import random
import logging
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SensorDataGenerator:
    def __init__(self, kafka_broker='kafka:9092', max_retries=30, retry_delay=5):
        self.kafka_broker = kafka_broker
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.producer = None
        
    def connect_to_kafka(self):
        """Пытается подключиться к Kafka с повторными попытками"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Attempt {attempt + 1}/{self.max_retries} to connect to Kafka at {self.kafka_broker}")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_broker,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                
                # Проверяем подключение отправкой тестового сообщения
                self.producer.send('sensor-data-test', value={'test': True})
                self.producer.flush(timeout=10)
                
                logger.info(f"Successfully connected to Kafka at {self.kafka_broker}")
                return True
                
            except NoBrokersAvailable as e:
                logger.warning(f"Kafka not available yet. Waiting {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"Error connecting to Kafka: {e}")
                time.sleep(self.retry_delay)
        
        logger.error(f"Failed to connect to Kafka after {self.max_retries} attempts")
        return False
    
    def generate_sensor_data(self, sensor_id):
        """Генерация одного показания датчика"""
        sensor_type = random.choice(['temperature', 'vibration', 'pressure', 'current'])
        
        base_values = {
            'temperature': 75.0,
            'vibration': 2.5,
            'pressure': 100.0,
            'current': 15.0
        }
        
        base_value = base_values[sensor_type]
        
        # 5% вероятность аномалии
        if random.random() < 0.05:
            anomaly_type = random.choice(['spike', 'stuck', 'noise', 'missing'])
        else:
            anomaly_type = 'normal'
        
        if anomaly_type == 'missing':
            value = None
        elif anomaly_type == 'stuck':
            value = base_value
        elif anomaly_type == 'spike':
            value = base_value + random.uniform(20, 50)
        elif anomaly_type == 'noise':
            value = base_value + random.gauss(0, base_value * 0.2)
        else:
            value = base_value + random.gauss(0, base_value * 0.05)
        
        data = {
            "timestamp": datetime.utcnow().isoformat(),
            "sensor_id": f"sensor_{sensor_id:03d}",
            "sensor_type": sensor_type,
            "value": round(value, 3) if value is not None else None,
            "unit": self._get_unit(sensor_type),
            "status": anomaly_type.upper(),
            "metadata": {
                "factory": random.choice(['Plant_A', 'Plant_B']),
                "line": random.choice(['Assembly_Line_1', 'Assembly_Line_2']),
                "machine_id": f"CNC_{random.randint(1, 20):03d}"
            }
        }
        
        return data
    
    def _get_unit(self, sensor_type):
        units = {
            'temperature': 'C',
            'vibration': 'mm/s',
            'pressure': 'kPa',
            'current': 'A'
        }
        return units.get(sensor_type, 'unit')
    
    def run(self, sensor_count=5, interval=1.0):
        """Основной цикл генерации данных"""
        if not self.connect_to_kafka():
            return
        
        logger.info(f"Starting data generation for {sensor_count} sensors with interval {interval}s")
        sensor_counter = 0
        
        try:
            while True:
                for sensor_id in range(sensor_count):
                    data = self.generate_sensor_data(sensor_id)
                    
                    try:
                        # Отправка в Kafka
                        future = self.producer.send('sensor-data', value=data)
                        # Ждем подтверждения
                        future.get(timeout=10)
                        
                        sensor_counter += 1
                        if sensor_counter % 10 == 0:
                            logger.info(f"Sent {sensor_counter} messages")
                        else:
                            logger.debug(f"Sent: {data['sensor_id']} = {data['value']}")
                            
                    except Exception as e:
                        logger.error(f"Failed to send message: {e}")
                        # Попробуем переподключиться
                        if not self.connect_to_kafka():
                            return
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Shutting down generator...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            if self.producer:
                self.producer.close()

if __name__ == "__main__":
    kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
    sensor_count = int(os.getenv('SENSOR_COUNT', '5'))
    
    logger.info(f"Initializing SensorDataGenerator with Kafka at {kafka_broker}")
    generator = SensorDataGenerator(kafka_broker)
    generator.run(sensor_count=sensor_count, interval=0.5)
