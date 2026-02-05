#!/usr/bin/env python3
import json
import time
import random
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_kafka_producer_with_retry(max_retries=10, retry_delay=5):
    """Создает продюсера с повторными попытками подключения"""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092').split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=30000,
                retries=3,
                retry_backoff_ms=1000
            )
            # Проверяем подключение
            producer.list_topics(timeout=10)
            logger.info(f"✅ Подключение к Kafka успешно (попытка {attempt + 1})")
            return producer
        except (NoBrokersAvailable, KafkaTimeoutError) as e:
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ Попытка {attempt + 1}/{max_retries}: Kafka недоступна. Повтор через {retry_delay}с...")
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ Не удалось подключиться к Kafka после {max_retries} попыток")
                raise
    
    raise NoBrokersAvailable("Не удалось подключиться к Kafka")

def main():
    logger.info("🚀 Запуск генератора данных...")
    
    # Подключаемся к Kafka с повторными попытками
    producer = create_kafka_producer_with_retry()
    
    topic = os.getenv('KAFKA_TOPIC', 'sensor-data')
    delay_ms = float(os.getenv('GENERATOR_DELAY_MS', '500')) / 1000.0
    
    count = 0
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    try:
        while True:
            try:
                # Генерируем данные
                data = {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "sensor_id": f"sensor_{count % 10:03d}",
                    "value": round(random.uniform(70, 80), 2),
                    "unit": "C",
                    "batch_id": count // 100,
                    "message_id": count
                }
                
                # Отправляем с обработкой ошибок
                future = producer.send(topic, data)
                future.get(timeout=10)
                
                count += 1
                consecutive_errors = 0  # Сбрасываем счетчик ошибок
                
                if count % 20 == 0:
                    logger.info(f"📨 Отправлено сообщений: {count}")
                
                time.sleep(delay_ms)
                
            except (KafkaTimeoutError, Exception) as e:
                consecutive_errors += 1
                logger.error(f"❌ Ошибка отправки (ошибка подряд: {consecutive_errors}): {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("⚠️ Слишком много ошибок подряд. Переподключение...")
                    producer.close()
                    time.sleep(5)
                    producer = create_kafka_producer_with_retry(max_retries=3)
                    consecutive_errors = 0
                
                time.sleep(2)  # Пауза перед повторной попыткой
                
    except KeyboardInterrupt:
        logger.info(f"🛑 Остановка. Всего отправлено: {count}")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
    finally:
        producer.close()
        logger.info("🔌 Продюсер закрыт")

if __name__ == "__main__":
    main()