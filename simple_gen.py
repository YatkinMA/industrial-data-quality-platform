#!/usr/bin/env python3
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("Starting SUPER SIMPLE generator...")

# Подключаемся к Kafka
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

count = 0
try:
    while True:
        # Простые данные
        data = {
            "timestamp": datetime.utcnow().isoformat(),
            "sensor_id": f"sensor_{count % 10:03d}",
            "value": random.uniform(70, 80),
            "unit": "C"
        }
        
        # Отправляем
        future = producer.send('sensor-data', data)
        future.get(timeout=10)  # Ждем подтверждения
        
        count += 1
        if count % 50 == 0:
            print(f"✅ Sent {count} messages")
        
        time.sleep(0.5)  # Пауза между сообщениями
        
except KeyboardInterrupt:
    print(f"\nStopped. Total sent: {count}")
except Exception as e:
    print(f"ERROR: {e}")
finally:
    producer.close()
    print("Producer closed")
