import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_types = ['temperature', 'vibration', 'pressure', 'current']
factories = ['Plant_A', 'Plant_B']

logger.info("Simple generator started. Press Ctrl+C to stop.")

count = 0
try:
    while True:
        for sensor_id in range(5):
            data = {
                "timestamp": datetime.utcnow().isoformat(),
                "sensor_id": f"sensor_{sensor_id:03d}",
                "sensor_type": random.choice(sensor_types),
                "value": random.uniform(70, 80) if random.random() > 0.05 else random.uniform(100, 150),
                "unit": "C" if random.choice(sensor_types) == 'temperature' else 'unit',
                "status": "NORMAL" if random.random() > 0.05 else "SPIKE",
                "metadata": {
                    "factory": random.choice(factories),
                    "line": f"Line_{random.randint(1, 3)}",
                    "machine_id": f"CNC_{random.randint(1, 20):03d}"
                }
            }
            
            producer.send('sensor-data', data)
            count += 1
            
            if count % 50 == 0:
                logger.info(f"Sent {count} messages")
        
        time.sleep(0.5)
        
except KeyboardInterrupt:
    logger.info(f"Generator stopped. Total sent: {count}")
except Exception as e:
    logger.error(f"Error: {e}")
finally:
    producer.close()
