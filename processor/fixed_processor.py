# processor/fixed_processor.py
import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
from clickhouse_driver import Client
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting Fixed Data Processor")

    # Подключение к ClickHouse
    try:
        client = Client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
            user=os.getenv("CLICKHOUSE_USER", "admin"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "admin123"),
            database="production",
            settings={"use_numpy": False},
        )
        logger.info("Connected to ClickHouse")
    except Exception as e:
        logger.error(f"ClickHouse connection failed: {e}")
        return

    # Подключение к Kafka
    try:
        consumer = KafkaConsumer(
            "sensor-data",
            bootstrap_servers=os.getenv("KAFKA_BROKERS", "kafka:9092").split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Временно отключаем авто-коммит
            group_id=None,  # Не используем группу для начала
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            consumer_timeout_ms=10000,
        )
        logger.info("Connected to Kafka")
    except Exception as e:
        logger.error(f"Kafka connection failed: {e}")
        return

    batch = []
    batch_size = 50
    processed = 0

    logger.info("Starting to process messages...")

    try:
        for message in consumer:
            try:
                data = message.value

                # Преобразуем timestamp
                ts_str = data["timestamp"]
                if "Z" in ts_str:
                    ts_str = ts_str.replace("Z", "+00:00")

                timestamp = datetime.fromisoformat(ts_str)

                # Подготавливаем запись
                record = (
                    timestamp,
                    data["sensor_id"],
                    data["sensor_type"],
                    float(data["value"]) if data["value"] is not None else 0.0,
                    data["unit"],
                    data["status"],
                    data["metadata"]["factory"],
                    data["metadata"]["line"],
                    data["metadata"]["machine_id"],
                    datetime.now(),  # ingestion_time
                    0.0,  # anomaly_score
                )

                batch.append(record)

                # Вставляем батчем
                if len(batch) >= batch_size:
                    client.execute(
                        "INSERT INTO production.sensor_data_raw VALUES", batch
                    )
                    processed += len(batch)
                    logger.info(
                        f"Inserted batch of {len(batch)} records. Total: {processed}"
                    )
                    batch = []

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue

        # Вставляем остатки
        if batch:
            client.execute("INSERT INTO production.sensor_data_raw VALUES", batch)
            processed += len(batch)
            logger.info(f"Inserted final batch of {len(batch)} records")

    except Exception as e:
        logger.error(f"Processing error: {e}")
    finally:
        consumer.close()
        logger.info(f"Processor stopped. Total processed: {processed}")


if __name__ == "__main__":
    main()
