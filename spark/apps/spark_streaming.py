# spark/apps/spark_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Конфигурация
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka:9092')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_PORT', '8123')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'admin')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')
CLICKHOUSE_DB = 'production'

def create_spark_session():
    """Создание Spark сессии с необходимыми конфигурациями"""
    return SparkSession.builder \
        .appName("KafkaToClickHouse") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.clickhouse:clickhouse-jdbc:0.4.6,"
                "com.clickhouse:clickhouse-spark-runtime:0.4.6") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

def main():
    print("Starting Spark Streaming job...")
    
    # Создаем Spark сессию
    spark = create_spark_session()
    
    # Схема для данных сенсоров
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("sensor_id", StringType()),
        StructField("sensor_type", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("status", StringType()),
        StructField("metadata", StructType([
            StructField("factory", StringType()),
            StructField("line", StringType()),
            StructField("machine_id", StringType())
        ]))
    ])
    
    # Чтение из Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", "sensor-data") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Парсим JSON и преобразуем данные
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select(
        "data.*"
    ).withColumn(
        "timestamp", to_timestamp(col("timestamp"))
    ).withColumn(
        "factory", col("metadata.factory")
    ).withColumn(
        "line", col("metadata.line")
    ).withColumn(
        "machine_id", col("metadata.machine_id")
    ).drop("metadata")
    
    # Выводим схему для отладки
    parsed_df.printSchema()
    
    # Функция для записи каждой батча в ClickHouse
    def write_to_clickhouse(batch_df, batch_id):
        if batch_df.count() > 0:
            print(f"Processing batch {batch_id} with {batch_df.count()} records")
            
            # Подготовка данных
            batch_df = batch_df.withColumn(
                "ingestion_time", current_timestamp()
            )
            
            # Запись в ClickHouse
            batch_df.write \
                .format("jdbc") \
                .mode("append") \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("url", f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}") \
                .option("dbtable", "sensor_data_raw") \
                .option("user", CLICKHOUSE_USER) \
                .option("password", CLICKHOUSE_PASSWORD) \
                .option("batchsize", 1000) \
                .save()
            
            print(f"Batch {batch_id} written to ClickHouse")
    
    # Настраиваем стриминг
    query = parsed_df.writeStream \
        .foreachBatch(write_to_clickhouse) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/tmp/checkpoints/kafka-clickhouse") \
        .start()
    
    # Ждем завершения
    query.awaitTermination()

if __name__ == "__main__":
    main()
