-- clickhouse/init.sql
CREATE DATABASE IF NOT EXISTS production;

-- Исправляем TTL: используем DateTime вместо DateTime64 для TTL
CREATE TABLE IF NOT EXISTS production.sensor_data_raw
(
    timestamp DateTime64(3),
    sensor_id String,
    sensor_type Enum8('temperature' = 1, 'vibration' = 2, 'pressure' = 3, 'current' = 4),
    value Float64,
    unit String,
    status Enum8('NORMAL' = 1, 'STUCK' = 2, 'SPIKE' = 3, 'NOISE' = 4, 'MISSING' = 5),
    factory String,
    line String,
    machine_id String,
    ingestion_time DateTime DEFAULT now()  -- Используем DateTime для TTL
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (sensor_id, timestamp)
TTL ingestion_time + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS production.sensor_aggregates
(
    window_start DateTime,
    window_end DateTime,
    sensor_type String,
    machine_id String,
    avg_value Float64,
    std_value Float64,
    record_count UInt32,
    anomaly_count UInt32,
    processing_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (window_start, sensor_type, machine_id);

-- Простое материализованное представление без TTL
CREATE MATERIALIZED VIEW IF NOT EXISTS production.data_quality_daily
ENGINE = SummingMergeTree()
ORDER BY (factory, metric_date, metric_name) AS
SELECT
    factory,
    toDate(timestamp) as metric_date,
    'records_total' as metric_name,
    count() as metric_value
FROM production.sensor_data_raw
GROUP BY factory, metric_date;
