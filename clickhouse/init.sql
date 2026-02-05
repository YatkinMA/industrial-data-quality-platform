-- clickhouse/init.sql (обновленная версия)
CREATE DATABASE IF NOT EXISTS production;

-- Основная таблица с данными сенсоров
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
    ingestion_time DateTime DEFAULT now(),
    anomaly_score Float64 DEFAULT 0.0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (sensor_id, timestamp)
TTL ingestion_time + INTERVAL 30 DAY;

-- Агрегированные данные по минутам
CREATE TABLE IF NOT EXISTS production.sensor_aggregates_1min
(
    minute DateTime,
    sensor_type String,
    factory String,
    avg_value Float64,
    min_value Float64,
    max_value Float64,
    record_count UInt32,
    anomaly_count UInt32
)
ENGINE = AggregatingMergeTree()
ORDER BY (minute, sensor_type, factory);

-- Материализованное представление для агрегации
CREATE MATERIALIZED VIEW IF NOT EXISTS production.sensor_aggregates_1min_mv
TO production.sensor_aggregates_1min AS
SELECT
    toStartOfMinute(timestamp) as minute,
    sensor_type,
    factory,
    avg(value) as avg_value,
    min(value) as min_value,
    max(value) as max_value,
    count() as record_count,
    countIf(anomaly_score > 0.5) as anomaly_count
FROM production.sensor_data_raw
GROUP BY minute, sensor_type, factory;

-- Таблица для метрик качества данных
CREATE TABLE IF NOT EXISTS production.data_quality_metrics
(
    metric_date Date,
    metric_name String,
    metric_value Float64,
    factory String
)
ENGINE = SummingMergeTree()
ORDER BY (metric_date, metric_name, factory);

-- Представление для мониторинга качества
CREATE MATERIALIZED VIEW IF NOT EXISTS production.data_quality_daily
TO production.data_quality_metrics AS
SELECT
    toDate(timestamp) as metric_date,
    'completeness' as metric_name,
    countIf(value IS NOT NULL) / count() as metric_value,
    factory
FROM production.sensor_data_raw
GROUP BY metric_date, factory;