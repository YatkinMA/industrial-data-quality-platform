#!/bin/bash
echo "=== CHECKING DATA IN CLICKHOUSE ==="
echo

# Проверяем таблицы
echo "1. Tables in 'production' database:"
docker-compose exec clickhouse clickhouse-client \
  --user admin \
  --password admin123 \
  --query "
SHOW TABLES FROM production"

# Проверяем данные в каждой таблице
echo -e "\n2. Data in tables:"
docker-compose exec clickhouse clickhouse-client \
  --user admin \
  --password admin123 \
  --query "
SELECT 
    name as table_name,
    total_rows as rows
FROM system.tables 
WHERE database = 'production'
ORDER BY rows DESC"

# Проверяем sensor_data_raw детально
echo -e "\n3. sensor_data_raw details:"
docker-compose exec clickhouse clickhouse-client \
  --user admin \
  --password admin123 \
  --query "
SELECT 
    count() as total_records,
    min(timestamp) as oldest,
    max(timestamp) as newest,
    count(distinct sensor_id) as unique_sensors,
    round(avg(anomaly_score), 3) as avg_anomaly_score
FROM production.sensor_data_raw"

# Проверяем материализованные представления
echo -e "\n4. Materialized views data:"
docker-compose exec clickhouse clickhouse-client \
  --user admin \
  --password admin123 \
  --query "
SELECT 
    'data_quality_daily' as view,
    count() as records,
    min(metric_date) as start_date,
    max(metric_date) as end_date
FROM production.data_quality_metrics
UNION ALL
SELECT 
    'sensor_aggregates_1min' as view,
    count() as records,
    min(minute) as start_time,
    max(minute) as end_time
FROM production.sensor_aggregates_1min"

echo -e "\n=== CHECK COMPLETE ==="
