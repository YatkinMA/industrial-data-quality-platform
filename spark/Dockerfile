FROM bitnami/spark:3.5

USER root

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    clickhouse-driver==0.2.6 \
    pandas==2.1.0

# Копируем JAR файлы
COPY jars/*.jar /opt/bitnami/spark/jars/

# Копируем приложение
WORKDIR /app
COPY apps/ .

# Меняем владельца
RUN chown -R 1001:1001 /opt/bitnami/spark/jars /app
USER 1001
EOF
