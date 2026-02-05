FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    clickhouse-driver==0.2.6 \
    pandas==2.1.0

COPY processor.py .

CMD ["python", "processor.py"]
