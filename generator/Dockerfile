FROM python:3.9-slim

WORKDIR /app

# Копируем зависимости отдельно для кэширования
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY simple_gen.py .

# Запуск с ретраями
CMD ["python", "simple_gen.py"]