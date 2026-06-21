FROM python:3.12-slim

LABEL maintainer="VinaMilk Data Lakehouse"
LABEL description="VinaMilk Producers — MISA CSV / FX Rate / Budget Plan → Kafka/Event Hubs"

WORKDIR /app

# Cài thư viện từ root requirements.txt (shared với data_generator)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source producers
COPY producers/ .

# Tạo thư mục output cho MISA CSV
RUN mkdir -p /data/misa_exports

# Biến môi trường mặc định (override qua docker-compose)
ENV KAFKA_BOOTSTRAP=localhost:9092
ENV MISA_CSV_DIR=/data/misa_exports
ENV FX_POLL_INTERVAL_SECONDS=3600
ENV BUDGET_POLL_INTERVAL=300
ENV LOG_LEVEL=INFO

# Không có CMD mặc định — mỗi service trong docker-compose sẽ chỉ định CMD riêng
# Ví dụ:
#   command: ["python", "misa_csv_producer.py", "--loop"]
#   command: ["python", "fx_rate_producer.py",  "--loop", "--mock"]
#   command: ["python", "budget_plan_producer.py", "--loop"]
