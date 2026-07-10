FROM python:3.12-slim

LABEL maintainer="VinaMilk Data Lakehouse"
LABEL description="ERP Data Generator — Mock SAP S/4HANA transactions"

WORKDIR /app

# Cài thư viện từ root requirements.txt (shared với producers)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy generator source
COPY data_generator/ .

# Default environment (overridable via docker-compose)
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=vinamik_erp
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASS=1
ENV LOG_LEVEL=INFO

# Default: continuous mode at normal speed
CMD ["python", "generate_erp_data.py", "--speed", "normal"]
