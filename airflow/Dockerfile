FROM apache/airflow:2.9.2-python3.11

USER root

# Cài Java (cần cho PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Cài các thư viện Python cần thiết
COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
