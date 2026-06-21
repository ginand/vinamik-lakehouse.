FROM apache/spark:3.5.0-python3

LABEL maintainer="VinaMilk Data Lakehouse"
LABEL description="PySpark Streaming — Bronze Layer Ingestion (Event Hubs → ADLS Gen2 Delta)"

# Phải dùng root để cài thư viện và tạo thư mục cache
USER root

# Cài đặt thư viện Python
RUN pip install --no-cache-dir python-dotenv==1.0.1

# Tạo thư mục .ivy2 cho user spark (Spark dùng Ivy để download JAR lúc runtime)
RUN mkdir -p /home/spark/.ivy2/cache \
             /home/spark/.ivy2/jars \
             /home/spark/.ivy2/local \
    && chown -R spark:spark /home/spark/.ivy2

# Quay lại user spark (bảo mật)
USER spark

WORKDIR /opt/spark/work-dir

# Copy toàn bộ source code vào container
COPY *.py .

# Chạy với spark-submit, truyền packages để JVM load đúng JAR
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-azure:3.3.4", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "bronze_streaming.py"]
