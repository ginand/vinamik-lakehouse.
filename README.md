# VinaMilk Medallion Lakehouse 🥛

Dự án Xây dựng Nền tảng Dữ liệu (Data Lakehouse) cho VinaMilk sử dụng Kiến trúc Medallion, Apache Airflow, PySpark, dbt và DuckDB, chạy trên môi trường Docker Local và Azure Data Lake Storage Gen2.

---

## 🏗 Kiến trúc Hệ thống (Architecture)

Dự án mô phỏng quá trình thu thập và xử lý dữ liệu ERP (SAP S/4HANA) theo thời gian thực (CDC) và luồng Batch định kỳ thông qua kiến trúc 3 tầng chuẩn Data Lakehouse:

1. **Hệ sinh thái Nguồn (Source/Ingestion):**
   * **PostgreSQL (Mock ERP):** Sinh dữ liệu giả lập cho các nghiệp vụ (Doanh thu, Mua hàng, Công nợ...).
   * **Debezium + Kafka:** Bắt các thay đổi từ PostgreSQL (CDC) và đẩy thành Message stream vào Kafka.
2. **Bronze Layer (Raw Data):**
   * **PySpark Streaming:** Lắng nghe Kafka và ghi dữ liệu thô xuống ADLS Gen2 bằng định dạng `Delta Lake`.
3. **Silver Layer (Cleansed & Conformed):**
   * **PySpark Batch:** Chạy định kỳ, đọc dữ liệu Bronze, làm sạch, áp dụng Data Quality Rules (DQ Flags), và thực hiện `MERGE` (Upsert) vào bảng Silver.
4. **Gold Layer (Business KPIs):**
   * **dbt (Data Build Tool) + DuckDB:** Đọc dữ liệu từ bảng Silver trên Azure bằng *DuckDB in-process* thông qua extension `azure` & `delta`. Biến đổi bằng các SQL Models (`.sql`) và ghi lại các bảng phân tích KPI (Doanh thu, Dòng tiền, Công nợ) xuống ADLS Gen2.
5. **Orchestration:**
   * **Apache Airflow:** Quản lý lịch trình chạy định kỳ của luồng Silver Batch và Gold dbt.

---

## 🛠 Công nghệ Sử dụng (Tech Stack)

* **Ngôn ngữ:** Python 3.11, SQL
* **Xử lý Dữ liệu:** Apache Spark (PySpark), dbt Core, DuckDB
* **Message Broker / CDC:** Apache Kafka, Confluent Schema Registry, Debezium
* **Lưu trữ:** Azure Data Lake Storage Gen2 (ADLS Gen2), Delta Lake
* **Điều phối (Orchestration):** Apache Airflow (chạy bằng PostgreSQL backend)
* **Hạ tầng (Infrastructure):** Docker & Docker Compose

---

## 🚀 Hướng dẫn Cài đặt & Chạy (How to run)

### Yêu cầu trước khi chạy
* Đã cài đặt [Docker Desktop](https://www.docker.com/products/docker-desktop/) (cấp phát tối thiểu 8GB RAM).
* Có tài khoản Azure và đã tạo một Storage Account (ADLS Gen2).

### 1. Cấu hình biến môi trường
Tạo file `.env` ở thư mục gốc (ngang hàng với `docker-compose.yml`) và điền thông tin Azure của bạn:
```env
AZURE_STORAGE_ACCOUNT_NAME=tên_storage_account_của_bạn
AZURE_STORAGE_ACCOUNT_KEY=khóa_truy_cập_của_bạn
EVENT_HUBS_CONNECTION_STRING=tùy_chọn_nếu_dùng_event_hub
```

### 2. Khởi chạy toàn bộ hệ thống
Mở terminal tại thư mục gốc của dự án và chạy:
```bash
docker compose up -d --build
```
*Lưu ý: Lần chạy đầu tiên sẽ mất khoảng 5-10 phút để tải các Docker Images và cài đặt các thư viện Python (PySpark, dbt-duckdb).*

### 3. Theo dõi & Quản lý

Khi các Container báo trạng thái `healthy`, bạn có thể truy cập các công cụ giám sát:

* **Apache Airflow UI:** [http://localhost:8888](http://localhost:8888) (Tài khoản: `admin` / Mật khẩu: `admin`)
* **Kafka UI:** [http://localhost:8080](http://localhost:8080)

### 4. Chạy luồng xử lý dữ liệu (Trigger DAG)
* Mở **Airflow UI**.
* Bật nút Toggle để unpause DAG `vinamik_lakehouse_pipeline`.
* Nhấn nút **▶ Trigger DAG** để kích hoạt luồng xử lý dữ liệu từ Silver lên Gold ngay lập tức. DAG sẽ tự động chạy định kỳ mỗi 15 phút.

---

## 📁 Cấu trúc Thư mục

```text
vinamik-lakehouse/
├── airflow/               # Dockerfile và thư viện (requirements) cho Airflow
├── dags/                  # Chứa file cấu hình DAG điều phối của Airflow
├── dbt_gold/              # Dự án dbt xử lý dữ liệu tầng Gold (models, macros)
├── spark/                 # Các mã nguồn PySpark (Bronze streaming, Silver batch)
├── scripts/               # Các shell/bat scripts tiện ích
├── postgres-init/         # Script khởi tạo Database PostgreSQL (Mock ERP)
├── docker-compose.yml     # Quản lý toàn bộ hệ thống container (14 services)
└── .env                   # File chứa keys kết nối bảo mật (Gitignored)
```
