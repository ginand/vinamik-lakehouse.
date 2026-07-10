# VinaMilk Medallion Lakehouse 🥛

Dự án Xây dựng Nền tảng Dữ liệu (Data Lakehouse) cho VinaMilk sử dụng Kiến trúc Medallion, Apache Airflow, PySpark, Great Expectations, dbt và DuckDB, chạy trên môi trường Docker và Azure Data Lake Storage Gen2 (ADLS Gen2) + Azure Event Hubs.

---

## 🏗 Kiến trúc Hệ thống (Architecture)

Dự án mô phỏng quá trình thu thập và xử lý dữ liệu từ nhiều nguồn khác nhau (ERP, MISA, API, Google Sheets) theo thời gian thực (Streaming) và luồng Batch định kỳ thông qua kiến trúc 3 tầng chuẩn Data Lakehouse:

### 1. Hệ sinh thái Nguồn (Data Ingestion)
Chạy dưới dạng các Docker Container độc lập 24/7, đẩy dữ liệu vào **Azure Event Hubs**:
* **PostgreSQL (Mock ERP SAP S/4HANA):** Sinh dữ liệu giả lập cho các nghiệp vụ (Transactions, General Ledger, Accounts Receivable, Accounts Payable).
* **Debezium:** Bắt các sự kiện thay đổi từ PostgreSQL (CDC) và đẩy trực tiếp lên Event Hubs.
* **MISA CSV Producer:** Định kỳ đọc file hóa đơn xuất từ phần mềm kế toán MISA.
* **FX Rate Producer:** Lấy tỷ giá ngoại tệ từ ExchangeRate-API theo giờ.
* **Budget Plan Producer:** Lấy dữ liệu ngân sách kế hoạch từ Google Sheets.

### 2. Bronze Layer (Raw Data)
* **PySpark Structured Streaming:** Luồng chạy 24/7, liên tục lắng nghe tất cả các topic từ Azure Event Hubs và ghi dữ liệu thô (raw data) xuống ADLS Gen2 bằng định dạng `Delta Lake`. Dữ liệu được lưu trữ nguyên bản phục vụ tra cứu.

### 3. Silver Layer (Cleansed & Conformed)
* **PySpark Batch:** Đọc dữ liệu từ Bronze, chuẩn hóa định dạng, và áp dụng **Custom Data Quality Logic (Cờ lỗi PySpark)**.
* **Quarantine Pattern:** Các bản ghi vi phạm quy tắc (ví dụ: số tiền âm, sai mã tiền tệ, thiếu khóa chính) sẽ bị cách ly vào bảng Quarantine. Các bản ghi sạch được dùng cơ chế `MERGE INTO` (Upsert) vào bảng Silver.
* **Great Expectations (GX):** Thực hiện kiểm định hậu kỳ (Post-validation) trên bảng Silver và sinh báo cáo HTML (Data Docs) phục vụ giám sát chất lượng.

### 4. Gold Layer (Data Marts / OBT)
* **dbt (Data Build Tool) + DuckDB:** Truy vấn trực tiếp vào lớp Silver trên ADLS Gen2 bằng *DuckDB in-process*.
* Hệ thống xây dựng mô hình **One Big Table (OBT)** / Data Mart phẳng để phục vụ Power BI, bao gồm 7 bảng phân tích chính:
  1. `revenue_by_product_gold`: Tổng hợp doanh thu theo sản phẩm.
  2. `ar_aging_gold`: Phân tích tuổi nợ phải thu.
  3. `ap_aging_gold`: Phân tích công nợ phải trả.
  4. `budget_vs_actual_gold`: So sánh ngân sách và chi phí thực tế.
  5. `gl_trial_balance_gold`: Tổng hợp số dư và bảng cân đối phát sinh.
  6. `cash_flow_summary_gold`: Tổng hợp dòng tiền vào/ra.
  7. `dq_monitoring_gold`: Thống kê chất lượng dữ liệu phục vụ giám sát.
* Áp dụng dbt tests (`not_null`, `accepted_values`) để đảm bảo chất lượng dữ liệu đầu ra.

### 5. Orchestration (Apache Airflow)
* Quản lý tiến trình xử lý Batch ở nửa sau của hệ thống thông qua một DAG gồm 4 bước tuần tự:
  `Silver Batch (PySpark) → DQ Health Check → GX Validation → Gold dbt Run`
* **Fail-fast mechanism:** Tại task `DQ Health Check`, nếu tỷ lệ bản ghi lỗi trong bảng Quarantine > 5%, Airflow sẽ tự động ngắt toàn bộ pipeline để ngăn chặn dữ liệu bẩn lọt vào Gold.

### 6. Visualization
* **Power BI:** Kết nối trực tiếp vào các bảng Data Mart ở lớp Gold qua chế độ **DirectQuery**, giúp Dashboard luôn cập nhật và tối ưu tốc độ truy vấn mà không cần mô hình Data Model phức tạp.

---

## 🛠 Công nghệ Sử dụng (Tech Stack)

* **Ngôn ngữ:** Python 3.11, SQL
* **Xử lý Dữ liệu:** Apache Spark (PySpark), dbt Core, DuckDB
* **Kiểm định Chất lượng (Data Quality):** Great Expectations
* **Message Broker / CDC:** Azure Event Hubs (Kafka API), Debezium
* **Lưu trữ:** Azure Data Lake Storage Gen2 (ADLS Gen2), Delta Lake
* **Điều phối (Orchestration):** Apache Airflow
* **Hạ tầng (Infrastructure):** Docker & Docker Compose

---

## 🚀 Hướng dẫn Cài đặt & Chạy

### Yêu cầu trước khi chạy
* Đã cài đặt [Docker Desktop](https://www.docker.com/products/docker-desktop/) (cấp phát tối thiểu 8GB RAM).
* Có tài khoản Azure, đã tạo Storage Account (ADLS Gen2) và Event Hubs Namespace.

### 1. Cấu hình biến môi trường
Tạo file `.env` ở thư mục gốc (ngang hàng với `docker-compose.yml`) và điền thông tin Azure của bạn:
```env
AZURE_STORAGE_ACCOUNT_NAME=tên_storage_account_của_bạn
AZURE_STORAGE_ACCOUNT_KEY=khóa_truy_cập_của_bạn
EVENT_HUBS_NAMESPACE=tên_namespace_event_hubs
EVENT_HUBS_CONNECTION_STRING=Endpoint=sb://...
```

### 2. Khởi chạy toàn bộ hệ thống
Mở terminal tại thư mục gốc của dự án và chạy:
```bash
docker compose up -d --build
```

### 3. Theo dõi & Quản lý
Khi các Container báo trạng thái `healthy`, bạn có thể truy cập:
* **Apache Airflow UI:** [http://localhost:8888](http://localhost:8888) (Tài khoản: `admin` / Mật khẩu: `admin`)
* **Great Expectations Data Docs:** Mở file HTML trong thư mục `dags/gx_data_docs/` sau khi DAG chạy thành công.

### 4. Chạy luồng xử lý dữ liệu (Trigger DAG)
* Mở **Airflow UI**.
* Bật nút Toggle để unpause DAG `vinamik_lakehouse_pipeline`.
* Nhấn nút **▶ Trigger DAG** để kích hoạt luồng xử lý từ Silver lên Gold.

---

## 📁 Cấu trúc Thư mục

```text
vinamik-lakehouse/
├── airflow/               # Dockerfile và thư viện cho Airflow
├── dags/                  # DAG điều phối Airflow (4 tasks)
├── dbt_gold/              # dbt project xử lý dữ liệu tầng Gold (7 models)
├── spark/                 # PySpark (Bronze streaming, Silver batch, DQ checks)
├── producers/             # Scripts đẩy dữ liệu (MISA, FX API, Budget Sheets)
├── data_generator/        # Script giả lập ERP Transactions
├── debezium-standalone/   # Cấu hình Debezium CDC
├── postgres-init/         # Script khởi tạo Database PostgreSQL (Mock ERP)
├── docker-compose.yml     # Quản lý toàn bộ hệ thống container (Airflow, Postgres...)
└── .env                   # File chứa keys kết nối bảo mật (Gitignored)
```
