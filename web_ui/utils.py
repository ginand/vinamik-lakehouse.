import os
import duckdb
import streamlit as st

@st.cache_resource
def get_duckdb_conn():
    conn = duckdb.connect(':memory:')
    
    # Required to securely load CA certs in docker
    conn.execute("SET CA_CERT_FILE='/etc/ssl/certs/ca-certificates.crt';")
    
    # Load required extensions
    conn.execute("INSTALL httpfs; LOAD httpfs; INSTALL azure; LOAD azure; INSTALL delta; LOAD delta;")
    
    # Fetch connection string from environment
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    if not conn_str:
        st.error("Missing AZURE_STORAGE_CONNECTION_STRING environment variable.")
        st.stop()
        
    # Replace HTTPS with HTTP to avoid IMDS credential timeout bug on local duckdb setups
    conn_str = conn_str.replace("https", "http")
    
    # Create azure secret for DuckDB
    query = f"""
        CREATE OR REPLACE SECRET azure_secret (
            TYPE azure, 
            PROVIDER config, 
            CONNECTION_STRING '{conn_str}'
        );
    """
    conn.execute(query)
    return conn

@st.cache_data(ttl=300, show_spinner=False)
def query_delta(container, path, recent_only=False):
    conn = get_duckdb_conn()
    account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "vmlakehouse2024")
    
    # Map logic: pipeline writes quarantine tables with '_silver' suffix
    physical_path = path
    if container == "quarantine" and path.endswith("_quarantine"):
        physical_path = path.replace("_quarantine", "_silver")
        
    full_path = f"abfss://{container}@{account}.dfs.core.windows.net/{physical_path}"
    
    where_clause = ""
    if recent_only:
        if container == "silver":
            where_clause = "WHERE CAST(_silver_loaded_at AS DATE) = CURRENT_DATE()"
        elif container == "gold":
            where_clause = "WHERE CAST(_gold_computed_at AS DATE) = CURRENT_DATE()"
        elif container == "quarantine":
            where_clause = "WHERE CAST(_quarantined_at AS DATE) = CURRENT_DATE()"
        else:
            where_clause = "LIMIT 1000"

    try:
        if container == "gold":
            query = f"SELECT * FROM read_parquet('{full_path}.parquet') {where_clause}"
        else:
            query = f"SELECT * FROM delta_scan('{full_path}') {where_clause}"
            
        df = conn.execute(query).df()
        return df, None
    except Exception as e:
        err_msg = str(e)
        # Bắt lỗi khi bảng Delta trống hoặc chưa tồn tại (Rất hay gặp ở Quarantine layer khi không có lỗi DQ nào)
        if "No files in log segment" in err_msg or "404" in err_msg or "Path does not exist" in err_msg or "not a Delta table" in err_msg or "No metadata found" in err_msg:
            err_msg = "Bảng hiện tại đang trống (Chưa có dữ liệu nào được ghi vào)."
        return None, err_msg

def query_sql(sql_query):
    conn = get_duckdb_conn()
    try:
        df = conn.execute(sql_query).df()
        return df, None
    except Exception as e:
        return None, str(e)

def get_tables(container):
    # Centralized Data Catalog (Single source of truth)
    # Vì DuckDB HTTPFS không hỗ trợ list thư mục (directories) qua abfss:// protocol,
    # chúng ta quản lý danh mục bảng tại đây.
    catalog = {
        "bronze": ["transactions_bronze", "budget_plan_bronze", "misa_invoices_bronze"],
        "silver": [
            "transactions_silver", "budget_plan_silver", "misa_invoices_silver", 
            "general_ledger_silver", "fx_rates_silver", "ar_aging_silver", "ap_aging_silver"
        ],
        "gold": [
            "revenue_by_product_gold", "budget_vs_actual_gold", "ar_aging_gold", 
            "ap_aging_gold", "cash_flow_summary_gold", "gl_trial_balance_gold", "dq_monitoring_gold"
        ],
        "quarantine": ["transactions_quarantine", "budget_plan_quarantine"]
    }
    return catalog.get(container, [])
