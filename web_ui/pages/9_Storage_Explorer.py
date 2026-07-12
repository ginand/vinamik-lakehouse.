import streamlit as st
import datetime
import pandas as pd
from utils import query_delta, get_tables

st.set_page_config(page_title="Storage Explorer & Data Catalog", page_icon="📁", layout="wide")

# CSS Header
st.markdown("""
<div style="background: linear-gradient(90deg, #11998e 0%, #38ef7d 100%); padding: 20px; border-radius: 10px; color: white; margin-bottom: 20px;">
    <h1 style="color: white; margin: 0;">📁 Enterprise Storage Explorer & Data Catalog</h1>
    <p style="margin: 0; font-size: 1.1em; opacity: 0.9;">Hệ thống duyệt, quản lý schema và theo dõi tài sản dữ liệu trên Azure Data Lake Storage Gen2 (vmlakehouse2024)</p>
</div>
""", unsafe_allow_html=True)

# Lấy danh sách bảng thực tế từ Azure DL
folders = {
    "bronze": get_tables("bronze") or ["transactions_bronze", "budget_plan_bronze", "misa_invoices_bronze"],
    "silver": get_tables("silver") or ["transactions_silver", "budget_plan_silver", "misa_invoices_silver", "general_ledger_silver", "fx_rates_silver", "ar_aging_silver", "ap_aging_silver"],
    "gold": get_tables("gold") or ["revenue_by_product_gold", "budget_vs_actual_gold", "ar_aging_gold", "ap_aging_gold", "cash_flow_summary_gold", "gl_trial_balance_gold", "dq_monitoring_gold"],
    "quarantine": get_tables("quarantine") or ["transactions_quarantine", "budget_plan_quarantine"]
}

total_datasets = sum(len(v) for v in folders.values())

# Đo lường tổng số dòng (Total Records) của Silver layer làm ví dụ đại diện
try:
    df_silver_sample, _ = query_delta("silver", "transactions_silver")
    total_silver_rows = len(df_silver_sample) if df_silver_sample is not None else 0
except Exception:
    total_silver_rows = "N/A"

# KPIs
col1, col2, col3, col4 = st.columns(4)
col1.metric("Cloud Provider", "Microsoft Azure", "ADLS Gen2")
col2.metric("Total Datasets", f"{total_datasets} Bảng", "Tất cả các Zone")
col3.metric("Sample Silver TXN", f"{total_silver_rows} Rows", "Dữ liệu thực tế")
col4.metric("Access Control", "Azure Entra ID", "RBAC Secured")

st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["🥉 Bronze Layer (Raw Data)", "🥈 Silver Layer (Cleaned Data)", "🥇 Gold Layer (Data Marts)", "🏥 Quarantine (Dirty Data)"])

def render_layer_catalog(container, tables):
    st.markdown(f"#### 🗄️ Phân vùng vật lý: `abfss://{container}@vmlakehouse2024.dfs.core.windows.net/`")
    
    # Create Catalog DataFrame
    catalog_data = []
    for t in tables:
        fmt = "Parquet" if container == "gold" else "Delta Lake"
        catalog_data.append({
            "Tên Bảng (Dataset)": t,
            "Định dạng (Format)": fmt,
            "URI vật lý (ADLS Gen2 Path)": f"abfss://{container}@.../{t}",
            "Trạng thái": "✅ Active"
        })
    
    st.dataframe(pd.DataFrame(catalog_data), use_container_width=True)
    
    st.markdown("---")
    st.markdown(f"#### **🔍 Trình khám phá Dữ liệu (Data Explorer)**")
    selected_folder = st.selectbox(f"Chọn bảng để khám phá (trong {container.upper()}):", tables, key=f"sel_{container}")
    
    with st.spinner("Đang truy xuất metadata và tải phân đoạn dữ liệu từ Azure..."):
        df, err = query_delta(container, selected_folder)
        
    if err:
        if "Bảng hiện tại đang trống" in err or "Không có bản ghi" in err:
            st.success("✅ Bảng đang trống (Không có bản ghi nào bị lỗi hoặc chưa có dữ liệu mới).")
        else:
            st.error(f"Lỗi truy xuất dữ liệu: {err}")
    elif df is not None and not df.empty:
        # Schema and Data
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("Số dòng (Rows)", f"{len(df):,}")
        col_m2.metric("Số cột (Columns)", f"{len(df.columns)}")
        col_m3.metric("Định dạng dữ liệu", "Parquet" if container == "gold" else "Delta Lake")
        
        col_schema, col_preview = st.columns([1, 2.5])
        
        with col_schema:
            st.markdown("**Cấu trúc cột (Schema / Dictionary)**")
            dtypes_df = df.dtypes.reset_index()
            dtypes_df.columns = ["Tên cột", "Kiểu dữ liệu (DType)"]
            st.dataframe(dtypes_df, use_container_width=True, height=350)
            
        with col_preview:
            st.markdown("**Bản xem trước (Data Preview - Top 100)**")
            st.dataframe(df.head(100), use_container_width=True, height=350)
        
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("⬇ Download toàn bộ CSV", data=csv, file_name=f"{selected_folder}.csv", mime="text/csv", key=f"btn_{container}_{selected_folder}")
    else:
        st.success("✅ Bảng đang trống.")

with tab1:
    render_layer_catalog("bronze", folders["bronze"])
with tab2:
    render_layer_catalog("silver", folders["silver"])
with tab3:
    render_layer_catalog("gold", folders["gold"])
with tab4:
    render_layer_catalog("quarantine", folders["quarantine"])
