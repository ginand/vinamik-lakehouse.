import streamlit as st
from utils import query_delta, get_tables

st.set_page_config(page_title="Silver Layer", page_icon="🥈", layout="wide")
st.markdown("""
<div style="background: linear-gradient(90deg, #bdc3c7 0%, #2c3e50 100%); padding: 20px; border-radius: 10px; color: white; margin-bottom: 20px;">
    <h1 style="color: white; margin: 0;">🥈 Silver Layer (Cleaned & Conformed)</h1>
    <p style="margin: 0; font-size: 1.1em; opacity: 0.9;">Khu vực dữ liệu đã được làm sạch, chuẩn hóa kiểu dữ liệu và lọc bỏ các bản ghi lỗi Data Quality</p>
</div>
""", unsafe_allow_html=True)

tables = get_tables("silver")
if not tables:
    st.warning("Không tìm thấy bảng nào trong thư mục Silver.")
    tables = ["transactions_silver", "budget_plan_silver", "misa_invoices_silver", "general_ledger_silver", "fx_rates_silver", "ar_aging_silver", "ap_aging_silver"] # Fallback

selected_table = st.selectbox("Chọn bảng (Table):", tables)

recent_only = st.checkbox("⚡ Tải nhanh (Chỉ lấy dữ liệu vừa được hệ thống xử lý hôm nay)", value=True)
auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh (5s)", value=True, help="Tự động làm mới dữ liệu mỗi 5 giây")

def render_page():
    with st.spinner(f"Đang tải {selected_table}..."):
        df, err = query_delta("silver", selected_table, recent_only=recent_only)

    if err:
        if "Bảng hiện tại đang trống" in err:
            st.info("ℹ️ Bảng hiện tại đang trống (Chưa có dữ liệu nào được ghi vào).")
        else:
            st.error(f"Lỗi tải dữ liệu: {err}")
    else:
        st.success(f"Tải thành công {len(df)} bản ghi.")
        st.dataframe(df, use_container_width=True)
        
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("⬇ Download CSV", data=csv, file_name=f"{selected_table}.csv", mime="text/csv")

if hasattr(st, "fragment") and auto_refresh:
    @st.fragment(run_every=5)
    def live_update():
        render_page()
    live_update()
else:
    render_page()
    if auto_refresh and not hasattr(st, "fragment"):
        import time
        time.sleep(5)
        st.rerun()
