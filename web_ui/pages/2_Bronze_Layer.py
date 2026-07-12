import streamlit as st
from utils import query_delta, get_tables

st.set_page_config(page_title="Bronze Layer", page_icon="📂", layout="wide")
st.markdown("""
<div style="background: linear-gradient(90deg, #b08d57 0%, #4d3319 100%); padding: 20px; border-radius: 10px; color: white; margin-bottom: 20px;">
    <h1 style="color: white; margin: 0;">🥉 Bronze Layer (Raw Data)</h1>
    <p style="margin: 0; font-size: 1.1em; opacity: 0.9;">Khu vực tập kết dữ liệu thô nguyên bản (Ingestion từ ERP, Google Sheets, CSV)</p>
</div>
""", unsafe_allow_html=True)

tables = get_tables("bronze")
if not tables:
    st.warning("Không tìm thấy bảng nào trong thư mục Bronze hoặc đang gặp lỗi kết nối Azure.")
    tables = ["transactions_bronze", "budget_plan_bronze", "misa_invoices_bronze"] # Fallback

selected_table = st.selectbox("Chọn bảng (Table):", tables)

auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh (5s)", value=True, help="Tự động làm mới dữ liệu mỗi 5 giây")

def render_page():
    with st.spinner(f"Đang tải {selected_table}..."):
        df, err = query_delta("bronze", selected_table)

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
