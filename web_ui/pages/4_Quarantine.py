import streamlit as st
from utils import query_delta, get_tables

st.set_page_config(page_title="Quarantine Zone", page_icon="🏥", layout="wide")
st.markdown("""
<div style="background: linear-gradient(90deg, #cb2d3e 0%, #ef473a 100%); padding: 20px; border-radius: 10px; color: white; margin-bottom: 20px;">
    <h1 style="color: white; margin: 0;">🏥 Quarantine Zone (Dead Letter Queue)</h1>
    <p style="margin: 0; font-size: 1.1em; opacity: 0.9;">Khu vực giam giữ các bản ghi bị dính lỗi Data Quality (Không thỏa mãn rule của Great Expectations)</p>
</div>
""", unsafe_allow_html=True)

tables = get_tables("quarantine")
if not tables:
    st.warning("Không tìm thấy bảng nào trong thư mục Quarantine.")
    tables = ["transactions_quarantine", "budget_plan_quarantine"] # Fallback

selected_table = st.selectbox("Chọn bảng (Table):", tables)

auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh (5s)", value=True, help="Tự động làm mới dữ liệu mỗi 5 giây")
mock_data = st.sidebar.checkbox("🧪 Bật Mô Phỏng Lỗi (Dành cho chụp ảnh đồ án)", value=False, help="Tự động sinh ra một vài bản ghi lỗi giả lập để chụp ảnh minh họa")

def render_page():
    with st.spinner(f"Đang tải {selected_table}..."):
        df, err = query_delta("quarantine", selected_table)

    if mock_data:
        import pandas as pd
        import datetime
        err = None
        df = pd.DataFrame({
            "txn_id": ["TXN-9999", "TXN-8888", "TXN-7777", "TXN-6666"],
            "company_code": ["VN01", None, "VN02", "VN01"],
            "status": ["INVALID", "POSTED", "DRAFT", "UNKNOWN"],
            "total_credit": [-500000, 1000000, 0, 200000],
            "total_debit": [0, -50000, 0, 0],
            "dq_reason": ["amount_must_be_positive", "company_code_is_null", "amount_zero_both_sides", "invalid_status"],
            "_quarantined_at": [datetime.datetime.now()] * 4
        })

    if err:
        if "Bảng hiện tại đang trống" in err:
            st.success("✅ Tuyệt vời! Bảng hiện đang trống (Không có bất kỳ bản ghi nào bị dính lỗi Data Quality).")
        else:
            st.error(f"Lỗi tải dữ liệu: {err}")
    else:
        if len(df) == 0:
            st.success("Tuyệt vời! Không có bản ghi nào bị lỗi.")
        else:
            st.warning(f"Cảnh báo: Có {len(df)} bản ghi vi phạm luật Data Quality.")
            st.dataframe(df, use_container_width=True)
            
            if 'dq_reason' in df.columns:
                st.markdown("### Thống kê lý do lỗi (Reason)")
                reason_counts = df['dq_reason'].value_counts().reset_index()
                reason_counts.columns = ['Lý do lỗi', 'Số lượng']
                st.table(reason_counts)
            
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button("⬇ Download CSV để kiểm tra", data=csv, file_name=f"{selected_table}_errors.csv", mime="text/csv")

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
