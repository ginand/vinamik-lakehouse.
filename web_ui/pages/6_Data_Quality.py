import streamlit as st
from utils import query_delta

st.set_page_config(page_title="Data Quality", page_icon="✔", layout="wide")
st.markdown("""
<div style="background: linear-gradient(90deg, #1D976C 0%, #93F9B9 100%); padding: 20px; border-radius: 10px; color: #1a1a1a; margin-bottom: 20px;">
    <h1 style="color: #1a1a1a; margin: 0;">🛡️ Data Quality Rules (Business Logic)</h1>
    <p style="margin: 0; font-size: 1.1em; opacity: 0.9;">Từ điển các quy tắc nghiệp vụ (Great Expectations / PySpark) dùng để kiểm duyệt và cách ly dữ liệu xấu</p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
Dashboard giám sát tình trạng Data Quality của toàn bộ hệ thống dựa trên bảng `dq_monitoring_gold`.
""")

auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh (5s)", value=True, help="Tự động làm mới trang mỗi 5 giây để theo dõi lượng lỗi Data Quality sinh ra Real-time")

def render_dq():
    with st.spinner("Đang tải số liệu Data Quality..."):
        df, err = query_delta("gold", "dq_monitoring_gold")

    if err:
        if "Bảng hiện tại đang trống" in err:
            st.success("✅ Tuyệt vời! Hiện tại không có bất kỳ lỗi Data Quality nào được phát hiện.")
        else:
            st.error(f"Lỗi tải dữ liệu DQ: {err}")
    else:
        if len(df) == 0:
            st.success("✅ Tuyệt vời! Không có bản ghi lỗi nào trong ngày hôm nay.")
        else:
            # Tính toán DQ Summary
            total_errors = df['error_count'].sum()
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Tổng số lỗi phát hiện", f"{total_errors:,}", "- Cách ly tại Quarantine", delta_color="inverse")
            col2.metric("Số quy tắc bị vi phạm", f"{df['error_type'].nunique()}")
            col3.metric("Bảng bị ảnh hưởng", f"{df['source_table'].nunique()}")
            
            st.markdown("### 🔍 Phân tích chi tiết")
            st.dataframe(df, use_container_width=True)
            
            st.markdown("### Quy tắc kiểm định (DQ Rules Viewer)")
            rule_desc = {
                "dq_amount_zero": "Giá trị giao dịch = 0",
                "dq_invalid_product": "Mã sản phẩm không tồn tại",
                "dq_wrong_currency": "Loại tiền tệ không hợp lệ",
                "dq_missing_customer": "Thiếu mã khách hàng"
            }
            
            for rule in df['error_type'].unique():
                with st.expander(f"Quy tắc: {rule}"):
                    st.write(f"**Mô tả:** {rule_desc.get(rule, 'Lỗi không xác định')}")
                    # Filter errors for this rule
                    rule_df = df[df['error_type'] == rule]
                    st.write(f"**Số bản ghi vi phạm:** {rule_df['error_count'].sum()}")
                    st.write(f"**Nguồn:** {', '.join(rule_df['source_table'].unique())}")

if hasattr(st, "fragment") and auto_refresh:
    @st.fragment(run_every=5)
    def live_update():
        render_dq()
    live_update()
else:
    render_dq()
    if auto_refresh and not hasattr(st, "fragment"):
        import time
        time.sleep(5)
        st.rerun()
