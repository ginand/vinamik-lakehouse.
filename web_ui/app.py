import streamlit as st

st.set_page_config(
    page_title="VinaMilk Lakehouse",
    page_icon="🥛",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS cho giao diện "Sang - Xịn - Mịn"
st.markdown("""
<style>
    .hero-box {
        background: linear-gradient(135deg, #0e5a97 0%, #1e81b0 100%);
        padding: 40px;
        border-radius: 15px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        margin-bottom: 30px;
    }
    .hero-title {
        font-size: 3rem;
        font-weight: 800;
        margin-bottom: 10px;
    }
    .hero-subtitle {
        font-size: 1.2rem;
        opacity: 0.9;
    }
    .card {
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid #1e81b0;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        height: 100%;
        color: black !important;
    }
    .card h4 {
        color: #0e5a97;
        margin-top: 0;
    }
</style>
""", unsafe_allow_html=True)

# Hero Section
st.markdown("""
<div class="hero-box">
    <div class="hero-title">🥛 VinaMilk Data Lakehouse</div>
    <div class="hero-subtitle">Centralized Data Platform Monitoring & Explorer Portal</div>
</div>
""", unsafe_allow_html=True)

st.markdown("### 👋 Chào mừng bạn đến với Cổng Quản trị Dữ liệu Trung tâm")
st.write("Hệ thống này cung cấp một bộ công cụ mạnh mẽ để **giám sát, kiểm định và khám phá** toàn bộ vòng đời của dữ liệu doanh nghiệp thông qua chuẩn **Medallion Architecture**.")

st.markdown("<br>", unsafe_allow_html=True)

# Grid Layout cho các tính năng
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    <div class="card">
        <h4>📊 Khám phá Dữ liệu (Data Layers)</h4>
        <p>Truy cập trực tiếp vào Data Lake thông qua giao diện Web:</p>
        <ul>
            <li><b>📂 Bronze:</b> Dữ liệu thô từ ERP.</li>
            <li><b>🥈 Silver:</b> Dữ liệu đã làm sạch.</li>
            <li><b>🥇 Gold:</b> Các Data Marts phục vụ BI.</li>
            <li><b>📁 Storage:</b> Quản lý file Azure.</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="card">
        <h4>🛡️ Quản trị Chất lượng (Data Quality)</h4>
        <p>Bảo vệ tính toàn vẹn của dữ liệu báo cáo:</p>
        <ul>
            <li><b>✔ Thống kê DQ:</b> Tỷ lệ dữ liệu sạch/lỗi.</li>
            <li><b>🟥 Quarantine:</b> Kho lưu trữ và cách ly dữ liệu lỗi để xử lý.</li>
            <li><b>🧪 Test Results:</b> Kết quả kiểm định tự động (dbt, GX).</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div class="card">
        <h4>⚙️ Vận hành Hệ thống (Operations)</h4>
        <p>Theo dõi luồng chảy và tiến trình hệ thống:</p>
        <ul>
            <li><b>🏠 Dashboard:</b> KPI tổng quan thời gian thực.</li>
            <li><b>🔗 Data Lineage:</b> Sơ đồ tự động truy xuất nguồn gốc dữ liệu.</li>
            <li><b>⏱ Pipeline Status:</b> Giám sát Apache Airflow.</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br><br>", unsafe_allow_html=True)
st.info("👈 **Hướng dẫn:** Vui lòng sử dụng **Menu bên trái** để điều hướng đến các chức năng quản trị chi tiết.")

st.markdown("---")
st.caption("© 2026 VinaMilk Data Engineering Team. Developed for Enterprise Architecture Thesis.")

