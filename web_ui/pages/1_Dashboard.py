import os
import json
import streamlit as st
import pandas as pd
from utils import query_delta
import datetime

st.set_page_config(page_title="Dashboard", page_icon="🏠", layout="wide")
st.markdown("""
<div style="background: linear-gradient(90deg, #4b6cb7 0%, #182848 100%); padding: 20px; border-radius: 10px; color: white; margin-bottom: 20px;">
    <h1 style="color: white; margin: 0;">🏠 Enterprise Data Lakehouse Dashboard</h1>
    <p style="margin: 0; font-size: 1.1em; opacity: 0.9;">Bảng điều khiển trung tâm tổng quan dữ liệu toàn hệ thống</p>
</div>
""", unsafe_allow_html=True)
# Fetch data safely
@st.cache_data(ttl=5)
def get_metrics():
    metrics = {
        "bronze_records": 0,
        "silver_records": 0,
        "quarantine_records": 0,
        "gold_models": 7,
        "dq_pass_rate": 100.0
    }
    
    # Bronze (Transactions sample)
    df_bronze, _ = query_delta("bronze", "transactions_bronze")
    if df_bronze is not None:
        metrics["bronze_records"] += len(df_bronze)
    
    # Silver (Transactions sample)
    df_silver, _ = query_delta("silver", "transactions_silver")
    if df_silver is not None:
        metrics["silver_records"] += len(df_silver)
        
    df_silver_budget, _ = query_delta("silver", "budget_plan_silver")
    if df_silver_budget is not None:
        metrics["silver_records"] += len(df_silver_budget)
        metrics["bronze_records"] += len(df_silver_budget) # Approx
        
    # Quarantine
    df_quar, _ = query_delta("quarantine", "transactions_quarantine")
    if df_quar is not None:
        metrics["quarantine_records"] += len(df_quar)
        
    # Calculate DQ Rate
    total_silver_quar = metrics["silver_records"] + metrics["quarantine_records"]
    if total_silver_quar > 0:
        metrics["dq_pass_rate"] = (metrics["silver_records"] / total_silver_quar) * 100
        
    # Dynamically count Gold models from dbt manifest
    dbt_target = "/dbt_gold/target/manifest.json"
    if not os.path.exists(dbt_target):
        dbt_target = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "dbt_gold", "target", "manifest.json")
        
    if os.path.exists(dbt_target):
        with open(dbt_target, "r", encoding="utf-8") as f:
            manifest = json.load(f)
            metrics["gold_models"] = sum(1 for n in manifest.get("nodes", {}).values() if n.get("resource_type") == "model")
    else:
        metrics["gold_models"] = 0

    return metrics, dbt_target

# Auto-refresh toggle cho trang Dashboard
auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh (5s)", value=True, help="Tự động làm mới trang mỗi 5 giây để theo dõi lượng dữ liệu tăng lên Real-time")

def render_dashboard():
    with st.spinner("Đang kéo dữ liệu trực tiếp từ Azure Data Lake..."):
        metrics, dbt_target = get_metrics()

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("📦 Bronze (Raw)", f"{metrics['bronze_records']:,} Records")
    col2.metric("🥈 Silver (Clean)", f"{metrics['silver_records']:,} Records")
    col3.metric("🟥 Quarantine (Dirty)", f"{metrics['quarantine_records']:,} Records", "- Lỗi Data Quality", delta_color="inverse")
    col4.metric("🥇 Gold (Models)", f"{metrics['gold_models']} Models", "Ready for BI")

    st.markdown("---")

    col5, col6, col7 = st.columns(3)
    col5.metric("✔ DQ Pass Rate", f"{metrics['dq_pass_rate']:.2f}%")

    # Determine status dynamically based on Silver records
    col6.metric("⚙ Pipeline Status", "🟢 SUCCESS" if metrics['silver_records'] > 0 else "🔴 STOPPED")

    last_run_time = "N/A"
    if os.path.exists(dbt_target):
        last_mtime = os.path.getmtime(dbt_target)
        last_run_time = datetime.datetime.fromtimestamp(last_mtime).strftime("%H:%M:%S")
    col7.metric("⏱ Last Run", last_run_time)

    st.markdown("### 📊 Phân bổ chất lượng dữ liệu (Data Quality Distribution)")
    import plotly.express as px
    
    col_c1, col_c2 = st.columns([1, 1])

    with col_c1:
        fig_pie = px.pie(
            names=["Silver (Sạch)", "Quarantine (Lỗi)"],
            values=[metrics['silver_records'], metrics['quarantine_records']],
            color_discrete_sequence=["#00CC96", "#EF553B"],
            hole=0.4
        )
        fig_pie.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_c2:
        chart_data = pd.DataFrame({
            "Layer": ["Bronze", "Silver", "Gold Models", "Quarantine"],
            "Count": [metrics['bronze_records'], metrics['silver_records'], metrics['gold_models'], metrics['quarantine_records']]
        })
        fig_bar = px.bar(chart_data, x="Layer", y="Count", color="Layer", text_auto=True)
        fig_bar.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300, showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_bar, use_container_width=True)

# Kích hoạt tính năng chạy ngầm (Fragment)
if hasattr(st, "fragment") and auto_refresh:
    @st.fragment(run_every=5)
    def live_update():
        render_dashboard()
    live_update()
else:
    render_dashboard()
    if auto_refresh and not hasattr(st, "fragment"):
        import time
        time.sleep(5)
        st.rerun()

st.info("💡 Mẹo: Truy cập Menu bên trái để khám phá sâu hơn từng phân vùng (Bronze, Silver, Gold).")
