import streamlit as st

st.set_page_config(page_title="Data Lineage", page_icon="🔗", layout="wide")
st.title("🔗 Data Lineage (Luồng dữ liệu)")

st.markdown("""
Bản đồ luồng chảy dữ liệu (Data Lineage) của toàn bộ hệ thống VinaMilk Data Lakehouse. 
Bản đồ này giúp truy xuất nguồn gốc của dữ liệu từ khi sinh ra ở hệ thống ERP cho đến khi lên Dashboard.
""")

st.markdown("### Kiến trúc luồng dữ liệu (End-to-End Architecture)")

# Sử dụng Mermaid JS thông qua markdown của Streamlit
mermaid_code = """
```mermaid
graph TD
    classDef source fill:#f9d0c4,stroke:#333,stroke-width:2px;
    classDef stream fill:#c4e0f9,stroke:#333,stroke-width:2px;
    classDef storage fill:#f9f5c4,stroke:#333,stroke-width:2px;
    classDef layer fill:#d4f9c4,stroke:#333,stroke-width:2px;
    classDef bi fill:#f3c4f9,stroke:#333,stroke-width:2px;
    classDef err fill:#ff9999,stroke:#333,stroke-width:2px;

    PG[(PostgreSQL<br>Mock SAP ERP)]:::source -->|WAL Logs| CDC[Debezium CDC]:::stream
    GS[Google Sheets<br>Finance]:::source -->|API| BP[Budget Producer]:::stream
    MISA[MISA SME<br>CSV]:::source -->|File Watcher| MP[Misa Producer]:::stream
    
    CDC -->|Kafka Topic| EH[(Azure Event Hubs)]:::stream
    BP -->|Kafka Topic| EH
    MP -->|Kafka Topic| EH
    
    EH -->|PySpark Streaming| BR[(Bronze Layer<br>Raw Delta)]:::layer
    BR -->|PySpark + Data Quality| SV[(Silver Layer<br>Clean Delta)]:::layer
    
    BR -->|Bản ghi lỗi| QUAR[(Quarantine<br>Dirty Data)]:::err
    
    SV -->|dbt-duckdb + dbt test| GD[(Gold Layer<br>Data Marts)]:::layer
    
    GD -->|DirectQuery / Import| PBI[Power BI<br>Dashboards]:::bi
    QUAR -->|Monitoring| DQ[DQ Dashboard]:::bi
```
"""

st.markdown(mermaid_code)

st.info("💡 Sơ đồ trên mô tả luồng kiến trúc vật lý tổng thể.")

st.markdown("---")
st.markdown("### 🕸️ Lineage động (Dynamic dbt Lineage)")
st.markdown("Sơ đồ dưới đây được **Code Python tự động vẽ ra (Dynamic)** bằng cách đọc trực tiếp file `manifest.json` do hệ thống dbt sinh ra. Điều này chứng minh luồng dữ liệu là hoàn toàn thực tế chứ không phải ảnh demo.")

import json
import os

manifest_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "dbt_gold", "target", "manifest.json")

if os.path.exists(manifest_path):
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})
    
    import re
    # Patch dependencies because dbt project uses a custom silver_source macro instead of native {{ source() }}
    for n_id, n_data in nodes.items():
        if n_data.get("resource_type") == "model":
            model_name = n_data.get("name")
            model_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "dbt_gold", "models", f"{model_name}.sql")
            if os.path.exists(model_path):
                with open(model_path, "r", encoding="utf-8") as fsql:
                    sql_content = fsql.read()
                matches = re.findall(r"silver_source\(['\"](.*?)['\"]\)", sql_content)
                for m in matches:
                    source_id = f"source.vinamik_gold.silver.{m}"
                    if "nodes" not in n_data.get("depends_on", {}):
                        n_data["depends_on"] = {"nodes": []}
                    if source_id not in n_data["depends_on"]["nodes"]:
                        n_data["depends_on"]["nodes"].append(source_id)
                        
                quarantine_matches = re.findall(r"\('([a-z_]+_silver)',", sql_content)
                for m in quarantine_matches:
                    source_id = f"source.vinamik_gold.quarantine.{m}"
                    if "nodes" not in n_data.get("depends_on", {}):
                        n_data["depends_on"] = {"nodes": []}
                    if source_id not in n_data["depends_on"]["nodes"]:
                        n_data["depends_on"]["nodes"].append(source_id)
    
    # Danh sách các Gold models
    model_names = sorted([data.get("name") for data in nodes.values() if data.get("resource_type") == "model"])
    model_names.insert(0, "Tất cả các bảng (Hiển thị toàn bộ)")
    
    st.info("🎯 **Tính năng Đánh giá tác động (Root Cause Analysis):** Hãy chọn một bảng Gold cụ thể để xem hệ thống truy ngược (trace-back) các bảng Silver cấu thành nên nó.")
    selected_target = st.selectbox("Lọc luồng dữ liệu theo bảng:", model_names)
    
    edges = []
    upstream_set = set()
    
    def get_upstream(node_name):
        upstream_set.add(node_name)
        for n_id, n_data in nodes.items():
            if n_data.get("name") == node_name:
                for p_id in n_data.get("depends_on", {}).get("nodes", []):
                    p_name = p_id.split(".")[-1]
                    edges.append((p_name, node_name, p_id))
                    if p_name not in upstream_set:
                        get_upstream(p_name)
                        
    if selected_target != "Tất cả các bảng (Hiển thị toàn bộ)":
        get_upstream(selected_target)
    else:
        for n_id, n_data in nodes.items():
            if n_data.get("resource_type") == "model":
                child = n_data.get("name")
                upstream_set.add(child)
                for p_id in n_data.get("depends_on", {}).get("nodes", []):
                    parent = p_id.split(".")[-1]
                    upstream_set.add(parent)
                    edges.append((parent, child, p_id))
                    
    mermaid_lines = ["```mermaid", "graph LR"]
    mermaid_lines.append("    classDef source fill:#c4e0f9,stroke:#00509E,stroke-width:2px,color:#000,rx:5px,ry:5px;")
    mermaid_lines.append("    classDef quarantine_source fill:#ffcccb,stroke:#b22222,stroke-width:2px,color:#000,rx:5px,ry:5px;")
    mermaid_lines.append("    classDef model fill:#f3c4f9,stroke:#6A0DAD,stroke-width:2px,color:#000,rx:15px,ry:15px;")
    mermaid_lines.append("    classDef target fill:#ffdfba,stroke:#FF8C00,stroke-width:4px,color:#000,rx:15px,ry:15px;")
    
    unique_edges = set(edges)
    
    for node in upstream_set:
        if node == selected_target:
            mermaid_lines.append(f"    {node}([{node}]):::target")
        else:
            is_source = any(p_id.startswith("source.") for p, c, p_id in unique_edges if p == node)
            if is_source:
                is_quarantine = any("quarantine" in p_id for p, c, p_id in unique_edges if p == node)
                if is_quarantine:
                    mermaid_lines.append(f"    {node}[({node}_quarantine)]:::quarantine_source")
                else:
                    mermaid_lines.append(f"    {node}[({node})]:::source")
            else:
                mermaid_lines.append(f"    {node}([{node}]):::model")
                
    for p, c, p_id in unique_edges:
        if "quarantine" in p_id:
            mermaid_lines.append(f"    {p} -.->|monitor errors| {c}")
        else:
            mermaid_lines.append(f"    {p} ==>|transform| {c}")
                
    mermaid_lines.append("```")
    
    st.markdown("\n".join(mermaid_lines))
else:
    st.warning("Chưa tìm thấy file manifest.json. Vui lòng chạy `dbt build` trong thư mục dbt_gold để hệ thống tự động sinh Lineage.")
