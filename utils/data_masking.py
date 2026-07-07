# ─────────────────────────────────────────────────────────
# utils/data_masking.py
# Hỗ trợ che giấu (masking) dữ liệu PII cho môi trường staging/dev
# Tham chiếu: Mục 11.4 - Đề tài Tốt nghiệp
# ─────────────────────────────────────────────────────────

from pyspark.sql.functions import col, regexp_replace, sha2, concat_ws, lit

def mask_pii(df, env: str = "development"):
    """
    Che giấu (mask) dữ liệu PII trong môi trường non-production.
    Trong môi trường production, dữ liệu được giữ nguyên (vì đã được
    mã hóa mức storage bằng encryption.py).
    """
    if env == "production":
        return df

    # Thực hiện masking nếu các cột tồn tại trong DataFrame
    columns = df.columns
    masked_df = df

    if "email" in columns:
        # Mask email: abc@def.com → a**@***.com
        # Regex: (?<=.{1})[^@]*(?=@) → thay thế tất cả ký tự giữa ký tự đầu tiên và @ bằng dấu *
        masked_df = masked_df.withColumn(
            "email", 
            regexp_replace(col("email"), r"(?<=.{1})[^@]*(?=@)", "**")
        )
        
    if "phone" in columns:
        # Mask phone: 0987654321 → *******321
        masked_df = masked_df.withColumn(
            "phone", 
            regexp_replace(col("phone"), r"\d(?=\d{3})", "*")
        )
        
    if "customer_id" in columns:
        # Hash customer_id để tham chiếu chéo không bị lộ nhưng vẫn join được
        masked_df = masked_df.withColumn(
            "customer_id", 
            sha2(concat_ws("-", col("customer_id"), lit("vinamik_salt")), 256)
        )

    if "tax_code" in columns:
        # Mask tax_code: 0300588569 → 0300******
        masked_df = masked_df.withColumn(
            "tax_code",
            regexp_replace(col("tax_code"), r"(?<=.{4}).", "*")
        )

    return masked_df
