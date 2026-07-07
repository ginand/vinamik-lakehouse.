# ─────────────────────────────────────────────────────────
# utils/encryption.py
# Cung cấp hàm mã hóa (Column-Level Encryption) cho dữ liệu nhạy cảm
# Tham chiếu: Mục 11.2 - Đề tài Tốt nghiệp
# ─────────────────────────────────────────────────────────

from cryptography.fernet import Fernet
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import os

# Trong môi trường thực tế, key này được nạp từ Azure Key Vault qua biến môi trường.
# Ở đây ta dùng một dummy key mặc định (Base64 32-byte) nếu không tìm thấy biến môi trường
# để code vẫn chạy được trên local docker.
DEFAULT_KEY = b"TjBrcFhVMTB3eWVmZHdEcFg1WUV1Wk5Bdk9BVF9uVk8="
ENCRYPTION_KEY = os.environ.get("AKV_ENCRYPTION_KEY", DEFAULT_KEY.decode())

try:
    fernet = Fernet(ENCRYPTION_KEY.encode())
except Exception as e:
    # Fallback cho local test
    fernet = Fernet(DEFAULT_KEY)


def _encrypt_string(val: str) -> str:
    """Mã hóa chuỗi thành dạng được bảo vệ."""
    if not val:
        return None
    return fernet.encrypt(str(val).encode()).decode()


def _decrypt_string(val: str) -> str:
    """Giải mã chuỗi đã mã hóa."""
    if not val:
        return None
    try:
        return fernet.decrypt(val.encode()).decode()
    except Exception:
        return None


# Đăng ký thành UDF của PySpark để có thể dùng với withColumn()
encrypt_udf = udf(_encrypt_string, StringType())
decrypt_udf = udf(_decrypt_string, StringType())

# Ví dụ sử dụng trong pipeline:
# from utils.encryption import encrypt_udf
# df = df.withColumn("customer_id_encrypted", encrypt_udf(col("customer_id")))
