import json
import re
import pandas as pd
from pandas import json_normalize
from datetime import datetime
from typing import Optional

# 1. Chuẩn hóa tên (full_name)
def normalize_text(text: str) -> str:
    """Chuẩn hóa Unicode và khoảng trắng."""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text


def clean_full_name(name: Optional[str]) -> Optional[str]:
    """Làm sạch và chuẩn hóa tên người tuyển dụng."""
    if not isinstance(name, str) or not name.strip():
        return None

    name = normalize_text(name)
    if name.lower() in ["phòng nhân sự", "phòng tuyển dụng"]:
        return name
    lowered = name.lower()
    parts = name.split()

    # 1️⃣ Keyword đặc biệt
    keywords = ["đến", "tiết", "liên hệ", "cover letter", "qua", "tiếp", 'nhánh', 'zalo', 'hẹn', 'facebook']
    for kw in keywords:
        if lowered.startswith(kw):
            return "Phòng Nhân sự"

    # 2️⃣ Hai chữ đầu viết hoa, chữ 3 viết thường → có thể là tên riêng
    if len(parts) >= 3:
        if parts[0][0].isupper() and parts[1][0].isupper() and parts[2][0].islower():
            return parts[0]

    # 3️⃣ Quy tắc hai chữ đầu
    if len(parts) >= 2:
        first, second = parts[0], parts[1]
        if first[0].isupper() and second[0].islower():
            return first
        if first[0].islower() and second[0].isupper():
            return second

    # 4️⃣ Prefix không phải tên (CV, HR, Anh, Chị, Mr, Ms, etc.)
    prefixes = ["cv", "hr", "anh", "chị", "mr", "ms", "phòng"]
    if parts and parts[0].lower() in prefixes and len(parts) > 1:
        return " ".join(parts[1:])
    
    name = normalize_text(name)
    
    # 5️⃣ Giữ nguyên
    return name



# 2. Hàm trích số điện thoại hợp lệ
def extract_first_phone_correct(phone_str: Optional[str]) -> Optional[str]:
    """
    Trả về số điện thoại đầu tiên hợp lệ:
    - bắt đầu bằng '0'
    - dài 10 hoặc 11 chữ số, theo quy tắc đã mô tả
    """
    if not isinstance(phone_str, str):
        return None

    digits = re.sub(r"\D", "", phone_str)
    n = len(digits)
    if n < 10:
        return None

    for i in range(n):
        if digits[i] != "0":
            continue
        if n - i < 10:
            continue

        num10 = digits[i:i + 10]
        next_char = digits[i + 10] if (n - i) > 10 else ""
        next_next = digits[i + 11] if (n - i) > 11 else ""

        if next_char == "":
            return num10
        elif next_char == "0":
            if next_next == "0":
                return digits[i:i + 11] if (n - i) >= 11 else num10
            else:
                return num10
        else:
            return digits[i:i + 11] if (n - i) >= 11 else num10

    return None


# 3. Hàm xử lý toàn bộ DataFrame
def process_recruiter(df: pd.DataFrame) -> pd.DataFrame:
    """Tiền xử lý bảng recruiter."""
    # Làm sạch tên
    df["full_name"] = df["full_name"].apply(clean_full_name)

    # Chuẩn hóa số điện thoại
    df["phone"] = df["phone"].apply(extract_first_phone_correct)

    return df
