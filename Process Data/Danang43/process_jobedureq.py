import json
import pandas as pd
import re
from pandas import json_normalize
from typing import Optional

# 1. Định nghĩa hàm xử lý field_of_study

keywords_after = [
    'ngành', 'chứng chỉ', 'về', 'mảng', 'nghề',
    'lĩnh vực', 'nền tảng', 'gì', 'nào', 'trở lên'
]

keywords_before = [
    'yêu cầu', 'tốt', 'trở lên', 'ưu tiên', 'là',
    'tương tự', 'ứng viên', 'có'
]

def extract_and_clean_field_of_study(text: str) -> Optional[str]:
    """Làm sạch và chuẩn hóa dữ liệu trong cột field_of_study."""
    if pd.isna(text):
        return None

    original_text = text.strip()
    text_lower = original_text.lower()
    cleaned = None

    # 🔹 Nhóm "lấy phần sau"
    for kw in keywords_after:
        pattern = rf'{kw}\s*(.*)'
        match = re.search(pattern, text_lower)
        if match:
            part = match.group(1).strip()
            if not part or not re.search(r'[A-Za-zÀ-ỹĐđ]', part):
                return None
            start = text_lower.find(part)
            cleaned = original_text[start:].strip()
            break

    # 🔹 Nhóm "lấy phần trước"
    if cleaned is None:
        for kw in keywords_before:
            pattern = rf'(.*)\s+{kw}\b'
            match = re.search(pattern, text_lower)
            if match:
                part = match.group(1).strip()
                if not part or not re.search(r'[A-Za-zÀ-ỹĐđ]', part):
                    return None
                end = len(part)
                cleaned = original_text[:end].strip()
                break

    # 🔹 Nếu vẫn chưa có keyword nào → giữ nguyên
    if cleaned is None:
        cleaned = original_text

    # 🔹 Chuẩn hóa khoảng trắng
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # 🔹 Nếu không còn chữ cái hoặc chứa số → None
    if not re.search(r'[A-Za-zÀ-ỹĐđ]', cleaned) or re.search(r'\d', cleaned):
        return None

    # 🔹 Viết hoa chữ cái đầu
    cleaned = cleaned.capitalize()

    return cleaned or None


# 2. Hàm xử lý chính toàn bộ bảng jobEducationReq

def process_job_education_req(df: pd.DataFrame) -> pd.DataFrame:
    """Tiền xử lý toàn bộ bảng job_education_requirements."""
    # Chuẩn hoá field_of_study
    df['field_of_study'] = df['field_of_study'].astype(str).apply(extract_and_clean_field_of_study)

    # Chuẩn hoá các giá trị null
    df['field_of_study'].replace(['', 'None', 'none', 'null', 'Null'], pd.NA)

    # Xoá record nếu education_level = 'other' và field_of_study bị null
    df = df[~((df['education_level'].str.lower() == 'other') & (df['field_of_study'].isna()))].copy()

    return df

