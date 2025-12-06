import json
import re
import pandas as pd
from pandas import json_normalize
from datetime import datetime
from typing import Optional
 
# 1. Hàm xử lý ngày tháng
def process_expires_at(date_str: str) -> Optional[str]:
    """Chuyển đổi ngày hạn nộp từ dd/mm/yyyy sang yyyy-mm-dd."""
    if not date_str or not isinstance(date_str, str):
        return None

    date_str = date_str.strip()

    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None
    
# 2. Làm sạch text trong description và benefits
def clean_text(text: Optional[str]) -> Optional[str]:
    if pd.isna(text):
        return None

    text = str(text).strip()
    if not text:
        return None

    # Xóa chỉ mục dạng "1.", "2)", "3-" ở đầu hoặc giữa câu
    text = re.sub(r'(^|\s)\d+[\.\)\-]\s*', r'\1', text)

    # Thay '-' bằng '.' nếu sau nó là chữ hoa (và không nằm giữa số)
    text = re.sub(r'(?<!\d)\s*[\-\•\–\—\·]\s*(?=[A-ZÀ-ỸĐ])', '. ', text)

    # Xóa '-' thừa ở đầu
    text = re.sub(r'^\s*[\-\•\–\—\·]+\s*', '', text)

    # Chuẩn hóa dấu '.'
    text = re.sub(r'\.{2,}', '.', text)
    text = re.sub(r'\.\s*', '. ', text)

    # Chuẩn hóa khoảng trắng
    text = re.sub(r'\s{2,}', ' ', text).strip()

    # Loại bỏ dấu '.' trùng cuối
    text = re.sub(r'\.{2,}$', '.', text).strip('. ')

    if not re.search(r'[A-Za-zÀ-ỹĐđ]', text):
        return None

    return text


# 3. Hàm xử lý toàn bộ DataFrame
def process_job(df: pd.DataFrame) -> pd.DataFrame:
    """Tiền xử lý bảng job."""
    # Chuẩn hóa đơn vị tiền tệ
    df['currency'] = df['currency'].str.replace('VND', 'vnđ', case=False)

    # Chuyển đổi ngày hạn nộp sang yyyy-mm-dd
    df['expires_at'] = df['expires_at'].apply(process_expires_at)

    # Làm sạch description và benefits
    df['description'] = df['description'].apply(clean_text)
    df['benefits'] = df['benefits'].apply(clean_text)

    return df

