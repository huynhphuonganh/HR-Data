import json
import pandas as pd
import re
from pandas import json_normalize
from typing import Optional


def clean_company_name(name):
    """Chuẩn hóa tên công ty: lấy phần tử đầu tiên trong list, xóa khoảng trắng thừa."""
    # Nếu là list → lấy phần tử đầu tiên
    if isinstance(name, list):
        name = name[0] if len(name) > 0 else None

    # Nếu không phải chuỗi → bỏ qua
    if not isinstance(name, str):
        return None

    # Xóa khoảng trắng thừa
    name = re.sub(r"\s+", " ", name).strip()

    # Nếu rỗng → None
    return name if name else None


def process_company(df: pd.DataFrame) -> pd.DataFrame:
    if "name" in df.columns:
        df["name"] = df["name"].apply(clean_company_name)
    return df


