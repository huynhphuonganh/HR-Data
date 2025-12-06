import json
import pandas as pd
from datetime import datetime
INPUT_FILE = "../Data/raw/company.json"
CHECKPOINT_FILE = "../crawl_check.json"
OUTPUT_FILE = "../Data/process/company_clean.json"
def load_json_safely(file):
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, list):
        return pd.DataFrame(data)
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list) and all(isinstance(i, dict) for i in value):
                return pd.DataFrame(value)
    raise TypeError(" JSON phải là list hoặc dict chứa list dict")

def split_address(addr):
    if isinstance(addr, str):
        parts = [p.strip() for p in addr.split(',') if p.strip()]
    else:
        parts = []
    line1 = parts[0] if len(parts) > 0 else None
    line2 = parts[1] if len(parts) > 1 else None
    line3 = "Đà Nẵng"   
    line4 = "Việt Nam"
    return line1, line2, line3, line4
def get_company_links(checkpoint_file):
    try:
        with open(checkpoint_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(" Không tìm thấy file checkpoint.")
        return {}
    results = data.get("results", [])
    company_links = {}
    for item in results:
        job_link = item.get("metadata", {}).get("source")
        company_id = item.get("job", {}).get("company_id")
        if company_id and job_link:
            company_links.setdefault(company_id, []).append(job_link)
    print(f"Đã tạo mapping link_jobs cho {len(company_links)} công ty.")
    return company_links

def preprocess_company():
    df = load_json_safely(INPUT_FILE)
    company_links = get_company_links(CHECKPOINT_FILE)
    df["link_jobs"] = df["company_id"].apply(lambda cid: company_links.get(cid, []))
    df["companyID"] = range(1, len(df) + 1)
    df[["line1", "line2", "line3", "line4"]] = df["address"].apply(lambda x: pd.Series(split_address(x)))
    drop_cols = [
        "company_id",
        "hq_city_by",
        "hq_ward_by",
        "hq_country_by",
        "created_at",
        "updated_at",
        "full_address"]
    df = df.drop(columns=drop_cols, errors="ignore")
    df.to_json(OUTPUT_FILE, orient="records", indent=4, force_ascii=False)
    print("Tiền xử lý xong! File lưu tại:", OUTPUT_FILE)

if __name__ == "__main__":
    preprocess_company()
