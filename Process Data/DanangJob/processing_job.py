import json
import pandas as pd
from datetime import datetime
import re
import math

INPUT_FILE = "../Data/raw/job.json"
COMPANY_RAW_FILE = "../Data/raw/company.json"
COMPANY_CLEAN_FILE = "../Data/process/company_clean.json"
RECRUITER_RAW_FILE = "../Data/raw/recruiter.json"
RECRUITER_CLEAN_FILE = "../Data/process/recruiter_clean.json"
OUTPUT_FILE = "../Data/process/jobs_clean.json"
# ---- MAPPING ----
EXPERIENCE_MAP = {
    "0": 0, "1": 1, "1-2": 1, "2": 1, "3-5": 3, 
    "5": 5, "5-10": 5, "10+": 10}

def map_year_experience(value):
    if not value or "không" in str(value).lower():
        return 0
    value = re.sub(r"[^\d\-+]", "", str(value))
    if value in EXPERIENCE_MAP:
        return EXPERIENCE_MAP[value]
    try:
        num = int(re.findall(r"\d+", value)[0])
        return 0 if num == 0 else 1 if num <= 2 else 3 if num <= 5 else 5 if num <= 10 else 10
    except:
        return 0

def split_address(addr):
    if not addr:
        return None, None, "Đà Nẵng", "Việt Nam"
    parts = [p.strip() for p in re.split(r",|-", addr) if p.strip()]
    return (parts[0] if len(parts) > 0 else None, 
            parts[1] if len(parts) > 1 else None, 
            "Đà Nẵng", "Việt Nam")

def clean_text(text):
    if not text:
        return None
    text = re.sub(r"[👉✅•\*\-—–_•📌📩📞📍]+", " ", text)
    text = re.sub(r"[-]{2,}|\s{2,}", " ", text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    return "\n".join(lines) if lines else None

def detect_working_days(desc, benefits):
    text = f"{desc or ''} {benefits or ''}".lower()
    if any(x in text for x in ["thứ 2", "thứ hai", "t2", "t3", "t4", "t5", "t6"]) and "thứ 7" not in text:
        return "T2-T6"
    elif any(x in text for x in ["thứ 7", "t7", "thứ bảy", "chủ nhật", "cn"]):
        return "T2-T7"
    return "all_week"

def clean_salary(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if "thỏa thuận" in str(value).lower():
        return None
    numbers = re.sub(r"[^\d.]", "", str(value))
    try:
        return float(numbers) if numbers else None
    except:
        return None

def convert_date(value):
    try:
        return datetime.strptime(value, "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        return None

def preprocess_jobs():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
        df = pd.DataFrame(raw_data.get("data", []))

    if "job_id" in df.columns:
        df.insert(0, "jobID", range(1, len(df) + 1))
        df = df.drop(columns=["job_id"], errors="ignore")
    else:
        df["jobID"] = range(1, len(df) + 1)

    with open(COMPANY_RAW_FILE, "r", encoding="utf-8") as f:
        company_raw = json.load(f)
        company_raw = company_raw["data"] if isinstance(company_raw, dict) and "data" in company_raw else company_raw
    with open(COMPANY_CLEAN_FILE, "r", encoding="utf-8") as f:
        company_clean = json.load(f)
        company_clean = company_clean["data"] if isinstance(company_clean, dict) and "data" in company_clean else company_clean
    company_map = dict(zip(pd.DataFrame(company_raw)["company_id"], pd.DataFrame(company_clean)["companyID"]))

    with open(RECRUITER_RAW_FILE, "r", encoding="utf-8") as f:
        recruiter_raw = json.load(f)
        recruiter_raw = recruiter_raw["data"] if isinstance(recruiter_raw, dict) and "data" in recruiter_raw else recruiter_raw
    with open(RECRUITER_CLEAN_FILE, "r", encoding="utf-8") as f:
        recruiter_clean = json.load(f)
        recruiter_clean = recruiter_clean["recruiters"] if isinstance(recruiter_clean, dict) and "recruiters" in recruiter_clean else recruiter_clean
    recruiter_map = dict(zip(pd.DataFrame(recruiter_raw)["recruiter_id"], pd.DataFrame(recruiter_clean)["recruiterID"]))
    # ---- Map IDs ---
    df["companyID"] = df["company_id"].map(company_map)
    df["recruiterID"] = df["recruiter_id"].map(recruiter_map)
    # ---- Check mapping correctness ----
    missing_company = df[df["companyID"].isna()]["company_id"].unique()
    missing_recruiter = df[df["recruiterID"].isna()]["recruiter_id"].unique()
    if len(missing_company) > 0:
        print("⚠️ Cảnh báo: Các company_id sau không tìm thấy trong mapping:", list(missing_company))
    if len(missing_recruiter) > 0:
        print("⚠️ Cảnh báo: Các recruiter_id sau không tìm thấy trong mapping:", list(missing_recruiter))

    if len(missing_company) == 0 and len(missing_recruiter) == 0:
        print("Mapping giữa job → company & recruiter đều hợp lệ!")
    # ---- Làm sạch và xử lý cột ----
    df[["line1", "line2", "line3", "line4"]] = df["address"].apply(lambda x: pd.Series(split_address(x)))
    df["salary_min"] = df["salary_min"].apply(clean_salary)
    df["salary_max"] = df["salary_max"].apply(clean_salary)
    df["year_experience"] = df["year_experience"].apply(map_year_experience)
    df["expires_at"] = df["expires_at"].apply(convert_date)
    for col in ["description", "responsibilities", "benefits"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
    df["working_days"] = df.apply(lambda row: detect_working_days(row.get("description"), row.get("benefits")), axis=1)
    
    for col in ["companyID", "recruiterID"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["created_at"] = None
    df["updated_at"] = None
    df = df.drop(columns=["company_id", "recruiter_id", "address"], errors="ignore")
    output_data = {"jobs": df.to_dict(orient="records")}
    for job in output_data["jobs"]:
        for key, value in list(job.items()):
            if value is pd.NA or value is pd.NaT or (isinstance(value, float) and math.isnan(value)):
                job[key] = None
    # ---- SAVE ----
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    print(f"Đã lưu {len(output_data['jobs'])} jobs tại: {OUTPUT_FILE}")

if __name__ == "__main__":
    preprocess_jobs()
