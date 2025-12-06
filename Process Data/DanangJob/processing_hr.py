import json
import pandas as pd

INPUT_FILE = "../Data/raw/recruiter.json"
COMPANY_RAW_FILE = "../Data/raw/company.json"          
COMPANY_CLEAN_FILE = "../Data/process/company_clean.json"  
OUTPUT_FILE = "../Data/process/recruiter_clean.json"

def preprocess_recruiter():
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    df = pd.DataFrame(data)

    # --- 1. Xóa khoảng trắng ở cột phone ---
    if "phone" in df.columns:
        df["phone"] = df["phone"].astype(str).str.replace(r'\s+', '', regex=True)
    # --- 2. Xóa job_description nếu tồn tại ---
    if "job_description" in df.columns:
        df = df.drop(columns=["job_description"])
    # --- 3. Chuyển role về 'Recruiter' (viết hoa đầu) ---
    df["role"] = "Recruiter"
    # --- 4. Đổi tên cột name -> full_name để phù hợp database ---
    df = df.rename(columns={"name": "full_name"})
    # --- 5. Thêm cột photo_url = None ---
    df["photo_url"] = None
    # --- 6. Tạo recruiterID tự tăng ---
    df["recruiterID"] = range(1, len(df) + 1)
    # --- 7. Giữ quan hệ với bảng company ---
    with open(COMPANY_RAW_FILE, 'r', encoding='utf-8') as f1:
        company_raw = json.load(f1)
        if isinstance(company_raw, dict) and "data" in company_raw:
            company_raw = company_raw["data"]
    with open(COMPANY_CLEAN_FILE, 'r', encoding='utf-8') as f2:
        company_clean = json.load(f2)
        if isinstance(company_clean, dict) and "data" in company_clean:
            company_clean = company_clean["data"]
    df_raw_company = pd.DataFrame(company_raw)
    df_clean_company = pd.DataFrame(company_clean)
    company_map = dict(zip(df_raw_company["company_id"], df_clean_company["companyID"]))
    df["companyID"] = df["company_id"].map(company_map)
    df["is_active"] = True
    df["created_by"] = None
    df["created_at"] = "2025-10-08T13:01:44.418964"
    df["updated_at"] = "2025-10-08T13:01:44.418964"
    # --- 9. Xóa cột recruiter_id và company_id gốc ---
    df = df.drop(columns=["recruiter_id", "company_id"], errors="ignore")
    output_data = {"recruiters": df.to_dict(orient="records")}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    print("Tiền xử lý recruiter hoàn tất! File lưu tại:", OUTPUT_FILE)

if __name__ == "__main__":
    preprocess_recruiter()
