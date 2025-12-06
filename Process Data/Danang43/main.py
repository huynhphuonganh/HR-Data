import json
import os
import pandas as pd
import numpy as np
import math
from datetime import datetime
from pandas import json_normalize
from process_company import  process_company
from process_job import  process_job
from process_recruiter import  process_recruiter
from process_jobedureq import  process_job_education_req
from process_skill import process_skill

RAW_PATHS = {
    "company": "data/raw/company.json",
    "job": "data/raw/job.json",
    "recruiter": "data/raw/recruiter.json",
    "job_edu": "data/raw/jobeducationreq.json",
    "skill": "data/raw/skill.json",
    "job_skill": "data/raw/jobskill.json"
}

#  Hàm load file JSON 
def load_json(raw_path: str, key: str) -> pd.DataFrame:
    """Load JSON và flatten theo key."""
    with open(raw_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return json_normalize(data.get(key, []))

#  Hàm quan hệ 1-n (foreign key thông thường) ===
def clean_relations(df_child, df_parent, fk_col, pk_col):
    """Xóa record con không còn parent."""
    valid_ids = set(df_parent[pk_col].dropna())
    return df_child[df_child[fk_col].isin(valid_ids)]

# Hàm quan hệ nhiều-nhiều job–skill ===
def clean_job_skill_relations(df_jobSkill, df_job, df_skill):
    """Xử lý quan hệ job–skill và xóa skill không còn job nào dùng."""
    valid_job_ids = set(df_job["jobID"].dropna())
    df_jobSkill = df_jobSkill[df_jobSkill["jobID"].isin(valid_job_ids)]

    valid_skill_ids = set(df_skill["skillID"].dropna())
    df_jobSkill = df_jobSkill[df_jobSkill["skillID"].isin(valid_skill_ids)]

    used_skill_ids = set(df_jobSkill["skillID"].dropna())
    df_skill = df_skill[df_skill["skillID"].isin(used_skill_ids)]

    return df_jobSkill, df_skill

def clean_record_values(record: dict):
    """Thay NaN/Inf trong record bằng None."""
    clean = {}
    for k, v in record.items():
        if isinstance(v, (float, np.floating)) and (math.isnan(v) or math.isinf(v)):
            clean[k] = None
        elif pd.isna(v):
            clean[k] = None
        elif isinstance(v, dict):
            clean[k] = clean_record_values(v)
        elif isinstance(v, list):
            clean[k] = [clean_record_values(x) if isinstance(x, dict) else (None if isinstance(x, float) and math.isnan(x) else x) for x in v]
        else:
            clean[k] = v
    return clean
 
def main():
    # --- Load & xử lý ---
    df_company = process_company(load_json(RAW_PATHS["company"], "companies"))
    df_recruiter = process_recruiter(load_json(RAW_PATHS["recruiter"], "recruiters"))
    df_job = process_job(load_json(RAW_PATHS["job"], "jobs"))
    df_jobEdu = process_job_education_req(load_json(RAW_PATHS["job_edu"], "job_education_requirements"))
    df_skill = process_skill(load_json(RAW_PATHS["skill"], "skills"))
    df_jobSkill = load_json(RAW_PATHS["job_skill"], "jobskills")
    
    # --- Xóa các công ty lỗi ---
    deleted_companies = set(df_company[df_company["address"].isna()]["companyID"])
    df_company = df_company[df_company["address"].notna()]
    
    # --- Xóa các record liên quan --- 
    df_recruiter = df_recruiter[~df_recruiter["companyID"].isin(deleted_companies)]
    df_job = df_job[~df_job["companyID"].isin(deleted_companies)]
    df_jobEdu = clean_relations(df_jobEdu, df_job, "jobID", "jobID")
    df_jobSkill, df_skill = clean_job_skill_relations(df_jobSkill, df_job, df_skill)
    
    # --- Xuất JSON ---
    output_dir = "Data/processed"
    os.makedirs(output_dir, exist_ok=True)

    output_files = {
        "company": ("companies", df_company),
        "recruiter": ("recruiters", df_recruiter),
        "job": ("jobs", df_job),
        "job_edu": ("job_education_requirements", df_jobEdu),
        "skill": ("skills", df_skill),
        "job_skill": ("jobskills", df_jobSkill),
    }
    

    for name, (json_key, df) in output_files.items():
        output_path = os.path.join(output_dir, f"{name}_processed.json")

        # --- Convert thành list of dict ---
        records = df.to_dict(orient="records")

        # --- Làm sạch NaN / inf / NaT trong từng record ---
        clean_records = [clean_record_values(r) for r in records]

        # --- Metadata ---
        metadata = {
            "table_name": name,
            "total_records": int(len(df)),
            "processed_time": datetime.now().isoformat(),
            "description": f"Cleaned and standardized {name} data."
        }

        
        obj = {
            "metadata": metadata,
            json_key: clean_records
        }

        # --- Ghi file JSON ---
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, separators=(',', ': '))
            
    print("Dữ liệu đã xử lý và lưu tại:", output_dir) 
    
if __name__ == "__main__":
    main()


