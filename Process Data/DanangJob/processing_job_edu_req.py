import json
import pandas as pd
import re
INPUT_FILE = "../Data/raw/job_edu_req.json"
JOB_RAW_FILE = "../Data/raw/job.json"
JOB_CLEAN_FILE = "../Data/process/jobs_clean.json"
OUTPUT_FILE = "../Data/process/job_edu_req_clean.json"
# ---- MAPPING ----
EDU_MAP = {
    "trung cấp": "high_school",
    "12/12": "high_school",
    "phổ thông": "high_school",
    "cao đẳng": "associate",
    "cđ": "associate",
    "đại học": "bachelor",
    "đh": "bachelor",
    "cử nhân": "bachelor",
    "thạc sĩ": "master",
    "tiến sĩ": "doctorate",}

def normalize_edu(value):
    if not value or str(value).strip() == "":
        return "other"
    val = str(value).lower().strip()
    for k, v in EDU_MAP.items():
        if k in val:
            return v
    return "other"

def preprocess_job_edu_req():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)
        df = pd.DataFrame(raw["data"])
    with open(JOB_RAW_FILE, "r", encoding="utf-8") as f:
        job_raw = json.load(f)
        job_raw = job_raw["data"] if isinstance(job_raw, dict) and "data" in job_raw else job_raw
    with open(JOB_CLEAN_FILE, "r", encoding="utf-8") as f:
        job_clean = json.load(f)
        job_clean = job_clean["jobs"] if isinstance(job_clean, dict) and "jobs" in job_clean else job_clean
    job_map = dict(zip(pd.DataFrame(job_raw)["job_id"], pd.DataFrame(job_clean)["jobID"]))
    # ---- Map jobID ----
    df["jobID"] = df["job_id"].map(job_map)
    # ---- Chuẩn hóa education_level ----
    df["education_level"] = df["education_level"].apply(normalize_edu)
    # ---- Kiểm tra mapping ----
    missing_jobs = df[df["jobID"].isna()]["job_id"].unique()
    if len(missing_jobs) > 0:
        print("Cảnh báo: Các job_id sau không tìm thấy trong mapping:", list(missing_jobs))
    else:
        print("Mapping job_id → jobID hợp lệ!")
    df.insert(0, "jobEducationReqID", range(1, len(df) + 1))
    # ---- Chọn & sắp xếp cột ----
    df = df[["jobEducationReqID", "jobID", "education_level", "field_of_study", "mandatory"]]
    output_data = {"jobEducationReqs": df.to_dict(orient="records")}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    print(f"Đã lưu {len(df)} job education reqs tại: {OUTPUT_FILE}")

if __name__ == "__main__":
    preprocess_job_edu_req()
