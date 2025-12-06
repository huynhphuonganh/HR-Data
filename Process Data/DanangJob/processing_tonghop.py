import json
import re
import os
from datetime import datetime
# === 1. Load JSON files ===
BASE_PATH = "../Data/process"
def load_json(filename, key=None):
    with open(os.path.join(BASE_PATH, filename), encoding="utf-8") as f:
        data = json.load(f)
        return data[key] if key else data
companies = load_json("company_clean.json")
jobs_data = load_json("jobs_clean.json", "jobs")
recruiters_data = load_json("recruiter_clean.json", "recruiters")
job_edu_reqs = load_json("job_edu_req_clean.json", "jobEducationReqs")
job_skills = load_json("job_skill_clean.json", "data")
skills = load_json("skill_clean.json", "data")

# === 2. Chuẩn hóa số điện thoại về dạng 10 số chuẩn VN ===
def normalize_phone(phone):
    if not phone:
        return None
    phone = str(phone)
    phone = re.sub(r"[^0-9\+]", "", phone)  # Giữ lại số và dấu +
    phone = re.sub(r"^\+84", "0", phone)    # Đổi mã +84 → 0
    phone = re.sub(r"^84", "0", phone)
    # Nếu có nhiều số, lấy số đầu tiên hợp lệ
    candidates = re.findall(r"0\d{9}", phone)
    return candidates[0] if candidates else None

# === 3. Làm sạch bảng recruiter ===
for idx, r in enumerate(recruiters_data, start=1):
    # Chuẩn hóa lại dữ liệu (giữ nguyên 2 cột phone và email)
    r["phone"] = normalize_phone(r.get("phone"))
    r["email"] = r.get("email")  
    # Giữ khóa kép là (phone, email)
    r["company_name"] = None  # sẽ gán sau bằng map
    r.pop("recruiterID", None)
    r.pop("companyID", None)

# === 4. Tạo mapping (định danh ID → tên, recruiter) ===
company_map = {c["companyID"]: c["name"] for c in companies}
job_map = {j["jobID"]: j["title"] for j in jobs_data}
job_company_map = {j["jobID"]: company_map.get(j["companyID"]) for j in jobs_data}
skill_map = {s["skillID"]: s["name"] for s in skills}
recruiter_map = {}

# Tạo map recruiterID → recruiter record (để lookup)
with open(os.path.join(BASE_PATH, "recruiter_clean.json"), encoding="utf-8") as f:
    raw_recruiters = json.load(f)["recruiters"]

for rr in raw_recruiters:
    phone = normalize_phone(rr.get("phone"))
    email = rr.get("email")
    if rr.get("recruiterID"):
        recruiter_map[rr["recruiterID"]] = {
            "phone": phone,
            "email": email
        }

# === 5. Thay ID bằng tên và thêm các trường tham chiếu ===
# --- Company ---
for c in companies:
    c.pop("companyID", None)

# --- Jobs ---
for j in jobs_data:
    rec_id = j.get("recruiterID")
    rec_info = recruiter_map.get(rec_id, {})
    j["created_by"] = None
    j.pop("recruiterID", None)
    j.pop("companyID", None)
    j.pop("jobID", None)
    # ❌ Yêu cầu 2: xóa 2 cột recruiter_phone, recruiter_email (nếu có)
    j.pop("recruiter_phone", None)
    j.pop("recruiter_email", None)

# --- Recruiters ---
for rr in recruiters_data:
    for raw in raw_recruiters:
        if (
            normalize_phone(raw.get("phone")) == rr["phone"]
            and raw.get("email") == rr["email"]
        ):
            rr["company_name"] = company_map.get(raw.get("companyID"))

# --- JobEduReq ---
for e in job_edu_reqs:
    #Yêu cầu 1: đổi khóa ngoại job_title → title
    e["title"] = job_map.get(e["jobID"])
    e.pop("jobEducationReqID", None)
    e.pop("jobID", None)

# --- JobSkill ---
for js in job_skills:
    js["title"] = job_map.get(js["jobID"])
    js["skill_name"] = skill_map.get(js["skillID"])
    js.pop("jobSkillID", None)
    js.pop("jobID", None)
    js.pop("skillID", None)
    #  xóa company_name
    js.pop("company_name", None)

# --- Skills ---
for s in skills:
    s.pop("skillID", None)  # dùng name làm khóa chính

# === 6. Ghi file sau khi xử lý ===
OUTPUT_PATH = "../Data/processed"
os.makedirs(OUTPUT_PATH, exist_ok=True)

def make_table_json(table_name, records):
    """Tạo cấu trúc JSON có metadata cho từng bảng."""
    return {
        "metadata": {
            "table_name": table_name,
            "total_records": len(records),
            "processed_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "description": "Cleaned and standardized data."
        },
        table_name: records
    }

def save_json(filename, data):
    with open(os.path.join(OUTPUT_PATH, filename), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# Ghi từng file JSON riêng
save_json("company_processed.json", make_table_json("companies", companies))
save_json("jobs_processed.json", make_table_json("jobs", jobs_data))
save_json("recruiter_processed.json", make_table_json("recruiters", recruiters_data))
save_json("jobEduReq_processed.json", make_table_json("jobEducationReqs", job_edu_reqs))
save_json("jobSkill_processed.json", make_table_json("jobSkills", job_skills))
save_json("skill_processed.json", make_table_json("skills", skills))

print("✅ Hoàn tất tiền xử lý và ghi file tại:", OUTPUT_PATH)
