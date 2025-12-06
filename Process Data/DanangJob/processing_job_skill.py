import json
RAW_FILE = "../Data/raw/job_skill.json"
JOB_RAW = "../Data/raw/job1.json"
JOB_CLEAN = "../Data/process/jobs_clean.json"
SKILL_RAW = "../Data/raw/skills.json"
SKILL_CLEAN = "../Data/process/skill_clean.json"
OUT_FILE = "../Data/process/job_skill_clean.json"
with open(RAW_FILE, 'r', encoding='utf-8') as f:
    job_skill_raw = json.load(f)['data']
with open(JOB_RAW, 'r', encoding='utf-8') as f:
    job_raw = json.load(f)['data']
with open(JOB_CLEAN, 'r', encoding='utf-8') as f:
    job_clean = json.load(f)['jobs']  
with open(SKILL_RAW, 'r', encoding='utf-8') as f:
    skill_raw = json.load(f)['data']
with open(SKILL_CLEAN, 'r', encoding='utf-8') as f:
    skill_clean = json.load(f)['data']
# Tạo mapping dictionaries
job_mapping = {job_raw[i]['job_id']: job_clean[i]['jobID'] 
               for i in range(len(job_raw))}

skill_mapping = {skill_raw[i]['skill_id']: skill_clean[i]['skillID'] 
                 for i in range(len(skill_raw))}
# Xử lý job_skil
job_skill_clean = []
for idx, item in enumerate(job_skill_raw, start=1):
    job_skill_clean.append({
        "jobSkillID": idx,
        "jobID": job_mapping[item['job_id']],
        "skillID": skill_mapping[item['skill_id']],
        "mandatory": True,
        "min_proficiency": None,
        "min_years_experience": None,
        "notes": None})
with open(OUT_FILE, 'w', encoding='utf-8') as f:
    json.dump({"data": job_skill_clean}, f, indent=2, ensure_ascii=False)

print(f" Đã xử lý {len(job_skill_clean)} records")
print(f" Lưu vào: {OUT_FILE}")