import os
import re
import json
import requests
import hashlib
import time
from bs4 import BeautifulSoup
from datetime import datetime
from getlinkJob import get_categories, crawl_linkjobs, safe_request
from Company.extract_company import get_company_info
from Recruiter.extract_recruiter import get_contact_info, extract_role
from Job.extract_job import get_job_info
from JobEduReq.extract_jobedureq import crawl_job_edu_req
from Skill.extract_skill import (extract_job_skill_name, categorize_skill, get_field_by_category, infer_type_from_category, FIELD_MAPPING,)
# ===== CẤU HÌNH TỐI ƯU =====
CHECKPOINT_INTERVAL = 50
JOB_DELAY = 0.3
# --- Hàm tạo ID ổn định, không trùng ---
def make_id(prefix, key):
    """Sinh ID ổn định, không đổi giữa các lần crawl."""
    h = hashlib.md5(key.encode("utf-8")).hexdigest()[:10]
    return f"{prefix}_{h.upper()}"
def safe_str(value):
    return str(value or "").strip().lower()

# --- Load/Save checkpoint ---
def load_checkpoint(filename="crawl_checkpoint.json"):
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                data = json.load(f)
                print(f"Đã load checkpoint: {len(data.get('results', []))} job")
                return data
        except:
            return None
    return None
def save_checkpoint(data, filename="crawl_checkpoint.json"):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ Lỗi lưu checkpoint: {e}")
# HÀM CHÍNH
def crawl_all_jobs(max_cat=3, max_pages=2, resume_from_checkpoint=True):
    checkpoint = load_checkpoint() if resume_from_checkpoint else None
    if checkpoint:
        all_results = checkpoint.get("results", [])
        seen_jobs = set(checkpoint.get("seen_jobs", []))
        seen_companies = set(checkpoint.get("seen_companies", []))
        seen_recruiters = set(checkpoint.get("seen_recruiters", []))
        processed_categories = set(checkpoint.get("processed_categories", []))
        print(f"🔄 Tiếp tục từ checkpoint: {len(all_results)} job")
    else:
        all_results = []
        seen_jobs = set()
        seen_companies = set()
        seen_recruiters = set()
        processed_categories = set()
    categories = get_categories()[:max_cat]
    job_counter = len(all_results)

    for cat in categories:
        if cat["name"] in processed_categories:
            print(f" Bỏ qua ngành đã crawl: {cat['name']}")
            continue
            
        print(f"\n Đang crawl ngành: {cat['name']}")
        job_links = crawl_linkjobs(cat["url"], max_pages=max_pages)
        print(f"Tổng {len(job_links)} link job.")
        field_value = next(
            (v for k, v in FIELD_MAPPING.items()
             if k.lower() in cat["name"].lower() or cat["name"].lower() in k.lower()),
            "Other")
        print(f"Field cho ngành '{cat['name']}': {field_value}")
        for job_url in job_links:
            try:
                print(f"[{job_counter + 1}] Đang xử lý: {job_url}")
                r = safe_request(job_url)
                if not r:
                    print(f" Không thể tải job: {job_url}")
                    continue
                soup = BeautifulSoup(r.text, "html.parser")
                job_data = get_job_info(soup)
                company_data = get_company_info(soup)
                recruiter_data = get_contact_info(job_url)
                recruiter_data["role"] = extract_role(recruiter_data.get("email"))
                edu_data = crawl_job_edu_req(soup, job_url)
                skills_raw = extract_job_skill_name(soup)
                # ===== Chuẩn hóa danh sách kỹ năng =====
                skills = []
                for s in skills_raw:
                    cat_name = categorize_skill(s)
                    skills.append({
                        "skill_name": s,
                        "category": cat_name,
                        "type": infer_type_from_category(cat_name),
                        "field": field_value})
                # --- Loại trùng skill trong cùng job ---
                unique_skills = []
                seen_skill = set()
                for s in skills:
                    key = tuple(s.items())
                    if key not in seen_skill:
                        seen_skill.add(key)
                        unique_skills.append(s)
                skills = unique_skills
                # ===== TẠO KEY CHỐNG TRÙNG =====
                company_key = f"{safe_str(company_data.get('name'))}|{safe_str(company_data.get('address'))}"
                recruiter_key = f"{safe_str(recruiter_data.get('name'))}|{safe_str(recruiter_data.get('email'))}|{safe_str(recruiter_data.get('phone'))}"
                job_key = f"{safe_str(job_data.get('title'))}|{safe_str(company_data.get('name'))}|{safe_str(company_data.get('address'))}"
                # ===== SINH ID ỔN ĐỊNH =====
                company_id = make_id("COMP", company_key)
                recruiter_id = make_id("REC", recruiter_key)
                job_id = make_id("JOB", job_key)
                # ===== Kiểm tra trùng =====
                if job_id in seen_jobs:
                    print(f"Bỏ qua job trùng: {job_id}")
                    continue
                seen_jobs.add(job_id)
                # KIỂM TRA COMPANY TRÙNG - chỉ đánh dấu, không ngăn job
                company_is_new = company_id not in seen_companies
                if company_is_new:
                    seen_companies.add(company_id)
                else:
                    print(f"Company trùng: {company_id}")
                # KIỂM TRA RECRUITER TRÙNG - chỉ đánh dấu, không ngăn job
                recruiter_is_new = recruiter_id not in seen_recruiters
                if recruiter_is_new:
                    seen_recruiters.add(recruiter_id)
                else:
                    print(f"Recruiter trùng: {recruiter_id}")
                # ===== ÁNH XẠ KHÓA CHÍNH / NGOẠI =====
                job_data["job_id"] = job_id
                company_data["company_id"] = company_id
                recruiter_data["recruiter_id"] = recruiter_id
                recruiter_data["company_id"] = company_id
                job_data["company_id"] = company_id
                job_data["recruiter_id"] = recruiter_id
                for edu in edu_data:
                    edu["job_id"] = job_id
                for s in skills:
                    s["skill_id"] = make_id("SKILL", safe_str(s["skill_name"]))
                job_skill_records = [
                    {"job_id": job_id, "skill_id": s["skill_id"]}
                    for s in skills]
                # ===== GỘP DỮ LIỆU =====
                #Chỉ thêm company/recruiter nếu là mới
                data = {
                    "metadata": {
                        "source": job_url,
                        "category": cat["name"],
                        "crawled_at": datetime.now().isoformat(),
                    },
                    "job": job_data,
                    "company": company_data if company_is_new else None,  
                    "recruiter": recruiter_data if recruiter_is_new else None,  
                    "job_edu_req": edu_data,
                    "skills": skills,
                    "job_skill": job_skill_records,}
                all_results.append(data)
                job_counter += 1
                if job_counter % CHECKPOINT_INTERVAL == 0:
                    checkpoint_data = {
                        "results": all_results,
                        "seen_jobs": list(seen_jobs),
                        "seen_companies": list(seen_companies),
                        "seen_recruiters": list(seen_recruiters),
                        "processed_categories": list(processed_categories),
                        "last_update": datetime.now().isoformat()}
                    save_checkpoint(checkpoint_data)
                    print(f"Đã lưu checkpoint tại job #{job_counter}")
                time.sleep(JOB_DELAY)
            except Exception as e:
                print(f"Lỗi xử lý job: {e}")
        processed_categories.add(cat["name"])
        checkpoint_data = {
            "results": all_results,
            "seen_jobs": list(seen_jobs),
            "seen_companies": list(seen_companies),
            "seen_recruiters": list(seen_recruiters),
            "processed_categories": list(processed_categories),
            "last_update": datetime.now().isoformat()}
        save_checkpoint(checkpoint_data)
        print(f"Đã lưu checkpoint sau ngành '{cat['name']}'")
    return all_results

# CHẠY TOÀN BỘ
if __name__ == "__main__":
    results = crawl_all_jobs(max_cat=26, max_pages=300)
    print(f"\n🎯 Crawl hoàn tất {len(results)} job duy nhất (sau khi lọc trùng).")
    total_jobs = len(results)
    crawl_time = datetime.now().isoformat()

    def save_json_with_metadata(filename, records, category_name="Tổng hợp"):
        metadata = {
            "category": category_name,
            "total_jobs": total_jobs,
            "total_records": len(records),
            "crawl_time": crawl_time,
        }
        output = {"metadata": metadata, "data": records}
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        print(f"💾 Đã lưu {filename} ({len(records)} bản ghi)")
    job_records = [item["job"] for item in results]
    company_records = [item["company"] for item in results if item["company"] is not None]
    recruiter_records = [item["recruiter"] for item in results if item["recruiter"] is not None]
    job_edu_req_records = []
    for item in results:
        if isinstance(item["job_edu_req"], list):
            job_edu_req_records += item["job_edu_req"]
    skills_dict = {}
    for item in results:
        for s in item["skills"]:
            skill_id = s["skill_id"]
            if skill_id not in skills_dict:
                skills_dict[skill_id] = s
    skills_records = list(skills_dict.values())
    job_skill_records = []
    for item in results:
        if isinstance(item["job_skill"], list):
            job_skill_records += item["job_skill"]
    # Lưu file
    category_name = results[0].get("metadata", {}).get("category", "Tổng hợp") if results else "Tổng hợp"
    save_json_with_metadata("job.json", job_records, category_name)
    save_json_with_metadata("company.json", company_records, category_name)
    save_json_with_metadata("recruiter.json", recruiter_records, category_name)
    save_json_with_metadata("job_edu_req.json", job_edu_req_records, category_name)
    save_json_with_metadata("skills.json", skills_records, category_name)
    save_json_with_metadata("job_skill.json", job_skill_records, category_name)