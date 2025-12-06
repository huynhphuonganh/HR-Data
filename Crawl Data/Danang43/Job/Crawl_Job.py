import sys, os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.append(project_root)
    
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import itertools
from Crawl.Get_Link import get_job_links
from Crawl.Job.Utils_Job import (
    extract_job_title,
    extract_job_expires_at,
    extract_job_salary,
    extract_job_status,
    extract_job_address,
    extract_job_description,
    extract_job_responsibilities,
    extract_job_benefits,
    extract_job_working_days,
    extract_job_employment_type,
    extract_job_work_mode,
    extract_job_years_experience
)

BASE_URL = "https://www.danang43.vn/viec-lam?page={}"

def get_job_info(link):
    try:
        resp = requests.get(link, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"Lỗi tải job {link}: {e}")
        return None

    soup = BeautifulSoup(resp.content, "html.parser")

    # --- gọi các hàm extract ---
    title = extract_job_title(soup)
    expires_at = extract_job_expires_at(soup)
    min_salary, max_salary = extract_job_salary(soup)
    status = extract_job_status(soup)
    address = extract_job_address(soup)
    description = extract_job_description(soup)
    responsibilities = extract_job_responsibilities(soup)
    benefits = extract_job_benefits(soup)
    working_days = extract_job_working_days(soup)
    employment_type = extract_job_employment_type(soup)
    work_mode = extract_job_work_mode(soup)
    years_experience = extract_job_years_experience(soup)
    job = {
        "jobID": None, 
        "companyID": None,
        "title": title,
        "department": None,           
        "employment_type": employment_type,
        "work_mode": work_mode,
        "working_days": working_days,
        "address": address,
        "line1": None,              
        "line2": None,
        "line3": "Đà Nẵng",
        "line4": "Việt Nam",
        "provinceCode": None,         
        "wardCode": None,             
        "CountryID": None,            
        "salary_min": min_salary,
        "salary_max": max_salary,
        "currency": "VND",          
        "description": description,
        "responsibilities": responsibilities,     
        "benefits": benefits,
        "years_experience": years_experience,    
        "status": status,
        "posted_at": None,            
        "expires_at": expires_at,
        "created_by": None,  # recruiter ID 
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }

    return job

id_counter = itertools.count(1)
store = {}

metadata = {
    "total_records": 0,
    "crawled_records": 0,
    "start_time": datetime.now().isoformat(),
    "last_updated": None,
    "source": "Danang43.vn Crawler",
    "description": "Jobs data crawled from Danang43.vn by job postings"
}


def gen_id():
    return next(id_counter)

def upsert_job(job, companyID, recruiterID):
    key = (job["title"], companyID)
    if key in store:
        return store[key]

    job["jobID"] = gen_id()
    job["companyID"] = companyID
    job["created_by"] = recruiterID
    store[key] = job

    metadata["crawled_records"] += 1
    metadata["last_updated"] = datetime.now().isoformat()

    return job


def get_all_job():
    return list(store.values())


def get_job_metadata():
    return {
        "metadata": metadata,
        "jobs": get_all_job()
    }
# def crawl_jobs(from_page=1, to_page=1):
#     all_jobs = []

#     for page in range(from_page, to_page + 1):
#         job_links = get_job_links(page)
#         for link in job_links:
#             job = get_job_info(link)
#             if job:
#                 all_jobs.append(job)

#         time.sleep(1)  # tránh bị chặn

#     print(f"Hoàn thành crawl, tổng cộng {len(all_jobs)} jobs")
#     return all_jobs



# if __name__ == "__main__":
#     # Crawl từ page 1, chỉ lấy thử vài job
#     jobs = crawl_jobs(from_page=1, to_page=1)

#     print(f"\nCrawl được {len(jobs)} jobs\n")

#     for i, job in enumerate(jobs[:15], start=1):
#         print(f"{i}. Job {i}:")
#         print(f"   - title: {job.get('title')}")
#         print(f"   - department: {job.get('department')}")
#         print(f"   - employment_type: {job.get('employment_type')}")
#         print(f"   - work_mode: {job.get('work_mode')}")
#         print(f"   - working_days: {job.get('working_days')}")
#         print(f"   - address: {job.get('address')}")
#         print(f"   - salary_min: {job.get('salary_min')}")
#         print(f"   - salary_max: {job.get('salary_max')}")
#         print(f"   - currency: {job.get('currency')}")
#         print(f"   - description: {job.get('description')}")
#         print(f"   - benefits: {job.get('benefits')}")
#         print(f"   - years_experience: {job.get('years_experience')}")
#         print(f"   - status: {job.get('status')}")
#         print(f"   - expires_at: {job.get('expires_at')}")
#         print(f"   - created_by: {job.get('created_by')}")
#         print("-" * 50)