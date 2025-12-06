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
from Crawl.JobEducationReq.Utils_JobEduReq import (extract_job_education_requirements)


def get_job_edu_req_info(job_url):
    
    try:
        resp = requests.get(job_url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"Lỗi tải job {job_url}: {e}")
        return []

    soup = BeautifulSoup(resp.content, "html.parser")

    requirements = extract_job_education_requirements(soup)

    job_edu_reqs = []
    if requirements:
        for req in requirements:
            job_edu_reqs.append({
                "jobEducationReqID": None,
                "jobID": None,
                "education_level": req.get("education_level"),
                "field_of_study": req.get("field_of_study"),
                "mandatory": req.get("mandatory")
            })

    return job_edu_reqs



id_counter = itertools.count(1)
store = []

metadata = {
    "total_records": 0,
    "crawled_records": 0,
    "start_time": datetime.now().isoformat(),
    "last_updated": None,
    "source": "Danang43.vn Crawler",
    "description": "Job education requirements data crawled from Danang43.vn by job postings"
}

def gen_id():
    return next(id_counter)

def insert_education(req, jobID):
    req["jobEducationReqID"] = gen_id()
    req["jobID"] = jobID
    store.append(req)

    metadata["total_records"] = len(store)
    metadata["crawled_records"] += 1
    metadata["last_updated"] = datetime.now().isoformat()

    return req

def get_all_education():
    return store

def get_JobEduReq_metadata():
    return {
        "metadata": metadata,
        "job_education_requirements": get_all_education()
    }
# def crawl_job_edu_req(from_page=1, to_page=1):
#     all_job_edu_reqs = []

#     for page in range(from_page, to_page + 1):
#         job_links = get_job_links(page)
#         for link in job_links:
#             jobs = get_job_edu_req_info(link)
#             if jobs:
#                 all_job_edu_reqs.extend(jobs)

#         time.sleep(1)  # tránh bị chặn

#     print(f"Hoàn thành crawl, tổng cộng {len(all_job_edu_reqs)} requests")
#     return all_job_edu_reqs



# if __name__ == "__main__":
#     # Crawl từ page 1, chỉ lấy thử vài job
#     jobs = crawl_job_edu_req(from_page=1, to_page=1)

#     print(f"\nCrawl được {len(jobs)} requests\n")

#     for i, job in enumerate(jobs[:40], start=1):
#         print(f"{i}. Job {i}:")
#         print(f"   - jobEducationReqID: {job.get('jobEducationReqID')}")
#         print(f"   - jobID: {job.get('jobID')}")
#         print(f"   - education_level: {job.get('education_level')}")
#         print(f"   - field_of_study: {job.get('field_of_study')}")
#         print(f"   - mandatory: {job.get('mandatory')}")
#         print("-" * 50)