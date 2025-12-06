import sys, os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.append(project_root)
    
import requests
from bs4 import BeautifulSoup
import time
import itertools
from datetime import datetime
from Crawl.Get_Link import get_job_links
from Crawl.Recruiter.Utils_Recruiter import (
    extract_name,
    extract_email,
    extract_phone
)

BASE_URL = "https://www.danang43.vn/viec-lam?page={}"

def get_recruiter_info(link):
    try:
        page = requests.get(link, timeout=10)
        page.raise_for_status()
    except Exception as e:
        print(f"Lỗi tải {link}: {e}")
        return None

    soup = BeautifulSoup(page.content, "html.parser")
    code = soup.find("code")
    if not code:
        return None

    raw_html = code.decode_contents().replace("<br>", "\n")
    lines = [BeautifulSoup(line, "html.parser").get_text(" ", strip=True)
             for line in raw_html.split("\n") if line.strip()]

    email, phone, name = None, None, None

    for a in code.find_all("a", href=True):
        if a["href"].startswith("mailto:"):
            email = a["href"].replace("mailto:", "").strip()
            break

    if not email:
        for line in lines:
            email = email or extract_email(line)

    for line in lines:
        phone = phone or extract_phone(line)
        name = name or extract_name(line)


    return {
        "recruiterID": None,
        "companyID": None,
        "full_name": name,
        "photo_url": None,
        "phone": phone,
        "email": email,
        "role": "Recruiter",
        "is_active": True,
        "created_by": None,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    


id_counter = itertools.count(1)
store = {}

metadata = {
    "total_records": 0,
    "crawled_records": 0,
    "start_time": datetime.now().isoformat(),
    "last_updated": None,
    "source": "Danang43.vn Crawler",
    "description": "Recruiters data crawled from Danang43.vn by job postings"
}

def gen_id():
    return next(id_counter)

def upsert_recruiter(recruiter, companyID):
    key = (recruiter.get("email"), companyID)
    if key in store:
        return store[key]

    recruiter["recruiterID"] = gen_id()
    recruiter["companyID"] = companyID
    store[key] = recruiter

    metadata["crawled_records"] += 1
    metadata["last_updated"] = datetime.now().isoformat()

    return recruiter

def get_all_recruiter():
    return list(store.values())

def get_Recruiter_metadata():
    return {
        "metadata": metadata,
        "recruiters": get_all_recruiter()
    }
# def crawl_recruiters(from_page=1, to_page=1):
#     all_recruiters = []

#     for page in range(from_page, to_page + 1):
#         job_links = get_job_links(page)
#         for link in job_links:
#             recruiter = get_recruiter_info(link)
#             if recruiter:
#                 all_recruiters.append(recruiter)

#         print(f"Hoàn thành page {page}, tổng {len(all_recruiters)} recruiters")
#         time.sleep(1)

#     return all_recruiters


# if __name__ == "__main__":
#     recruiters = crawl_recruiters(from_page=1, to_page=1)
#     for i, r in enumerate(recruiters[:10], start=1):
#         print(f"{i}. {r}")
