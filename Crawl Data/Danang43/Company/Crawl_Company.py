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
from Crawl.Company.Utils_Company import (
    extract_company_name,
    extract_company_website,
    extract_company_contact
)

BASE_URL = "https://www.danang43.vn/viec-lam?page={}"

def get_company_info(link):
    website = None
    address, size = None, None

    try:
        resp = requests.get(link, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"Lỗi tải job {link}: {e}")
        return None
    
    soup = BeautifulSoup(resp.content, "html.parser")
    
    name = extract_company_name(soup)
    website = extract_company_website(soup)
    address, size = extract_company_contact(soup)

    return {
        "companyID": None,
        "name": name,
        "website": website,
        "industry": None,
        "size_range": size,
        "line1": None,
        "line2": None,
        "line3": 'Đà Nẵng',
        "line4": 'Việt Nam',
        "address": address,
        "hq_city_by": None,
        "hq_ward_by": None,
        "hq_country_by": None,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }

id_counter = itertools.count(1)
store = {}

# metadata lưu thông tin quá trình crawl
metadata = {
    "total_records": 0,
    "crawled_records": 0,
    "start_time": datetime.now().isoformat(),
    "last_updated": None,
    "source": "Danang43.vn Crawler",
    "description": "Companies data crawled from Danang43.vn by job postings"
}


def gen_id():
    return next(id_counter)


def upsert_company(company):
    name = company.get("name")
    website = company.get("website")

    if isinstance(name, list):
        name = " ".join(name) if name else None
    if isinstance(website, list):
        website = " ".join(website) if website else None

    key = (name, website)
    if key in store:
        return store[key]

    company["companyID"] = gen_id()
    store[key] = company

    metadata["crawled_records"] += 1
    metadata["last_updated"] = datetime.now().isoformat()

    return company


def get_all_company():
    return list(store.values())


def get_company_metadata():
    return {
        "metadata": metadata,
        "companies": get_all_company()
    }

# def crawl_companies(from_page=1, to_page=1):
#     all_companies = []
    
#     for page in range(from_page, to_page + 1):
#         job_links = get_job_links(page)
#         for link in job_links:
#             company = get_company_info(link)
#             if company:
#                 all_companies.append(company)

#         print(f"Hoàn thành page {page}, tổng {len(all_companies)} companies")
#         time.sleep(1)

#     return all_companies

# if __name__ == "__main__":
#     companies = crawl_companies(from_page=1, to_page=1)
#     print(f"Crawl được {len(companies)} công ty")
#     for c in companies[:20]:
#         print(f"{c['companyID']}. {c['name']} | {c['website']} | {c['address']} | {c['size_range']}")