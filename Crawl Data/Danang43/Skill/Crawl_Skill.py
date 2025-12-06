import sys, os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.append(project_root)
    
from collections import defaultdict
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import itertools
from Crawl.Get_Link import get_job_links
from Crawl.Skill.Utils_Skill import (extract_job_skill_name, categorize_skill, extract_skill_field)


BASE_URL = "https://www.danang43.vn/viec-lam?page={}"

def get_skill_info(link):
    try:
        resp = requests.get(link, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"Lỗi tải job {link}: {e}")
        return []

    soup = BeautifulSoup(resp.content, "html.parser")

    names = extract_job_skill_name(soup)  
    field = extract_skill_field(soup)
    skills = []
    
    if names:
        for name in names:
            category = categorize_skill(name)
            skills.append({
                "skillID": None,
                "name": name,
                "category": category,
                "field": field,
                "type": 'customize',
                "description": None
            })
    return skills

id_counter = itertools.count(1)
store = {}
metadata = {
    "total_records": 0,
    "crawled_records": 0,
    "start_time": datetime.now().isoformat(),
    "last_updated": None,
    "source": "Danang43.vn Crawler",
    "description": "Skills data crawled from Danang43.vn by job postings"
}


def gen_id():
    return next(id_counter)


def upsert_skill(skill):
    key = skill["name"].lower().strip()
    if key in store:
        return store[key]
    
    skill["skillID"] = gen_id()
    store[key] = skill

    metadata["crawled_records"] += 1
    metadata["last_updated"] = datetime.now().isoformat()

    return skill


def get_all_skill():
    return list(store.values())


def get_skill_metadata():
    return {
        "metadata": metadata,
        "skills": get_all_skill()
    }


# def crawl_skills(from_page=1, to_page=1):
#     all_skills = []
    
#     for page in range(from_page, to_page + 1):
#         job_links = get_job_links(page)
#         for link in job_links:
#             skills = get_skill_info(link)
#             if skills:
#                 all_skills.extend(skills)
            

#         time.sleep(1)  # tránh bị chặn

#     print(f"Hoàn thành crawl, tổng cộng {len(all_skills)} skills")
#     return all_skills


# if __name__ == "__main__":
#     skills = crawl_skills(from_page=1, to_page=1)

#     print(f"\nCrawl được {len(skills)} skill records\n")

#     for skill in skills[:20]:
#         print(f" - {skill['name']} → {skill['category']} → {skill['field']}")
