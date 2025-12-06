import json
from datetime import datetime
from Get_Link import get_job_links
from Company.Crawl_Company import get_company_info, upsert_company, get_company_metadata
from Recruiter.Crawl_Recruiter import get_recruiter_info, upsert_recruiter, get_Recruiter_metadata
from Job.Crawl_Job import get_job_info, upsert_job, get_job_metadata
from JobEducationReq.Crawl_JobEduReq import get_job_edu_req_info, insert_education, get_JobEduReq_metadata
from Skill.Crawl_Skill import get_skill_info, upsert_skill, get_skill_metadata
from JobSkill.Crawl_JobSkill import create_job_skill, insert_job_skill, get_JobSkill_metadata



def process_job_link(link):
    company = upsert_company(get_company_info(link))
    
    recruiter = upsert_recruiter(get_recruiter_info(link), company["companyID"])
    
    job = upsert_job(get_job_info(link), company["companyID"], recruiter["recruiterID"])

    for req in get_job_edu_req_info(link):
        insert_education(req, job["jobID"])

    for skill in get_skill_info(link):
        s = upsert_skill(skill)
        insert_job_skill(job["jobID"], s["skillID"])

def export_to_json():
    tables = {
        "Company": get_company_metadata(),
        "Recruiter": get_Recruiter_metadata(),
        "Job": get_job_metadata(),
        "JobEducationReq": get_JobEduReq_metadata(),
        "Skill": get_skill_metadata(),
        "JobSkill": get_JobSkill_metadata(),
    }

    for name, content in tables.items():
        filename = f"{name.lower()}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=2, default=str)
        print(f"✅ Đã xuất {filename} ({len(content.get(name.lower(), []))} bản ghi)")
    
# def main(from_page=1, to_page=1):
#     total_links = 0

#     for page in range(from_page, to_page + 1):
#         links = get_job_links(page)
#         total_links += len(links)
#         for link in links:
#             process_job_link(link)

#     export_to_json()

#     print(f" - Tổng số job đã crawl: {total_links}")
#     print(" - Dữ liệu đã xuất thành các file JSON.")


def main(start_page=1):
    total_links = 0
    page = start_page

    while True:
        links = get_job_links(page)
        if not links:
            print(f"🚩 Hết dữ liệu tại trang {page - 1}. Dừng crawl.")
            break

        print(f"🔹 Đang xử lý trang {page} ({len(links)} jobs)")
        total_links += len(links)

        for link in links:
            process_job_link(link)

        page += 1 

    export_to_json()
    
    print(f" - Tổng số job đã crawl: {total_links}")
    print(" - Dữ liệu đã xuất thành các file JSON.")
    
      
if __name__ == "__main__":
    main()

