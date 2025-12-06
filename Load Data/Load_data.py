import os
import asyncio
import sys

# Import modules
from Service.create_account import CreateAccount
from Service.skills import LoadSkillsData
from Service.company import LoadCompanyData
from Service.job import LoadJobData
from Service.recruiter import LoadRecruiterData
from Service.job_edu_req import LoadJobEducationReqData
from Service.job_skill import LoadJobSkill


async def main():
    """Main function to load all data."""
    print("Starting data loading process...")
    
    try:
        # # 1. Create data into table users (run only once)
        create_account = CreateAccount()
        await create_account.create_all_accounts()
        print("Creating Account Data")

        # # 2. Load data into table skill
        skills_data = LoadSkillsData()
        skills = await skills_data.load_skills_unique()
        await skills_data.insert_skill_unique(skills)
        print("Loading Skills Data")

        # # 3. Load data into table company
        company_data = LoadCompanyData()
        companies = await company_data.load_companies()
        await company_data.insert_company(companies)
        print("Loading Company Data")

        # # 4. Load data into table recruiter
        recruiter_data = LoadRecruiterData()
        recruiters = await recruiter_data.load_recruiters()
        await recruiter_data.insert_recruiter(recruiters)
        print("Loading Recruiter Data") 

        # # 5. Load data into table job
        job_data = LoadJobData()
        data_sources = ["Danang43", "DanangJob"]

        for src in data_sources:
            jobs = await job_data.load_jobs(src)
            await job_data.insert_job(jobs)
        print("Loading Job Data")

        # # 7. Load data into table job_education_requirement
        edu_loader = LoadJobEducationReqData()
        data_sources = ["Danang43", "DanangJob"]
        
        for src in data_sources:
            job_edu_reqs = await edu_loader.load_job_education_reqs(src)
            await edu_loader.insert_job_education_reqs(job_edu_reqs)
        print("Loading Job Education Requirement Data")
        
        # 8. Load data into table job_skill
        job_skill_loader = LoadJobSkill()
        data_sources = ["Danang43", "DanangJob"]
        
        for src in data_sources:
            job_skills = await job_skill_loader.load_job_skills(src)
            await job_skill_loader.insert_job_skills(job_skills)
        print("Loading Job Skill Data")
        
        
        print("ALL DATA LOADED SUCCESSFULLY!")

    except Exception as e:
        print(f"Critical error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
