import itertools
from datetime import datetime

def create_job_skill():
    return {
        "jobSkillID": None,          
        "jobID": None,             
        "skillID": None,         
        "mandatory": True,           
        "min_proficiency": None,     
        "min_years_experience": None,
        "notes": None                
    }

id_counter = itertools.count(1)
store = []   # chỉ cần list, không cần dict

metadata = {
    "total_records": 0,
    "crawled_records": 0,
    "start_time": datetime.now().isoformat(),
    "last_updated": None,
    "source": "Danang43.vn Crawler",
    "description": "Mapping between jobs and skills crawled from Danang43.vn"
}

def gen_id():
    return next(id_counter)

def insert_job_skill(jobID, skillID):
    js = create_job_skill()
    js["jobSkillID"] = gen_id()
    js["jobID"] = jobID
    js["skillID"] = skillID
    store.append(js)

    metadata["crawled_records"] += 1
    metadata["last_updated"] = datetime.now().isoformat()
    return js

def get_all_jobskill():
    return store

def get_JobSkill_metadata():
    return {
        "metadata": metadata,
        "jobskills": get_all_jobskill()
    }