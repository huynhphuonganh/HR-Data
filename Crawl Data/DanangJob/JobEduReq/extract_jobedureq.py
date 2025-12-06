import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse
import re
def crawl_job_edu_req(soup, job_url: str):
    records = []
    box = soup.select_one("#ctl00_ContentPlaceHolder1_lblmota")
    if not box:
        return records  
    text = box.get_text("\n", strip=True)
    text_lower = text.lower()
    text_clean = re.sub(r"\s+", " ", text_lower)  
    # ===== 2. Tìm education_level (có thể nhiều) =====
    edu_patterns = [
        r"(đại học|cao đẳng|trung cấp|thạc sĩ|cử nhân|phổ thông|12/12|CĐ|ĐH|)",
        r"(college|university|bachelor|master|phd)"]
    edu_matches = set()
    for p in edu_patterns:
        edu_matches.update(re.findall(p, text_clean, flags=re.I))
    edu_levels = [re.sub(r"\s+", " ", m).title() for m in edu_matches] or [""]
    # ===== 3. Tìm field_of_study (có thể nhiều) =====
    field_patterns = [
        r"(?:chuyên ngành|ngành học|field of study|major)\s*[:\-]?\s*([A-Za-zÀ-ỹ0-9\s,/&]+)",
        r"(?:đại học|cao đẳng).*chuyên ngành\s*([A-Za-zÀ-ỹ0-9\s,/&]+)" ]
    majors_raw = []
    for p in field_patterns:
        majors_raw += re.findall(p, text_clean, flags=re.I)
    #  Loại trùng giữ nguyên thứ tự
    majors_raw = list(dict.fromkeys(m.strip() for m in majors_raw if m.strip()))
    majors = []
    for m in majors_raw:
        majors += re.split(r"[,/&]+", m)
    majors = [x.strip(" .-").lower() for x in majors if x.strip()] or [""]
    # ===== 4. Sinh nhiều dict (cartesian product) =====
    for edu in edu_levels:
        for major in majors:
            records.append({
                "education_level": edu,
                "field_of_study": major,
                "mandatory": True if (edu.strip() or major.strip()) else False })
    return records
