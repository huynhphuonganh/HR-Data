import re
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
def get_company_info(soup, job_url=None):
    company_data = {
        "name": None,
        "website": None,
        "industry": None,
        "size_range": None,
        "line1": None,
        "line2": None,
        "line3": None,
        "line4": None,
        "address": None,
        "hq_city_by": None,
        "hq_ward_by": None,
        "hq_country_by": None,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "crawled_at": datetime.now().isoformat(),
        "link_jobs": []}
    # ===== 1. Lấy tên và địa chỉ công ty =====
    name_tag = soup.select_one("#ctl00_ContentPlaceHolder1_lblcongty")
    if name_tag:
        company_data["name"] = name_tag.get_text(strip=True)
    addr_tag = soup.select_one("#ctl00_ContentPlaceHolder1_diachicongty")
    if addr_tag:
        company_data["address"] = addr_tag.get_text(strip=True)
    # ===== 2. Fallback sang clearfix (Mô tả công việc) =====
    for clearfix_block in soup.select(".clearfix"):
        text = clearfix_block.get_text(" ", strip=True).lower()
        if "mô tả công việc" in text:
            current_website = company_data.get("website")
            if not current_website or not current_website.startswith("http") or any(
                x in current_website for x in ["facebook.com", "linkedin.com", "vieclam", "topcv", "jobstreet", "masothue"]):
                m = re.search(r"(https?://[^\s]+|www\.[^\s]+)", text)
                if m:
                    company_data["website"] = m.group(0).strip()
            company_data["industry"] = None
            company_data["size_range"] = None
            break
    # ===== 3. Gắn link job hiện tại vào danh sách (nếu có) =====
    if job_url:
        company_data["link_jobs"].append(job_url)
    return company_data
