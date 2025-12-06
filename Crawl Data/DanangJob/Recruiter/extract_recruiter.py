import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin
HEADERS = {"User-Agent": "Mozilla/5.0"}
def is_valid_value(val: str) -> bool:
    if not val:
        return False
    return bool(re.search(r"[A-Za-zÀ-ỹ0-9]", val))
def get_contact_info(job_url):
    res = requests.get(job_url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(res.text, "html.parser")
    contact_data = {
        "email": None,
        "phone": None,
        "name": None,
        "job_description": None
    }
    for block in soup.select(".block-info-company"):
        content = block.select_one(".block-content")
        if not content:
            continue

        rows = content.find_all("tr")
        for row in rows:
            cols = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
            if len(cols) >= 2:
                label, value = cols[0].lower(), cols[1].strip()

                if "email" in label and not contact_data["email"]:
                    contact_data["email"] = value
                elif ("liên hệ" in label or "người liên hệ" in label) and not contact_data["name"]:
                    contact_data["name"] = value
                elif ("điện thoại" in label or "phone" in label) and not contact_data["phone"]:
                    contact_data["phone"] = value
    need_fallback = False
    for key in ["email", "phone", "name"]:
        if not is_valid_value(contact_data[key]):
            contact_data[key] = None  
            need_fallback = True
    if need_fallback:
        clearfix_blocks = soup.select(".clearfix")
        for clearfix_block in clearfix_blocks:
            block_text = clearfix_block.get_text(" ", strip=True)
            if "mô tả công việc" in block_text.lower():
                text = block_text
                # EMAIL
                if not contact_data["email"]:
                    match_email = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
                    if match_email:
                        contact_data["email"] = match_email.group(0)
                # PHONE
                if not contact_data["phone"]:
                    match_phone = re.search(r"(\+84|0)\d{8,11}", text)
                    if match_phone:
                        contact_data["phone"] = match_phone.group(0)
                # NAME
                if not contact_data["name"]:
                    match_name = re.search(r"(Ms|Mr|Mrs|Anh|Chị)\.?\s+[A-ZÀ-Ỹ][\wÀ-ỹ]+", text)
                    if match_name:
                        contact_data["name"] = match_name.group(0)
                    else:
                        match_caps = re.search(r"\b([A-ZÀ-Ỹ][A-ZÀ-Ỹ]+(?:\s+[A-ZÀ-Ỹ][A-ZÀ-Ỹ]+){1,2})\b", text)
                        if match_caps:
                            contact_data["name"] = match_caps.group(0).title()
                if all([contact_data["email"], contact_data["phone"], contact_data["name"]]):
                    break
    return contact_data
def extract_role(email: str | None):
    if not email:
        return None
    return "Recruiter"


