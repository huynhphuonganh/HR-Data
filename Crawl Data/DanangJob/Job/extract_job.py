import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse
import re
def get_job_info(soup):
    job_data = {
        "title": None,
        "company_name": None,
        "department": None,
        "employment_type": None,
        "work_mode": "onsite",           # mặc định
        "working_days": None,
        "address": None,
        "salary_min": None,
        "salary_max": None,
        "currency": "VND",               # mặc định
        "description": None,
        "responsibilities": None,
        "benefits": None,
        "year_experience": None,
        "status": "open",                # mặc định
        "posted_at": None,
        "expires_at": None,
    }
    # ===== 1. Tiêu đề công việc =====
    title_tag = soup.select_one("#ctl00_ContentPlaceHolder1_tieude")
    if title_tag:
        raw_title = title_tag.get_text(strip=True)
        # Tách theo dấu "-" hoặc "/"
        first_job = re.split(r"\s*[-/]\s*", raw_title)[0]
        job_data["title"] = first_job.strip()
        t = raw_title.lower()
        if "part time" in t or "bán thời gian" in t:
            job_data["employment_type"] = "Part-time"
        elif "full time" in t or "toàn thời gian" in t:
            job_data["employment_type"] = "Full-time"
    # ===== 2. Tên công ty =====
    company_tag = soup.select_one("#ctl00_ContentPlaceHolder1_lblcongty")
    if company_tag:
        job_data["company_name"] = company_tag.get_text(strip=True)
    # ===== 3. Địa chỉ =====
    addr_tag = soup.select_one("#ctl00_ContentPlaceHolder1_diachicongty")
    if addr_tag:
        job_data["address"] = addr_tag.get_text(strip=True)
    # ===== 4. Ngày hết hạn =====
    extra_info = soup.select_one("#ctl00_ContentPlaceHolder1_lblhan")
    if extra_info:
        extra_text = extra_info.get_text(" ", strip=True)
        m = re.search(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{4})\b", extra_text)
        if m:
            job_data["expires_at"] = m.group(1)
#---------------- Xử lý các thông tin qua text---------------------------------#
    # ===== 5. Description =====
    desc_box = soup.select_one("#ctl00_ContentPlaceHolder1_lblmota")
    def clean_num(x):
        return int(re.sub(r"[^\d]", "", x))
    if desc_box:
        text = desc_box.get_text("\n", strip=True).lower()
    # ===== Keyword lists =====
        desc_start_kw = r"(mô tả công việc|description|job description|mô tả|nội dung công việc|vị trí|thông tin công việc|chi tiết công việc|job detail|job info)"
        desc_end_kw   = r"(yêu cầu|quyền lợi|benefit|phúc lợi|chế độ|trách nhiệm|responsibility|liên hệ|contact)"
        benefit_start_kw = r"(quyền lợi|benefits|phúc lợi|chế độ|đãi ngộ|lợi ích|quyền được hưởng)"
        benefit_end_kw   = r"(địa điểm làm việc|liên hệ|contact|hình thức làm việc|nộp hồ sơ|cách thức ứng tuyển|hồ sơ bao gồm|vui lòng ghi rõ|cách ứng tuyển|deadline|cách gửi cv|phương thức ứng tuyển|thời hạn nộp hồ sơ)"
        working_start_kw = r"(thời gian làm việc|giờ làm việc|lịch làm việc|ca làm việc|làm việc theo ca|ca sáng|ca chiều|ca tối|working time|work schedule)"
        working_end_kw   = r"(quyền lợi|benefits|phúc lợi|lương|mức lương|contact|liên hệ|nộp hồ sơ|cách thức ứng tuyển|deadline)"
        salary_unit_kw   = r"(triệu|tr|tr\.|million|m|vnd|vnđ|đ|đồng|usd|\$)"
        # ===== Description =====
        m = re.search(fr"(?:^|\n|\r|[\*\-\s])(?:{desc_start_kw})(.*?)(?={desc_end_kw}|$)", text, flags=re.I | re.S)
        job_data["description"] = m.group(2).strip() if m else ""
        # ===== Benefits =====
        m = re.search(fr"{benefit_start_kw}(.*?)(?={benefit_end_kw}|$)", text, flags=re.I|re.S)
        job_data["benefits"] = m.group(2).strip() if m else ""
        # ===== Salary =====
        def parse_salary(text):
            def to_int(x):
                return int(re.sub(r"[^\d]", "", x))
            # Lọc đoạn chứa từ khóa lương
            salary_kw = r"[ *_-]*(lương|mức lương|thu nhập|salary|offer|wage|đãi ngộ)"
            salary_lines = [line for line in text.splitlines() if re.search(salary_kw, line, flags=re.I)]
            salary_text = "\n".join(salary_lines)
            if not salary_text:
                return "Thỏa thuận", "Thỏa thuận"
            # range with unit (đơn vị ngay sau số thứ 2 – giữ nguyên)
            m = re.search(
                r"(\d[\d\.,]*)\s*[-–tođến=>→⇒➔➡]+\s*(\d[\d\.,]*)\s*(triệu|tr|million|m|vnd|vnđ|đ|usd|\$)",
                salary_text, flags=re.I)
            if m:
                unit = m.group(3).lower()
                mult = 1_000_000 if any(k in unit for k in ["triệu","tr","million","m"]) else 1
                return to_int(m.group(1))*mult, to_int(m.group(2))*mult
            m = re.search(
                r"(\d[\d\.,]*)\s*(triệu|tr|million|m|vnd|vnđ|đ|usd|\$)?\s*[-–tođến]+\s*(\d[\d\.,]*)\s*(triệu|tr|million|m|vnd|vnđ|đ|usd|\$)?",
                salary_text, flags=re.I)
            if m:
                # ưu tiên đơn vị bên phải, nếu không có thì lấy bên trái
                unit = (m.group(4) or m.group(2) or "").lower()
                mult = 1_000_000 if any(k in unit for k in ["triệu","tr","million","m"]) else 1
                return to_int(m.group(1))*mult, to_int(m.group(3))*mult
            m = re.search(
                r"(\d[\d\.,]*)\s*(triệu|tr|million|m|vnd|vnđ|đ|usd|\$)",
                salary_text, flags=re.I)
            if m:
                unit = m.group(2).lower()
                mult = 1_000_000 if any(k in unit for k in ["triệu","tr","million","m"]) else 1
                v = to_int(m.group(1))*mult
                return v, v
            # raw >= 7 digits (VND)
            m = re.search(r"(\d[\d\.,]{6,})", salary_text)
            if m:
                v = to_int(m.group(1))
                return v, v
            return "Thỏa thuận", "Thỏa thuận"
        job_data["salary_min"], job_data["salary_max"] = parse_salary(text)
        # ====== Experience =====
        exp = "Không yêu cầu"
        if m := re.search(r"(\d+)\s*[-–]\s*(\d+)\s*năm.*kinh nghiệm", text):
            exp = f"{m.group(1)}-{m.group(2)}"
        elif m := re.search(r"tối thiểu\s*(\d+)\s*năm.*kinh nghiệm", text):
            exp = m.group(1)
        elif m := re.search(r"(\d+)\s*năm.*kinh nghiệm", text):
            exp = m.group(1)
        elif "không yêu cầu kinh nghiệm" in text:
            exp = "Không yêu cầu"
        job_data["year_experience"] = exp
        # ===== Working days =====
        def parse_working_days(text: str) -> str | None:
            m = re.search(r"(Thứ\s*[2-7CN]|T[2-7]|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)", text, re.I)
            if m:
                return text[m.start():m.start()+20].strip()
            if re.search(r"(Sáng|Chiều|\d{1,2}[:h]\d{0,2})", text, re.I):
                return "T2 - CN"
            return None
    return job_data

