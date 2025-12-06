import re
from bs4 import BeautifulSoup

def clean_field(field):
    if not field:
        return None
    cleaned = re.sub(r"[^a-zA-ZÀ-ỹĐđ0-9\s\-]", "", field)
    return cleaned.strip() if cleaned.strip() else None

def extract_job_education_requirements(soup):
    if not soup:
        return []

    row_div = soup.find("div", class_="row")
    if not row_div:
        return []

    target_p = None
    for p in row_div.find_all("p"):
        strong = p.find("strong")
        if strong:
            strong_text = strong.get_text(strip=True).lower()
            if "yêu cầu" in strong_text or "kinh nghiệm" in strong_text:
                target_p = p
                break
            
    if not target_p:
        return []
    
    raw_html = str(target_p)
    parts = re.split(r"<br\s*/?>", raw_html, flags=re.IGNORECASE)
    lines = [BeautifulSoup(part, "html.parser").get_text(" ", strip=True) for part in parts]
    lines = [line.strip() for line in lines if line.strip()]

    results = []
    for text in lines:
        lower = text.lower()
        level = None

        if "trung học" in lower or "cấp 3" in lower or "thpt" in lower:
            level = "high_school"
        elif "cao đẳng" in lower:
            level = "associate"
        elif "đại học" in lower or "cử nhân" in lower:
            level = "bachelor"
        elif "thạc sĩ" in lower or "master" in lower:
            level = "master"
        elif "tiến sĩ" in lower or "phd" in lower or "doctorate" in lower:
            level = "doctorate"

        if not level and ("tốt nghiệp" in lower or "bằng cấp" in lower or "học vấn" in lower or "trình độ" in lower):
            level = "other"

        if not level:
            continue 
        
        # Tìm ngành học
        fields = []
        match = re.search(r"(các\s+ngành|ngành|chuyên ngành|lĩnh vực)[:\s]+(.+)", text, re.IGNORECASE)
        if match:
            raw_fields = match.group(2)
            raw_list = re.split(r",|;|/| hoặc ", raw_fields)
            for f in raw_list:
                f = clean_field(f)
                if not f:
                    continue
                f_lower = f.lower()
                if "liên quan" in f_lower or "tương đương" in f_lower or f.endswith("..."):
                    continue
                fields.append(f)

        if not fields:
            fields = [None]

        # mandatory
        mandatory = True
        if "ưu tiên" in lower or "không bắt buộc" in lower:
            mandatory = False

        for field in fields:
            results.append({
                "education_level": level,
                "field_of_study": field,
                "mandatory": mandatory
            })

    return results
