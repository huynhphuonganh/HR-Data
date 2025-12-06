import re
from datetime import datetime
from bs4 import BeautifulSoup


def extract_job_title(soup):
    title_tag = soup.find("span", class_="text")
    if title_tag:
        return title_tag.get_text(strip=True)
    return None

def extract_job_expires_at(soup):
    divs = soup.find_all("div", class_="col-xs-3 offset10")
    for div in divs:
        text = div.get_text(" ", strip=True)
        if "Hạn nộp:" in text:
            match = re.search(r"Hạn nộp:\s*(.*?)\s*(?:\||$)", text)
            if match:
                return match.group(1).strip()
    return None


def extract_job_salary(soup):
    divs = soup.find_all("div", class_="col-xs-3 offset10")
    for div in divs:
        text = div.get_text(" ", strip=True)

        # Tách các phần theo dấu |
        parts = [p.strip() for p in text.split("|")]

        for part in parts:
            if "mức lương" in part.lower():
                salary_text = part.replace("Mức lương:", "").strip().lower()

                # Trường hợp thỏa thuận
                if "thỏa thuận" in salary_text.lower():
                    return None, None 
                
                # Trường hợp "trên X triệu"
                match_above = re.search(r"trên\s*(\d+)", salary_text, re.IGNORECASE)
                if match_above:
                    min_salary = int(match_above.group(1)) * 1_000_000
                    return float(min_salary), None
                
                # Trường hợp "X - Y triệu"
                match_range = re.findall(r"\d+", salary_text)
                if len(match_range) == 2:
                    min_salary, max_salary = map(int, match_range)
                    min_salary *= 1_000_000
                    max_salary *= 1_000_000
                    return float(min_salary), float(max_salary)
                
                # fallback: chỉ có 1 giá trị
                if len(match_range) == 1:
                    val = int(match_range[0]) * 1_000_000
                    return float(val), float(val)
                
                return None, None

    return None, None



def extract_job_status(soup):
    expires_at = extract_job_expires_at(soup)
    if not expires_at:
        return "open"
    
    try:
        expire_date = datetime.strptime(expires_at, "%d/%m/%Y")
        if expire_date >= datetime.now():
            return "open"
        else:
            return "closed"
    except ValueError:
        return "open"
    
def extract_job_address(soup):
    sidebar = soup.find("div", class_="sidebar-blk-cnt hideexp")
    if not sidebar:
        return None

    for p in sidebar.find_all("p"):
        b_tag = p.find("b")
        if b_tag and "địa chỉ" in b_tag.get_text(strip=True).lower():
            # lấy toàn bộ text trong <p>, bỏ phần "Địa chỉ:"
            full_text = p.get_text(" ", strip=True)
            address = full_text.replace(b_tag.get_text(strip=True), "").strip(" :-")
            return address

    return None

def extract_job_description(soup):
    row_div = soup.find("div", class_="row")
    if not row_div:
        return None

    for p in row_div.find_all("p"):
        strong = p.find("strong")
        if strong:
            strong_text = strong.get_text(strip=True).lower()
            if "mô tả công việc" in strong_text:
                # Lấy toàn bộ text trong <p>, loại bỏ phần <strong>
                strong.extract()
                return p.get_text(" ", strip=True)

    return None

def extract_job_responsibilities(soup):
    row_div = soup.find("div", class_="row")
    if not row_div:
        return None

    for p in row_div.find_all("p"):
        strong = p.find("strong")
        if strong:
            strong_text = strong.get_text(strip=True).lower()
            if "vai trò" in strong_text or "công việc chính" in strong_text:
                # Lấy toàn bộ text trong <p>, loại bỏ phần <strong>
                strong.extract()
                return p.get_text(" ", strip=True)

    return None


def extract_job_benefits(soup):
    row_div = soup.find("div", class_="row")
    if not row_div:
        return None

    keywords = ["chế độ", "đãi ngộ", "phúc lợi", "quyền lợi"]
    
    for p in row_div.find_all("p"):
        strong = p.find("strong")
        if strong:
            strong_text = strong.get_text(strip=True).lower()
            if any(kw in strong_text for kw in keywords):
                # Loại bỏ phần <strong> để chỉ lấy nội dung mô tả
                strong.extract()
                return p.get_text(" ", strip=True)

    return None


def extract_job_working_days(soup):
    row_div = soup.find("div", class_="row")
    if not row_div:
        return "all_week"

    # chuyển <br> thành newline để dễ tách dòng
    text_all = row_div.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text_all.splitlines() if ln.strip()]

    for line in lines:
        # tìm keyword
        if re.search(r"thời gian làm việc|thời gian", line, flags=re.IGNORECASE):
            # bỏ keyword khỏi line
            cleaned = re.sub(r"thời gian làm việc|thời gian", "", line, flags=re.IGNORECASE).strip(" :-–—")
            if not cleaned:
                return "all_week"

            txt = cleaned.lower()
            txt = txt.replace("thứ", "t")
            # chuẩn hóa các dấu gạch
            txt = txt.replace("–", "-").replace("—", "-").replace(" ", "")
            
            if re.search(r"cả.*tuần", txt) \
               or re.search(r"t2\s*[-–—>]\s*cn", txt) \
               or "t2-chunhat" in txt \
               or re.search(r"cn.*(cũng.*làm|làm)", txt):
                return "all_week"
            
            if re.search(r"t2.*t7", txt) \
               or re.search(r"nghỉ.*cn", txt) \
               or re.search(r"t2.*sáng.*t7", txt) \
               or re.search(r"nghỉ.*chiều.*t7", txt):
                return "T2-T7"
            
            if re.search(r"t2.*t6", txt) \
               or re.search(r"nghỉ.*t7", txt):
                return "T2-T6"
            
            return "all_week"
    return "all_week"


    
    
def extract_job_employment_type(soup):
    row_div = soup.find("div", class_="row")
    if not row_div:
        return "full-time"  # mặc định

    for p in row_div.find_all("p"):
        text = p.get_text(" ", strip=True).lower()

        # chỉ xét nếu trong text có gợi ý liên quan hình thức làm việc
        if "hình thức" in text or "loại hình công việc" in text or "employment" in text or "giờ làm việc" in text or "thời gian làm việc" in text:
            if "toàn thời gian" in text or "full-time" in text:
                return "full-time"
            elif "bán thời gian" in text or "part-time" in text or "xoay ca" in text:
                return "part-time"
            elif "thực tập" in text or "intern" in text:
                return "intern"
            elif "hợp đồng" in text or "contract" in text:
                return "contract"
            elif "freelance" in text:
                return "freelance"

    # fallback mặc định
    return "full-time"


def extract_job_work_mode(soup):
    row_div = soup.find("div", class_="row")

    def detect_mode(text: str):
        text = text.lower()
        if "hybrid" in text or "offline/online" in text or "online/offline" in text:
            return "hybrid"
        elif "remote" in text or "online" in text or "làm việc từ xa" in text:
            return "remote"
        elif "onsite" in text or "offline" in text or "làm việc tại" in text:
            return "onsite"
        return None

    # 1. Quét qua nội dung chính trong <div class="row">
    if row_div:
        paras = row_div.find_all("p")
        for p in paras[:-1]:   # duyệt tất cả trừ p cuối
            text = p.get_text(" ", strip=True)
            mode = detect_mode(text)
            if mode:
                return mode

    # 2. Nếu không có thì fallback sang title
    title_tag = soup.find("span", class_="text")
    if title_tag:
        mode = detect_mode(title_tag.get_text(" ", strip=True))
        if mode:
            return mode

    # 3. Nếu vẫn không có thì mặc định
    return "onsite"



def extract_job_years_experience(soup):
    if not soup:
        return "No requirement"

    row_div = soup.find("div", class_="row")
    if not row_div:
        return "No requirement"

    # tìm đoạn p có strong chứa 'yêu cầu'
    target_p = None
    for p in row_div.find_all("p"):
        strong = p.find("strong")
        if strong and "yêu cầu" in strong.get_text(strip=True).lower():
            target_p = p
            break

    if not target_p:
        return "No requirement"

    # tách theo <br>
    raw_html = str(target_p)
    lines = re.split(r"<br\s*/?>", raw_html, flags=re.IGNORECASE)

    for line in lines:
        text = BeautifulSoup(line, "html.parser").get_text(" ", strip=True).lower()
        if "kinh nghiệm" in text:
            # Trường hợp tháng
            month_match = re.search(r"(\d+)\s*tháng", text)
            if month_match:
                months = int(month_match.group(1))
                if months < 12:
                    return "Less than 1 year"

            # Trường hợp năm
            year_match = re.search(r"(\d+)\s*năm", text)
            if year_match:
                years = int(year_match.group(1))
                if years <= 0:
                    return "Less than 1 year"
                elif years == 1:
                    return "1 year"
                elif years == 2:
                    return "2 years"
                elif years == 3:
                    return "3 years"
                elif years == 4:
                    return "4 years"
                elif years == 5:
                    return "5 years"
                else:
                    return "More than 5 years"

            # Không có số nhưng có "không yêu cầu"
            if "không yêu cầu" in text or "no requirement" in text:
                return "No requirement"

    return "No requirement"