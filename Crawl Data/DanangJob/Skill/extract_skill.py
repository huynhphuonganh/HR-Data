import re
from unidecode import unidecode
import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
def extract_job_skill_name(soup):
    skills = []
    mota = soup.select_one("#ctl00_ContentPlaceHolder1_lblmota")
    if not mota:
        return skills
    keywords = [
        "sử dụng thành thạo", "sử dụng", "thành thạo", "kỹ năng", "có khả năng", "biết", "thạo",
        "am hiểu", "nắm vững", "quen thuộc", "thực hành", "ứng dụng",
        "kinh nghiệm với", "trải nghiệm với", "làm việc với", "thao tác với",
        "hiểu biết về", "có kiến thức", "có chứng chỉ", "khác:", "kiến thức cơ bản về",
        "tiếng", "kỹ năng chuyên môn"]
    keywords_sorted = sorted(keywords, key=len, reverse=True)
    stopwords_head = {"các", "những", "một số", "về"}
    lines = []
    for tag in mota.find_all(["p", "li", "div", "span"]):
        text = tag.get_text(" ", strip=True)
        if text:
            lines.extend([line.strip() for line in re.split(r"[\n]", text) if line.strip()])
    # fallback: nếu chưa có line nào, lấy toàn bộ text trong mota
    if not lines:
        text = mota.get_text(" ", strip=True)
        lines = [line.strip() for line in re.split(r"[\n]", text) if line.strip()]
    # Tìm đoạn bắt đầu từ "yêu cầu" hoặc "kỹ năng"
    start_found = False
    for line in lines:
        low = line.lower()
        if not start_found:
            if "yêu cầu" in low or "kỹ năng" in low:
                start_found = True
            else:
                continue  
        if (line.startswith("◆") and "yêu cầu" not in low) or ("quyền lợi" in low and "yêu cầu" not in low):
            break
        sentences = re.split(r"[.]", line) if "." in line else [line]
        
        for sent in sentences:
            sent = sent.strip()
            if not sent:
                continue
            low_sent = sent.lower()
            for kw in keywords_sorted:
                if kw in low_sent:
                    idx = low_sent.find(kw)
                    skill_part = sent[idx:].strip(" :,-.·•\t\n") if kw == "tiếng" else sent[idx+len(kw):].strip(" :,-.·•\t\n")
                    skill_part = re.sub(r"[()…]", "", skill_part)
                    if skill_part:
                        for s in re.split(r"[;,/·•\n]", skill_part):
                            tokens = s.strip(" \t\n:,-.·•").split()
                            if not tokens:
                                continue
                            tokens = [t for t in tokens if t.lower() not in keywords]
                            while tokens and tokens[0].lower() in stopwords_head:
                                tokens = tokens[1:]
                            if tokens:
                                skills.append(" ".join(tokens))
                    break 
    return skills

CATEGORY_KEYWORDS = {
    "languages": [
        "tiếng",
        "toeic", "ielts", "toefl",
        "jlpt", "n1", "n2", "n3", "n4", "n5",
        "hsk", "topik"
    ],
    "interpersonal": [
        "giao tiếp", "đàm phán", "làm việc nhóm", "thuyết trình",
        "giải quyết vấn đề", "tư duy phản biện", "quản lý thời gian",
        "lãnh đạo", "chủ động", "cẩn thận", "ham học hỏi", "học hỏi nhanh",
        "chịu được áp lực", "chịu áp lực", "sáng tạo", "linh hoạt",
        "truyền đạt", "viết báo cáo", "soạn thảo văn bản", "tổ chức sự kiện",
        "hòa đồng", "kết nối", "tinh thần trách nhiệm"
    ],
    "education_training": [
    "giảng dạy", "sư phạm", "training", "đào tạo", "mentoring", 
    "coaching", "lesson plan", "soạn giáo án"
    ],
    "design_creative": [
    "figma", "sketch", "xd", "ui", "ux", "wireframe", "prototype", 
    "thiết kế giao diện", "thiết kế trải nghiệm", "branding", "thiết kế logo",
    "motion graphic", "after effects", "premiere", "video editing"
    ],
    "sales_customer_service": [
    "kỹ năng bán hàng", "chốt sales", "telesales", "gọi điện thoại", 
    "tư vấn khách hàng", "đàm phán hợp đồng", "dịch vụ khách hàng", 
    "customer service", "crm", "giữ chân khách hàng", "xử lý khiếu nại"
    ],
    "project_management": [
    "quản lý dự án", "project management", "pmp", "agile", "scrum", 
    "kanban", "waterfall", "quản lý tiến độ", "lập kế hoạch", "jira", 
    "trello", "asana", "ms project", "stakeholder", "quản lý nguồn lực"
    ],

    "tools_and_technologies": [

    "excel", "word", "powerpoint", "outlook", "tin học văn phòng",
    "microsoft office", "microsoft", "canva",

    # Thiết kế
    "photoshop", "pts", "ai", "illustrator", "corel", "autocad",
    "cad", "solidworks", "3ds max", "sketchup",

    # Lập trình / CNTT
    "python", "java", "c#", "c++", "javascript", "typescript", "php",
    "html", "css", "sql", "mysql", "postgresql", "mongodb", "nosql",
    "react", "vue", "angular", "nodejs", "spring", "dotnet", ".net",
    "api", "rest api", "graphql",

    # DevOps & Cloud
    "git", "github", "gitlab", "bitbucket",
    "docker", "kubernetes", "jenkins", "ci/cd",
    "aws", "azure", "gcp", "google cloud", "cloud computing",
    "linux", "unix", "windows server",

    # Data & AI
    "pandas", "numpy", "scikit-learn", "tensorflow", "keras",
    "pytorch", "machine learning", "deep learning", "ai",
    "spark", "hadoop", "big data",
    "sql server", "oracle database", "data warehouse", "etl",

    # Data visualization & BI
    "power bi", "tableau", "matplotlib", "seaborn",
    "ggplot", "data studio", "looker",
    "excel pivot", "dashboard",

    # ERP/CRM/HRM
    "erp", "sap", "oracle", "crm", "hrm", "misa", "fast accounting",

    # QA/QC / ISO
    "iso", "5s", "kaizen", "lean", "six sigma",

    # Website / hệ thống
    "wordpress", "shopify", "magento", "cms", "website", "hệ thống", "phần mềm"
],
    "industry_knowledge": [
        # Kinh tế / quản trị
        "kế toán", "kiểm toán", "tài chính", "ngân hàng",
        "marketing", "quản trị", "kinh doanh", "bán hàng",
        "thương mại", "thương mại điện tử", "nhân sự", "tuyển dụng",
        # Chuỗi cung ứng
        "logistics", "xuất nhập khẩu", "kho vận",
        # Luật / hành chính
        "luật", "pháp lý", "hành chính",
        # Kỹ thuật / sản xuất
        "cơ khí", "kỹ thuật", "công nghệ thông tin", "it",
        "sản xuất", "chế tạo", "quy trình sản xuất",
        # Dịch vụ
        "chăm sóc khách hàng", "bảo hiểm", "y tế", "giáo dục"
    ]
    
}

def categorize_skill(name: str) -> str:
    n = name.lower()

    for category, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in n:
                return category
    return "other"

FIELD_MAPPING = {
    "Kế toán - Kiểm toán - Thu mua - Kho": "Accounting/Finance",
    "Tài chính - Ngân hàng - Chứng khoán": "Accounting/Finance",
    "Sale - Marketing - Event": "Marketing/Sales",
    "Nhân viên bán hàng": "Marketing/Sales",
    "Luật - Pháp lý": "Legal",
    "Hành chính - Nhân sự - Thư ký": "HR/Admin",
    "Y tế - Dược": "Healthcare/Pharma",
    "Giáo dục - Đào tạo": "Education/Training",
    "Ngoại ngữ - Biên phiên dịch": "Education/Training",
    "Công nghệ thông tin": "IT/Software",
    "Điện - Điện tử - Điện lạnh": "Engineering/Technical",
    "Cơ khí - Ô tô": "Engineering/Technical",
    "Kỹ thuật - Bảo trì": "Engineering/Technical",
    "Xây dựng - Kiến trúc - Nội thất": "Construction/Architecture",
    "Thiết kế - Mỹ thuật": "Design/Creative",
    "Nhà hàng - Khách sạn - Du lịch": "Hospitality/Tourism",
    "Ẩm thực - Phục vụ - Thu ngân - Bar": "Hospitality/Tourism",
    "Bếp - Phụ bếp - Tạp vụ - Rửa bát": "Hospitality/Tourism",
    "Lễ tân - Đặt phòng - Tổng đài - Guest relation": "Hospitality/Tourism",
    "Buồng phòng - Giặt là - Làm vườn": "Hospitality/Tourism",
    "Bảo vệ - Vệ sĩ - An ninh": "Security",
    "Tài xế - Lái xe - Giao nhận": "Logistics/Driver",
    "Bất động sản - Bảo hiểm": "Real Estate/Insurance",
    "Giám đốc - Quản lý điều hành": "Management/Executive",
    "Tư vấn - CSKH - Telesales": "Customer Service/Sales",
    "Khác": "Other"
}
def get_field_by_category(category_text: str) -> str:
    for k, v in FIELD_MAPPING.items():
        if category_text.strip().lower() in k.lower():
            return v
    return "Other"
CATEGORY_TO_TYPE = {
    # Hard skills
    "languages": "hard_skill",
    "design_creative": "hard_skill",
    "tools_and_technologies": "hard_skill",
    "industry_knowledge": "hard_skill",
    # Soft skills
    "interpersonal": "soft_skill",
    "education_training": "soft_skill",
    "project_management": "soft_skill",
    "sales_customer_service": "soft_skill",
    # Others
    "other": "others"
}
def infer_type_from_category(category: str) -> str:
    return CATEGORY_TO_TYPE.get(category, "others")

