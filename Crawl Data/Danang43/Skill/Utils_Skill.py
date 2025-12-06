import re
from datetime import datetime
from bs4 import BeautifulSoup

def extract_job_skill_name(soup):
    skills = []
    
    row_div = soup.find("div", class_="row")
    if not row_div:
        return skills
    
    for p in row_div.find_all("p"):
        strong = p.find("strong")
        if strong and "yêu cầu" in strong.get_text(strip=True).lower():
            raw_html = str(p)
            parts = re.split(r"<br\s*/?>", raw_html, flags=re.IGNORECASE)
            lines = [BeautifulSoup(part, "html.parser").get_text(" ", strip=True) for part in parts]
            lines = [re.sub(r"^[\-\–\+\*\•\s]+", "", line).strip() for line in lines if line.strip()]
            
            filtered_lines = []
            for line in lines:
                low = line.lower()
                if "tốt nghiệp" in low:
                    continue
                if re.search(r"kinh nghiệm\s+\d+\s*năm", low):
                    continue
                filtered_lines.append(line)
                
            keywords = [
                "sử dụng", "thành thạo", "kỹ năng", "có khả năng", "biết", "thạo",
                "am hiểu", "nắm vững", "quen thuộc", "thực hành", "ứng dụng",
                "kinh nghiệm với", "trải nghiệm với", "làm việc với", "thao tác với",
                "hiểu biết về", "có kiến thức", "có chứng chỉ", "khác:",
                "sử dụng thành thạo", "tiếng", "kiến thức cơ bản về"
            ]
            for line in filtered_lines:
                low_line = line.lower()
                for kw in keywords:
                    if kw in low_line:
                        idx = low_line.find(kw)
                        if kw == "tiếng":
                            # giữ từ "tiếng" + phần sau
                            skill_text = line[idx:].strip(" :,-.")
                        else:
                            # lấy toàn bộ dòng (không cần tách nhỏ)
                            skill_text = line.strip(" :,-.")
                        if skill_text:
                            skills.append(skill_text)
                        break  # dừng ở keyword đầu tiên để tránh trùng lặp
    return skills



import re

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
        "hòa đồng", "kết nối", "tinh thần trách nhiệm", "độc lập"
    ],
    "tools_and_technologies": [
        # Văn phòng / cơ bản
    "excel", "word", "powerpoint", "outlook", "tin học văn phòng", "vi tính",
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
        "thương mại", "thương mại điện tử", "nhân sự", "tuyển dụng","TMĐT"
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

def extract_skill_field(soup):
    field_tag = soup.find("span", attrs={"style": "display: block;"})
    if not field_tag:
        return None
    
    a_tag = field_tag.find("a")
    if a_tag:
        return a_tag.get_text(strip=True)
    return None