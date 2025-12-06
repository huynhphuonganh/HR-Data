import re
from bs4 import BeautifulSoup


# Hàm trích xuất tên công ty 
def extract_company_name(soup):
    companies = []
    divs = soup.find_all("div", class_="title-employer")
    for div in divs:
        a_tag = div.find("a")
        if not a_tag:
            continue
        name = a_tag.get_text(strip=True)
        companies.append(name)
    return companies




# Hàm trích xuất website 
WEBSITE_REGEX = re.compile(
    r"(https?://[^\s]+|www\.[^\s]+)", re.IGNORECASE
)

BLOCKED_DOMAINS = [
    "forms.gle",
    "docs.google.com",
    "bit.ly",
    "goo.gl",
    "tinyurl.com"
]

BLOCKED_KEYWORDS = [
    "tuyen-dung", "nhan-su", "career", "jobs",
    "recruit", "form", "apply"
]

def extract_company_website(soup):
    code_tag = soup.find("code")
    if not code_tag:
        return None

    text = code_tag.get_text(" ", strip=True)
    match = WEBSITE_REGEX.search(text)
    if not match:
        return None

    website = match.group(0).strip()

    # Thêm http:// nếu thiếu schema
    if not website.startswith(("http://", "https://")):
        website = "http://" + website

    website_lower = website.lower()

    # Kiểm tra domain blacklist
    if any(bad in website_lower for bad in BLOCKED_DOMAINS):
        return None

    # Kiểm tra keyword trong đường dẫn
    if any(kw in website_lower for kw in BLOCKED_KEYWORDS):
        return None

    return website


# Hàm trích xuất địa chỉ và quy mô công ty
def extract_company_contact(soup):
    address, size = None, None

    ps = soup.find_all("p")
    for p in ps:
        b_tag = p.find("b", class_="mr-1")
        if not b_tag:
            continue

        label = b_tag.get_text(strip=True)
        # lấy phần text sau thẻ <b>
        content = b_tag.next_sibling.strip() if b_tag.next_sibling else None

        if "Địa chỉ" in label and content:
            address = content
        elif "Quy mô công ty" in label and content:
            size = content

            numbers = re.findall(r'\d+', size)
            if numbers:
                max_num = max(map(int, numbers))
            else:
                max_num = 0

            # Gán quy mô chuẩn dựa theo giá trị lớn nhất
            if max_num <= 10:
                size = "1-10"
            elif max_num <= 50:
                size = "11-50"
            elif max_num <= 200:
                size = "51-200"
            elif max_num <= 500:
                size = "201-500"
            elif max_num <= 1000:
                size = "501-1000"
            elif max_num <= 5000:
                size = "1001-5000"
            else:
                size = "5000+"

    return address, size
    return address, size
