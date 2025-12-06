import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.danang43.vn/viec-lam/page/{}"
FIRST_PAGE = "https://www.danang43.vn/viec-lam"

def get_job_links(page: int):
    """Lấy danh sách link job từ một page"""
    url = FIRST_PAGE if page == 1 else BASE_URL.format(page)
    print(f"Đang crawl page {page}: {url}")

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"Lỗi tải page {page}: {e}")
        return []
    
    soup = BeautifulSoup(resp.content, "html.parser")
    table = soup.find("table", class_="table-content")
    if not table:
        print(f"Page {page} không có dữ liệu")
        return []

    job_links = []
    for row in table.find_all("tr"):
        a_tag = row.find("a", href=True)
        if a_tag:
            link = a_tag["href"]
            if link.startswith("/"):
                link = "https://www.danang43.vn" + link
            job_links.append(link)

    print(f"Page {page}: tìm thấy {len(job_links)} job links")
    return job_links
