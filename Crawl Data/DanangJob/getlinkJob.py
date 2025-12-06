import requests
import re
import time
import json 
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
BASE_URL = "https://danangjob.vn/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ===== CẤU HÌNH TỐI ƯU CHO MẠNG KÉM =====
MAX_RETRIES = 3  
RETRY_DELAY = 2  
REQUEST_TIMEOUT = 10  
PAGE_DELAY = 0.1  

def safe_request(url, max_retries=MAX_RETRIES):
    """Request với retry logic cho mạng kém"""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r
            print(f"HTTP {r.status_code}, retry {attempt + 1}/{max_retries}")
        except requests.Timeout:
            print(f"Timeout, retry {attempt + 1}/{max_retries}")
        except requests.RequestException as e:
            print(f"Lỗi: {e}, retry {attempt + 1}/{max_retries}")
        
        if attempt < max_retries - 1:
            time.sleep(RETRY_DELAY)
    
    return None
# Lấy toàn bộ category
def get_categories(): 
    r = safe_request(urljoin(BASE_URL, "/viec-lam-theo-nganh-nghe"))
    if not r:
        print(" Không thể lấy danh sách categories")
        return []
    s = BeautifulSoup(r.text, "html.parser")
    return [
        {"name": a.get_text(strip=True), "url": urljoin(BASE_URL, a["href"])}
        for a in s.select("li.nganhnghe_top a")]
# Lấy tổng số trang của 1 category
def total_pages(cat_url):
    r = safe_request(cat_url)
    if not r:
        return 1
    s = BeautifulSoup(r.text, "html.parser")
    nums = []
    for a in s.select("ul.pagination a"):
        m = re.search(r"[?&]page=(\d+)", a.get("href", ""))
        if m:
            nums.append(int(m.group(1)))
        elif a.text.strip().isdigit():
            nums.append(int(a.text.strip()))
    return max(nums) if nums else 1
# Chuẩn hóa URL tránh trùng
def normalize(url):
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", ""))
# Lấy danh sách link job trong 1 category
def crawl_linkjobs(cat_url, max_pages=999):
    seen, jobs = set(), []
    consecutive_empty_pages = 0
    failed_pages = 0 
    for p in range(1, max_pages + 1):
        url = cat_url if p == 1 else urljoin(cat_url, f"?&page={p}")
        print(f"Trang {p}: {url}")
        
        valid_jobs_in_page = 0
        r = safe_request(url)
        if not r:
            failed_pages += 1
            if failed_pages >= 2: 
                print(f"Dừng: {failed_pages} trang liên tiếp lỗi")
                break
            continue
        
        failed_pages = 0
        
        try:
            s = BeautifulSoup(r.text, "html.parser")
            # Lấy tất cả các slide-item-wrapper
            job_items = s.select(".slide-item-wrapper")
            if not job_items:
                print(f"Trang {p} không có job nào")
                break
            for item in job_items:
                # Tìm link job trong item
                a = item.select_one("a[href*='/viec-lam/']")
                if not a:
                    continue
                job_url = normalize(urljoin(BASE_URL, a.get("href")))
                # Kiểm tra trùng lặp
                if job_url in seen or "viec-lam" not in job_url:
                    continue
                # Lọc theo hạn nộp
                is_valid = False
                span = item.select_one(".info-wrapper .extra-info .salary-level")
                if span:
                    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", span.text)
                    if m:
                        expires_at = datetime.strptime(m.group(1), "%d/%m/%Y")
                        if 2024 <= expires_at.year <= 2025:
                            is_valid = True
                
                if is_valid:
                    seen.add(job_url)
                    jobs.append(job_url)
                    valid_jobs_in_page += 1
            # Kiểm tra điều kiện dừng
            if valid_jobs_in_page == 0:
                consecutive_empty_pages += 1
                print(f"⚠️ Trang {p} không có job hợp lệ ({consecutive_empty_pages}/1)")
                if consecutive_empty_pages >= 1:
                    print(f" Dừng crawl: 1 trang tiếp không có job trong khoảng 2024-2025")
                    break
            else:
                consecutive_empty_pages = 0
                print(f"Trang {p}: Tìm thấy {valid_jobs_in_page} job hợp lệ")    
            time.sleep(PAGE_DELAY)     
        except Exception as e:
            print(f"⚠️ Lỗi khi parse trang {p}: {e}")
            continue  
    return jobs

def crawl_all_categories(max_pages_per_cat=3, delay=1): 
    all_results = []
    categories = get_categories()
    print(f"Tổng số ngành nghề: {len(categories)}")
    for i, cat in enumerate(categories, start=1):
        print(f"\n========== Ngành {i}/{len(categories)}: {cat['name']} ==========")
        try:
            links = crawl_linkjobs(cat["url"], max_pages=max_pages_per_cat)
            print(f" Đã lấy {len(links)} job link cho ngành '{cat['name']}'")
            
            result = {
                "category": cat["name"],
                "url": cat["url"],
                "total_links": len(links),
                "job_links": links
            }
            all_results.append(result)
            # Checkpoint: lưu tạm sau mỗi ngành
            with open("job_links_checkpoint.json", "a", encoding="utf-8") as f:
                f.write(json.dumps(result, ensure_ascii=False) + "\n")
                
        except Exception as e:
            print(f" Lỗi khi crawl ngành '{cat['name']}': {e}")
            continue  
        time.sleep(delay)
    return all_results