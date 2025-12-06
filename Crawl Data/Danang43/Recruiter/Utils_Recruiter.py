import re

EMAIL_REGEX = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
PHONE_REGEX = re.compile(r"(?:\+84|0)[\d\.\-\s]{8,}")
PREFIX_NAME_REGEX = re.compile(
    r"\b(?:Mr\.?|Ms\.?|Mrs\.?|Anh|Chi|Chị|A\.?|C\.?|AC|A\.C\.?|A\s+C|A\.\s*C\.)\s+"
    r"([A-ZÀ-Ỹ][\wÀ-ỹ]+(?:\s+[A-ZÀ-Ỹ][\wÀ-ỹ]+)*)",
    re.IGNORECASE
)
PAREN_PATTERN = re.compile(r"\(([^)]+)\)")

def extract_email(text):
    match = EMAIL_REGEX.search(text)
    return match.group(0) if match else None

def extract_phone(text):
    match = PHONE_REGEX.search(text)
    if match:
        return re.sub(r"[^\d]", "", match.group(0))
    return None

def looks_like_name(candidate: str) -> bool:
    parts = candidate.strip().split()
    if not 1 <= len(parts) <= 4:
        return False
    for p in parts:
        if not re.match(r"^[A-ZÀ-Ỹ][a-zà-ỹ]+$", p):
            return False
    return True

def extract_name(text):
    # --- 0. Bỏ bullet a., b., c. ở đầu dòng ---
    text = re.sub(r"^[a-z]\.\s*", "", text.strip())
    
    # --- 1. Tìm theo tiền tố ---
    match = PREFIX_NAME_REGEX.search(text)
    if match:
        candidate = match.group(1).strip()
        return candidate

    # --- 3. Nếu không có prefix → tìm trong ngoặc ---
    for match in PAREN_PATTERN.findall(text):
        candidate = re.sub(r"[-–,;].*$", "", match.strip())  # bỏ phần thừa
        if looks_like_name(candidate):
            return candidate

    # --- 3. Bộ phận ---
    dept_patterns = [
        (r"nhân\s*sự", "Phòng Nhân sự"),
        (r"tuyển\s*dụng", "Phòng Tuyển dụng"),
        (r"\bhr\b", "Phòng Nhân sự"),
    ]
    text_lower = text.lower()
    for regex, label in dept_patterns:
        if re.search(regex, text_lower):
            return label

    # --- 4.
    # Nếu không có gì ---
    return "Phòng Nhân sự"

