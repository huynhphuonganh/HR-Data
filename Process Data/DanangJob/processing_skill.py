import json
import re
import pandas as pd

INPUT_FILE = "../Data/raw/skills.json"
OUTPUT_FILE = "../Data/process/skill_clean.json"

def classify_skill(skill_name):
    text = skill_name.lower()
    if any(kw in text for kw in ["có khả năng", "chịu", "tinh thần", "trong công việc", "tốt", "về các", "chuẩn mực", "-", "&"]):
        return "customize"
    if len(text.split()) <= 3:
        return "built_in"
    return "customize"

def preprocess_skill():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)["data"]
    processed = []
    for i, item in enumerate(raw, start=1):
        name = item.get("skill_name", "").strip()
        category = item.get("category", "").strip()
        field = item.get("field", "").strip()
        skill_type = classify_skill(name)
        processed.append({
            "skillID": i,
            "name": name,
            "category": category,
            "field": field,
            "skill_type": skill_type,
            "description": None})
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({"data": processed}, f, ensure_ascii=False, indent=2)
    print(f" Done. Total {len(processed)} skills processed and saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    preprocess_skill()
