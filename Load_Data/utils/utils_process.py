from datetime import datetime, date, timedelta
from typing import Optional, Tuple, List, Dict, Any
import unicodedata
import re
import json
import os

class UtilsProcess:
    def __init__(self, base_path: Optional[str] = None):
        if base_path is None:
            base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

        self.base_path = base_path

    # ==================== FILE LOADING ====================
    @staticmethod
    def load_json_file(file_path: str) -> Dict[str, Any]:
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Error loading {file_path}: {e}")
            return {}

    def load_process_file(self, source_name: str, filename: str) -> Dict[str, Any]:
        """
        Load file từ thư mục Data/<source_name>/Processed/<filename>
        """
        file_path = os.path.join(self.base_path, "Data", source_name, "Processed", filename)
        return self.load_json_file(file_path)

    def load_all_process_files(self, source: str) -> Dict[str, Dict[str, Any]]:
        """
        Load tất cả file trong Data/<source>/Processed/
        """
        process_path = os.path.join(self.base_path, "Data", source, "Processed")
        print(f"📂 Loading files from {process_path}")
        
        json_data = {}
        for filename in os.listdir(process_path):
            if filename.endswith(".json"):
                key = filename.replace(".json", "")
                full_path = os.path.join(process_path, filename)
                json_data[key] = self.load_json_file(full_path)
                print(f"   ✅ Loaded {filename}")
        return json_data

    @staticmethod
    def parse_date(date_str: Optional[str]) -> Optional[date]:
        """
        Parse date string to datetime.date object
        Supports formats: YYYY-MM-DD, YYYY-MM, YYYY
        """
        if not date_str or date_str.strip() == "":
            return None
        
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError as e:
            print(f"Warning: Could not parse date '{date_str}': {e}")
            return None
            
    @staticmethod
    def parse_date_job(date_str):
        """
        Parse date string to date object
        """
        if not date_str:
            return None
        try:
            # Try different date formats
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]:
                try:
                    parsed = datetime.strptime(date_str, fmt)
                    return parsed.date() if fmt == "%Y-%m-%d %H:%M:%S" else parsed
                except ValueError:
                    continue
            return None
        except Exception:
            return None