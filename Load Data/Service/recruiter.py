import json
import os
import sys
from typing import Dict, List, Optional
import asyncpg

# Import config để kết nối database
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import get_connection, close_connection_pool
from utils.utils_process import UtilsProcess

class LoadRecruiterData:
    def __init__(self):
        """
        Khởi tạo LoadRecruiterData
        """
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.utils_process = UtilsProcess(base_path)

    async def load_recruiters(self) -> List[Dict[str, str]]:
        try:
            data_sources = ["Danang43", "DanangJob"]   
        
            processed_recruiters = []
            
            for source in data_sources:
                data = self.utils_process.load_process_file(source, "recruiter_processed.json")
                recruiters_data = data.get("recruiters", [])

                for i, recruiter in enumerate(recruiters_data, start=1):
                    email = recruiter.get("email") or f"hr_{i}@gmail.com"
                    phone = recruiter.get("phone") or "0000000000"

                    processed_recruiters.append({
                        "full_name": recruiter.get("full_name"),
                        "company_name": recruiter.get("company_name"),
                        "email": email,
                        "phone": phone,
                        "role": "recruiter",
                        "photo_url": None,
                        "is_active": True,
                        "created_at": self.utils_process.parse_date_job(recruiter.get("created_at")),
                        "updated_at": self.utils_process.parse_date_job(recruiter.get("updated_at")) 
                    })
            print(f"Loaded {len(processed_recruiters)} recruiters from JSON file.")
            return processed_recruiters

        except Exception as e:
            print(f"Error loading recruiters: {e}")
            return []

    async def insert_recruiter(self, recruiters: List[Dict[str, str]]) -> None:
        if not recruiters:
            print("No valid recruiters to insert.")
            return

        async with get_connection() as conn:
            stats = {"loaded": 0, "duplicates": 0, "skipped": 0}

            for idx, recruiter in enumerate(recruiters, start=1):
                try:
                    # Kiểm tra duplicate
                    existing_id = await conn.fetchval(
                        'SELECT "recruiterID" FROM "Recruiter" WHERE email = $1',
                        recruiter["email"]
                    )
                    if existing_id:
                        stats["duplicates"] += 1
                        continue

                    # Lấy userID chưa có trong bảng Recruiter với user_type là hr
                    user_id = await conn.fetchval("""
                        SELECT "userID"
                        FROM "Users"
                        WHERE user_type = 'hr'
                        AND "userID" NOT IN (SELECT created_by FROM "Recruiter" WHERE created_by IS NOT NULL)
                        ORDER BY "userID"
                        LIMIT 1
                    """)
                    if not user_id:
                        print(f"No userID found for recruiter '{recruiter['full_name']}'")
                        stats["skipped"] += 1
                        continue

                    # Lấy companyID từ bảng Company
                    company_id = await conn.fetchval(
                        'SELECT "companyID" FROM "Company" WHERE name = $1',
                        recruiter["company_name"]
                    )
                    if not company_id:
                        print(f"No companyID found for recruiter '{recruiter['full_name']}'")
                        stats["skipped"] += 1
                        continue

                    # Insert Recruiter
                    await conn.execute(
                        'INSERT INTO "Recruiter" ("companyID", "full_name", "email", "phone", "role", "photo_url", "is_active", "created_by", "created_at", "updated_at") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)',
                        company_id,
                        recruiter["full_name"],
                        recruiter["email"],
                        recruiter["phone"],
                        recruiter["role"],
                        recruiter["photo_url"],
                        recruiter["is_active"],
                        user_id,
                        recruiter["created_at"],
                        recruiter["updated_at"]
                    )
                    stats["loaded"] += 1

                    if idx % 1000 == 0:
                        print(f"Processed {idx}/{len(recruiters)} recruiters...")

                except asyncpg.UniqueViolationError:
                    stats["duplicates"] += 1
                except Exception as e:
                    print(f"Error inserting recruiter '{recruiter['full_name']}': {e}")
                    stats["skipped"] += 1

            # Summary
            self._print_summary(stats, len(recruiters))

    def _print_summary(self, stats: Dict[str, int], total: int) -> None:
        print("\n===== Insert Summary =====")
        print(f"Inserted: {stats['loaded']}")
        print(f"Duplicates: {stats['duplicates']}")
        print(f"Skipped (error): {stats['skipped']}")
        print(f"Total processed: {sum(stats.values())}/{total}")



# async def main():
#     """Main function để chạy quá trình load recruiters"""
#     processor = LoadRecruiterData()
    
#     try:
#         # Load và insert recruiters
#         recruiters = await processor.load_recruiters()
#         await processor.insert_recruiter(recruiters)
        
#         print("Successfully completed recruiters loading!")
#     except Exception as e:
#         print(f"Failed to load recruiters: {e}")
#     finally:
#         await close_connection_pool()


# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(main())