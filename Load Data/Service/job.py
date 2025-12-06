import json
import os
import sys
from typing import Dict, List, Optional
import asyncpg

# Import config để kết nối database
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import get_connection, close_connection_pool
from utils.utils_process import UtilsProcess

def fix_employment_type(value: str) -> str:
    if not value:
        return None

    v = value.strip().lower()

    mapping = {
        "full-time": "full_time",
        "part-time": "part_time",
        "contract": "contract",
        "intern": "intern",
        "freelance": "freelance",
    }

    return mapping.get(v, None)

class LoadJobData:
    def __init__(self):
        """
        Khởi tạo LoadJobData
        """
        # Set base path to project root
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.utils_process = UtilsProcess(base_path)

    async def load_jobs(self, data_source: str) -> List[Dict[str, str]]:
        """
        Load jobs data vào table "Job"
        """
        try:
            # Read processed data
            job_data = self.utils_process.load_process_file(data_source, "job_processed.json")

            jobs_data = job_data.get("jobs", [])

            processed_jobs = []
            for job in jobs_data:
                raw_currency = job.get("currency")

                # Normalize currency
                if raw_currency:
                    normalized = raw_currency.lower().strip()
                    if normalized in ["vnđ", "vnd"]:
                        currency = "VND"
                    else:
                        currency = raw_currency  # giữ nguyên giá trị gốc
                else:
                    currency = None

                processed_jobs.append({
                    "company_name": job.get("company_name"),
                    "title": job.get("title"),
                    "department": job.get("department"),
                    "employment_type": fix_employment_type(job.get("employment_type")),
                    "work_mode": job.get("work_mode"),
                    "working_days": job.get("working_days"),
                    "line1": job.get("line1"),
                    "line2": job.get("line2"),
                    "line3": job.get("line3"),
                    "line4": job.get("line4"),
                    "address": job.get("address"),
                    "salary_min": job.get("salary_min"),
                    "salary_max": job.get("salary_max"),
                    "currency": currency,
                    "description": job.get("description"),
                    "responsibilities": job.get("responsibilities"),
                    "benefits": job.get("benefits"),
                    "year_experience": job.get("year_experience"),
                    "status": job.get("status"),
                    "posted_at": self.utils_process.parse_date_job(job.get("posted_at")),
                    "expires_at": self.utils_process.parse_date_job(job.get("expires_at")),
                })

            print(f"Loaded {len(processed_jobs)} jobs from JSON file.")
            return processed_jobs

        except Exception as e:
            print(f"Error loading jobs: {e}")
            return []

    async def insert_job(self, jobs: List[Dict[str, str]]) -> None:
        """
        Insert job data into database
        """
        if not jobs:
            print("No valid jobs to insert.")
            return

        async with get_connection() as conn:
            stats = {"loaded": 0, "updated": 0, "skipped": 0}

            for idx, job in enumerate(jobs, start=1):
                try:
                    # Lấy companyID từ bảng "Company" (so sánh không phân biệt hoa thường)
                    company_id = await conn.fetchval(
                        'SELECT "companyID" FROM "Company" WHERE LOWER(TRIM("name")) = LOWER(TRIM($1))',
                        job["company_name"]
                    )

                    if not company_id:
                        print(f"No companyID found for job '{job.get('title', 'Unknown')}' - company: '{job['company_name']}'")
                        stats["skipped"] += 1
                        continue

                    # Kiểm tra job đã tồn tại chưa (dựa vào title + companyID)
                    existing_job_id = await conn.fetchval(
                        'SELECT "jobID" FROM "Job" WHERE "title" = $1 AND "companyID" = $2',
                        job["title"],
                        company_id
                    )

                    if existing_job_id:
                        # Update job đã tồn tại
                        await conn.execute(
                            '''
                            UPDATE "Job" SET "careerID" = $1, "department" = $2, "employment_type" = $3, 
                            "work_mode" = $4, "working_days" = $5, "line1" = $6, "line2" = $7, 
                            "line3" = $8, "line4" = $9, "address" = $10, "salary_min" = $11, 
                            "salary_max" = $12, "currency" = $13, "description" = $14, 
                            "responsibilities" = $15, "benefits" = $16, "year_experience" = $17, 
                            "status" = $18, "posted_at" = $19, "expires_at" = $20 WHERE "jobID" = $21
                            ''',
                            None,  # careerID
                            job["department"],
                            job["employment_type"],
                            job["work_mode"],
                            job["working_days"],
                            job["line1"],
                            job["line2"],
                            job["line3"],
                            job["line4"],
                            job["address"],
                            job["salary_min"],
                            job["salary_max"],
                            job["currency"],
                            job["description"],
                            job["responsibilities"],
                            job["benefits"],
                            job["year_experience"],
                            job["status"],
                            job["posted_at"],
                            job["expires_at"],
                            existing_job_id
                        )
                        stats["updated"] += 1
                    else:
                        # Insert job mới
                        await conn.execute(
                            '''
                            INSERT INTO "Job" ("companyID", "careerID", "title", "department", "employment_type", 
                            "work_mode", "working_days", "line1", "line2", "line3", "line4", "address", 
                            "salary_min", "salary_max", "currency", "description", "responsibilities", 
                            "benefits", "year_experience", "status", "posted_at", "expires_at")
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
                            ''',
                            company_id,
                            None,  # careerID
                            job["title"],
                            job["department"],
                            job["employment_type"],
                            job["work_mode"],
                            job["working_days"],  
                            job["line1"],
                            job["line2"],
                            job["line3"],
                            job["line4"],
                            job["address"],
                            job["salary_min"],
                            job["salary_max"],
                            job["currency"],
                            job["description"],
                            job["responsibilities"],
                            job["benefits"],
                            job["year_experience"],
                            job["status"],
                            job["posted_at"],
                            job["expires_at"],
                        )
                        stats["loaded"] += 1

                    # Progress indicator
                    if idx % 1000 == 0:
                        print(f"Processed {idx}/{len(jobs)} jobs...")

                except Exception as e:
                    print(f"Error inserting job '{job['title']}': {e}")
                    stats["skipped"] += 1

            # Summary
            self._print_summary(stats, len(jobs))

    def _print_summary(self, stats: Dict[str, int], total: int) -> None:
        """In summary kết quả insert"""
        print("\n===== Insert Summary =====")
        print(f"Inserted: {stats.get('loaded', 0)}")
        print(f"Updated: {stats.get('updated', 0)}")
        print(f"Skipped: {stats.get('skipped', 0)}")
        print(f"Total processed: {sum(stats.values())}/{total}")

async def main():
    """Main function để chạy quá trình load jobs"""
    processor = LoadJobData()

    try:
        data_sources = ["Danang43", "DanangJob"]

        for src in data_sources:
            jobs = await processor.load_jobs(src)
            await processor.insert_job(jobs)

        print("Successfully completed jobs loading!")

    except Exception as e:
        print(f"Failed to load jobs: {e}")
    finally:
        await close_connection_pool()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
