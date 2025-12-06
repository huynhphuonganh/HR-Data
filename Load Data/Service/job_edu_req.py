import json
import os
import sys
from typing import Dict, List
import asyncpg

# Import config để kết nối database
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import get_connection, close_connection_pool
from utils.utils_process import UtilsProcess

class LoadJobEducationReqData:
    def __init__(self):
        """
        Khởi tạo LoadJobEducationReqData
        """
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.utils_process = UtilsProcess(base_path)

    async def load_job_education_reqs(self, data_source: str) -> List[Dict[str, str]]:
        """
        Load job education req từ JSON của từng nguồn
        """
        try:
            data = self.utils_process.load_process_file(data_source, "job_education_req_processed.json")
            education_reqs_data = data.get("jobEducationReqs", [])

            processed_reqs = []

            for req in education_reqs_data:
                processed_reqs.append({
                    "title": req.get("title"),
                    "education_level": req.get("education_level"),
                    "field_of_study": req.get("field_of_study"),
                    "mandatory": req.get("mandatory", False)
                })

            print(f"[{data_source}] Loaded {len(processed_reqs)} education reqs.")
            return processed_reqs

        except Exception as e:
            print(f"Error loading job education reqs from {data_source}: {e}")
            return []

    async def insert_job_education_reqs(self, education_reqs: List[Dict[str, str]]) -> None:
        """
        Insert dữ liệu vào bảng JobEducationReq
        """
        if not education_reqs:
            print("No valid job education reqs to insert.")
            return

        async with get_connection() as conn:
            stats = {"loaded": 0, "duplicates": 0, "skipped": 0}

            for idx, req in enumerate(education_reqs, start=1):
                try:
                    title = req["title"]

                    # Tìm jobID dựa vào title (không phân biệt hoa thường)
                    job_id = await conn.fetchval(
                        '''
                        SELECT "jobID" 
                        FROM "Job" 
                        WHERE LOWER(TRIM("title")) = LOWER(TRIM($1))
                        LIMIT 1
                        ''',
                        title
                    )

                    if not job_id:
                        print(f"Job not found for title: '{title}' → SKIPPED")
                        stats["skipped"] += 1
                        continue

                    # Kiểm tra duplicate (jobID + education_level + field_of_study)
                    existing_req_id = await conn.fetchval(
                        '''
                        SELECT "jobEducationReqID"
                        FROM "JobEducationReq"
                        WHERE "jobID" = $1
                        AND "education_level" = $2
                        AND LOWER(TRIM("field_of_study")) = LOWER(TRIM($3))
                        ''',
                        job_id,
                        req["education_level"],
                        req["field_of_study"]
                    )

                    if existing_req_id:
                        stats["duplicates"] += 1
                        continue

                    # Insert data mới
                    await conn.execute(
                        '''
                        INSERT INTO "JobEducationReq"
                        ("jobID", "education_level", "field_of_study", "mandatory")
                        VALUES ($1, $2, $3, $4)
                        ''',
                        job_id,
                        req["education_level"],
                        req["field_of_study"],
                        req["mandatory"]
                    )

                    stats["loaded"] += 1

                    if idx % 1000 == 0:
                        print(f"Processed {idx}/{len(education_reqs)} education reqs...")

                except Exception as e:
                    print(f"Error inserting job education req (title={req['title']}): {e}")
                    stats["skipped"] += 1

            self._print_summary(stats, len(education_reqs))

    def _print_summary(self, stats: Dict[str, int], total: int):
        print("\n===== Insert Summary =====")
        print(f"Inserted: {stats['loaded']}")
        print(f"Duplicates: {stats['duplicates']}")
        print(f"Skipped: {stats['skipped']}")
        print(f"Total processed: {sum(stats.values())}/{total}")


async def main():
    processor = LoadJobEducationReqData()

    try:
        data_sources = ["Danang43", "DanangJob"]

        combined_data = []

        for src in data_sources:
            reqs = await processor.load_job_education_reqs(src)
            combined_data.extend(reqs)

        await processor.insert_job_education_reqs(combined_data)

        print("Successfully completed JobEducationReq loading!")

    except Exception as e:
        print(f"Failed to load job education reqs: {e}")

    finally:
        await close_connection_pool()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
