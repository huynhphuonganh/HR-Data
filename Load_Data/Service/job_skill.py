import json
import os
import sys
from typing import Dict, List, Optional
import asyncpg

# Import config để kết nối DB
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import get_connection, close_connection_pool
from utils.utils_process import UtilsProcess


class LoadJobSkill:
    def __init__(self):
        """
        Khởi tạo LoadJobSkill
        """
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.utils_process = UtilsProcess(base_path)

    async def load_job_skills(self, data_source: str) -> List[Dict[str, str]]:
        """
        Load job_skill data từ file processed JSON
        """
        try:
            data = self.utils_process.load_process_file(data_source, "job_skill_processed.json")

            job_skills_data = data.get("jobSkills", [])
            processed_skills = []

            for skill in job_skills_data:
                processed_skills.append({
                    "title": skill.get("title"),
                    "skill_name": skill.get("skill_name"),
                    "mandatory": skill.get("mandatory", True),
                    "min_proficiency": skill.get("min_proficiency"),
                    "min_years_experience": skill.get("min_years_experience"),
                    "notes": skill.get("notes")
                })
                
            print(f"Loaded {len(job_skills_data)} job skills from {data_source}.")
            return processed_skills

        except Exception as e:
            print(f"Error loading job skills: {e}")
            return []

    async def insert_job_skills(self, job_skills: List[Dict[str, str]]):
        """
        Insert or update JobSkill into database
        """
        if not job_skills:
            print("No job skills to insert.")
            return

        async with get_connection() as conn:
            stats = {"loaded": 0, "updated": 0, "skipped": 0}

            for idx, item in enumerate(job_skills, start=1):
                try:
                    title = item.get("title")
                    skill_name = item.get("skill_name")

                    if not title or not skill_name:
                        stats["skipped"] += 1
                        continue

                    # 🔹 Lấy jobID từ title
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
                        print(f"❌ Job not found for title: {title}")
                        stats["skipped"] += 1
                        continue

                    # 🔹 Lấy skillID từ skill_name
                    skill_id = await conn.fetchval(
                        '''
                        SELECT "skillID" 
                        FROM "Skill" 
                        WHERE LOWER(TRIM("name")) = LOWER(TRIM($1))
                        LIMIT 1
                        ''',
                        skill_name
                    )

                    if not skill_id:
                        print(f"❌ Skill not found: {skill_name}")
                        stats["skipped"] += 1
                        continue

                    # 🔹 Kiểm tra skill đã tồn tại cho job này chưa
                    existing_id = await conn.fetchval(
                        '''
                        SELECT "jobSkillID"
                        FROM "JobSkill"
                        WHERE "jobID" = $1 AND "skillID" = $2
                        ''',
                        job_id,
                        skill_id
                    )

                    if existing_id:
                        # Update nếu tồn tại
                        await conn.execute(
                            '''
                            UPDATE "JobSkill"
                            SET "mandatory" = $1,
                                "min_proficiency" = $2,
                                "min_years_experience" = $3,
                                "notes" = $4
                            WHERE "jobSkillID" = $5
                            ''',
                            item.get("mandatory", True),
                            item.get("min_proficiency"),
                            item.get("min_years_experience"),
                            item.get("notes"),
                            existing_id
                        )
                        stats["updated"] += 1

                    else:
                        # Insert mới
                        await conn.execute(
                            '''
                            INSERT INTO "JobSkill"
                            ("jobID", "skillID", "mandatory", "min_proficiency", 
                             "min_years_experience", "notes")
                            VALUES ($1,$2,$3,$4,$5,$6)
                            ''',
                            job_id,
                            skill_id,
                            item.get("mandatory", True),
                            item.get("min_proficiency"),
                            item.get("min_years_experience"),
                            item.get("notes")
                        )
                        stats["loaded"] += 1

                    if idx % 500 == 0:
                        print(f"Processed {idx}/{len(job_skills)}...")

                except Exception as e:
                    print(f"Error inserting job skill for job '{item.get('title')}': {e}")
                    stats["skipped"] += 1

            # Summary
            self._print_summary(stats, len(job_skills))

    def _print_summary(self, stats: Dict[str, int], total: int):
        print("\n===== JobSkill Insert Summary =====")
        print(f"Inserted: {stats['loaded']}")
        print(f"Updated: {stats['updated']}")
        print(f"Skipped: {stats['skipped']}")
        print(f"Total processed: {sum(stats.values())}/{total}")


async def main():
    loader = LoadJobSkill()
    try:
        data_sources = ["Danang43", "DanangJob"]

        for src in data_sources:
            job_skills = await loader.load_job_skills(src)
            await loader.insert_job_skills(job_skills)

    except Exception as e:
        print(f"Failed to load job skills: {e}")
    finally:
        await close_connection_pool()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
