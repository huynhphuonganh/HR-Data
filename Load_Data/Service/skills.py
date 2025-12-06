import json
import os
import sys
from typing import Dict, List, Optional
from datetime import datetime, date
import asyncpg

# Import config để kết nối database
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import get_connection, close_connection_pool
from utils.utils_process import UtilsProcess


class LoadSkillsData:
    def __init__(self):
        """
        Khởi tạo UtilsProcess
        """
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.utils_process = UtilsProcess(base_path)

    async def load_skills_unique(self) -> List[Dict[str, Optional[str]]]:
        """
        Load kỹ năng từ các nguồn JSON đã xử lý và trả về danh sách kỹ năng duy nhất
        """
        try:
            processed_skills = []

            sources = [
                ("Danang43", "type"),          # file có trường "type"
                ("DanangJob", "skill_type"),   # file có trường "skill_type"
            ]
            
            for folder, skill_key in sources:
                try:
                    # Load file JSON
                    data = self.utils_process.load_process_file(folder, "skill_processed.json")

                    # lấy phần "skills"
                    skill_list = data.get("skills", [])

                    print(f"{folder} → Loaded {len(skill_list)} skills")

                    # Chuẩn hóa & thêm vào list tổng
                    for skill in skill_list:
                        processed_skills.append({
                            "name": skill.get("name"),
                            "category": skill.get("category", "other"),
                            "field": "other",
                            "skill_type": "build_in",  
                            "description": None
                        })

                except Exception as e:
                    print(f"Error loading source {folder}: {e}")

            # Tổng hợp
            print(f"Total processed skills: {len(processed_skills)}")

            return processed_skills

        except Exception as e:
            print("Error while loading skills:", e)
            raise e

    async def insert_skill_unique(self, skills: List[Dict[str, Optional[str]]]):
        """
        Nhận danh sách kỹ năng (list of dict) và insert vào DB
        """
        if not skills:
            print("No valid skills to insert.")
            return

        async with get_connection() as conn:
            loaded_count = 0
            duplicate_count = 0
            skipped_count = 0

            for idx, skill in enumerate(skills, start=1):
                try:
                    # Kiểm tra trùng name
                    existing_skill = await conn.fetchval(
                        'SELECT "skillID" FROM "Skill" WHERE "name" = $1',
                        skill["name"],
                    )

                    if existing_skill:
                        duplicate_count += 1
                        print(f"Duplicate found → Name: {skill['name']}")
                        continue

                    # Insert
                    await conn.execute(
                        '''
                        INSERT INTO "Skill" ("name", "category", "field", "skill_type", "description")
                        VALUES ($1, $2, $3, $4, $5)
                        ''',
                        skill["name"],
                        skill["category"],
                        skill["field"],
                        skill["skill_type"],
                        skill["description"],
                    )
                    loaded_count += 1

                    # Hiển thị tiến trình mỗi 1000 dòng
                    if idx % 1000 == 0:
                        print(f"Inserted {loaded_count}/{len(skills)} skills...")

                except asyncpg.UniqueViolationError:
                    duplicate_count += 1
                    print(f"Duplicate found → Name: {skill['name']}")
                except Exception as e:
                    print(f"Error inserting skill {skill['name']}: {e}")
                    skipped_count += 1

            print("\n===== Insert Summary =====")
            print(f"Inserted: {loaded_count}")
            print(f"Duplicates: {duplicate_count}")
            print(f"Skipped (error): {skipped_count}")
            print(f"Total processed: {loaded_count + duplicate_count + skipped_count}/{len(skills)}")


async def main():
    """Main function to run the skills loading process"""
    process_skills = LoadSkillsData()
    try:
        skills = await process_skills.load_skills_unique()
        await process_skills.insert_skill_unique(skills)
        print("Successfully inserted all skills!")
    except Exception as e:
        print(f"Failed to insert skills: {e}")
    finally:
        await close_connection_pool()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
