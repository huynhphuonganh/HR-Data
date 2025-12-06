import json
import os
import sys
from typing import Dict, List, Optional
import asyncpg

# Import config để kết nối database
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import get_connection, close_connection_pool
from utils.utils_process import UtilsProcess

class LoadCompanyData:
    def __init__(self):
        """
        Khởi tạo LoadCompanyData
        """
        # Set base path to project root
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.utils_process = UtilsProcess(base_path)

    async def load_companies(self) -> List[Dict[str, str]]:
        """
        Load companies data vào table Company
        """
        try:
            data_sources = ["Danang43", "DanangJob"]   # <== THÊM DÒNG NÀY
        
            processed_companies = []

            for source in data_sources:
                # Read processed data
                data = self.utils_process.load_process_file(source, "company_processed.json")
                
                companies_data = data.get("companies", [])
                
                for company in companies_data:
                    created = company.get("created_at") or company.get("crawled_at")
                    updated = company.get("updated_at") or company.get("crawled_at")

                    processed_companies.append({
                        "name": company.get("name"),
                        "website": company.get("website"),
                        "industry": company.get("industry"),
                        "size_range": company.get("size_range"),
                        "line1": company.get("line1"),
                        "line2": company.get("line2"),
                        "line3": company.get("line3"),
                        "line4": company.get("line4"),
                        "address": company.get("address"),
                        "created_at": self.utils_process.parse_date_job(created),
                        "updated_at": self.utils_process.parse_date_job(updated),
                    })
                print(f"Loaded {len(companies_data)} companies from source: {source}")
                
            print(f"Loaded {len(processed_companies)} companies from JSON file.")
            return processed_companies

        except Exception as e:
            print(f"Error loading companies: {e}")
            return []

    async def insert_company(self, companies: List[Dict[str, str]]) -> None:
        """
        Insert company data into database
        """
        if not companies:
            print("No valid companies to insert.")
            return
            
        async with get_connection() as conn:
            stats = {"loaded": 0, "duplicates": 0, "skipped": 0}
            
            for idx, company in enumerate(companies, start=1):
                try:
                    # Kiểm tra duplicate
                    existing_id = await conn.fetchval(
                        'SELECT "companyID" FROM "Company" WHERE "name" = $1',
                        company["name"]
                    )
                    
                    if existing_id:
                        stats["duplicates"] += 1
                        continue
                    
                    # Insert company
                    await conn.execute(
                        '''
                        INSERT INTO "Company" 
                        ("name", "website", "industry", "size_range", "line1", "line2", "line3", "line4", "address", "created_at", "updated_at")
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ''',
                        company["name"],
                        company["website"],
                        company["industry"],
                        company["size_range"],
                        company["line1"],
                        company["line2"],
                        company["line3"],
                        company["line4"],
                        company["address"],
                        company["created_at"],
                        company["updated_at"]
                    )
                    stats["loaded"] += 1
                    
                    # Progress indicator
                    if idx % 1000 == 0:
                        print(f"Processed {idx}/{len(companies)} companies...")
                        
                except asyncpg.UniqueViolationError:
                    stats["duplicates"] += 1
                except Exception as e:
                    print(f"Error inserting company '{company['name']}': {e}")
                    stats["skipped"] += 1
            
            # Summary
            self._print_summary(stats, len(companies))

    def _print_summary(self, stats: Dict[str, int], total: int) -> None:
        """In summary kết quả insert"""
        print("\n===== Insert Summary =====")
        print(f"Inserted: {stats['loaded']}")
        print(f"Duplicates: {stats['duplicates']}")
        print(f"Skipped (error): {stats['skipped']}")
        print(f"Total processed: {sum(stats.values())}/{total}")


async def main():
    """Main function để chạy quá trình load companies"""
    processor = LoadCompanyData()
    
    try:
        companies = await processor.load_companies()
        await processor.insert_company(companies)
        print("Successfully completed companies loading!")
    except Exception as e:
        print(f"Failed to load companies: {e}")
    finally:
        await close_connection_pool()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
