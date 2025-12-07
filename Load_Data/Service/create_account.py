import warnings
import asyncpg
import os
import sys
import bcrypt

# Import config để kết nối database
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import get_connection, close_connection_pool

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)

class CreateAccount:
    def __init__(self):
        pass

    def _hash_password(self, password: str) -> str:
        """
        Hash password using bcrypt trực tiếp
        """
        try:
            password_bytes = password.encode('utf-8')
            salt = bcrypt.gensalt(rounds=12)
            hashed = bcrypt.hashpw(password_bytes, salt)
            return hashed.decode('utf-8')
        except Exception as e:
            print(f"❌ Error hashing password: {e}")
            raise
    
    async def check_existing_users(self, conn, user_type: str) -> int:
        """Kiểm tra số lượng user hiện có theo loại"""
        try:
            if user_type == 'candidate':
                count = await conn.fetchval(
                    'SELECT COUNT(*) FROM "Users" WHERE user_type = \'candidate\' AND account LIKE \'candidate%\''
                )
            elif user_type == 'recruiter':
                count = await conn.fetchval(
                    'SELECT COUNT(*) FROM "Users" WHERE user_type = \'hr\' AND account LIKE \'recruiter%\''
                )
            else:
                return 0
            return count or 0
        except Exception as e:
            print(f"Lỗi khi kiểm tra {user_type} hiện có: {e}")
            return 0

    async def create_fake_candidates(self, conn, start_index: int = 1, count: int = 2000) -> None:
        """Tạo fake candidate accounts"""
        print(f"🔐 Đang hash password bằng bcrypt...")
        hashed_password = self._hash_password("123456")
        print(f"✅ Hash hoàn tất: {hashed_password[:29]}...")

        print(f"👥 Đang tạo {count} candidate từ candidate{start_index} đến candidate{start_index + count - 1}...")

        batch_size = 100
        for i in range(start_index, start_index + count):
            account = f"candidate{i}@gmail.com"
            await conn.execute("""
                INSERT INTO "Users" (account, password, first_name, last_name, user_type, created_at)
                VALUES ($1, $2, $3, $4, 'candidate', NOW())
            """, account, hashed_password, "Candidate", str(i))

            if (i - start_index + 1) % batch_size == 0:
                print(f"Đã thêm batch {batch_size} candidate (đến candidate{i})")

        print(f"✅ Hoàn thành tạo {count} candidate!")

    async def create_fake_recruiters(self, conn, start_index: int = 1, count: int = 2000) -> None:
        """Tạo fake recruiter accounts"""
        print(f"🔐 Đang hash password bằng bcrypt...")
        hashed_password = self._hash_password("123456")
        print(f"✅ Hash hoàn tất: {hashed_password[:29]}...")

        print(f"👥 Đang tạo {count} recruiter từ recruiter{start_index} đến recruiter{start_index + count - 1}...")

        batch_size = 100
        for i in range(start_index, start_index + count):
            account = f"recruiter{i}@gmail.com"
            await conn.execute("""
                INSERT INTO "Users" (account, password, first_name, last_name, user_type, created_at)
                VALUES ($1, $2, $3, $4, 'hr', NOW())
            """, account, hashed_password, "Recruiter", str(i))

            if (i - start_index + 1) % batch_size == 0:
                print(f"Đã thêm batch {batch_size} recruiter (đến recruiter{i})")

        print(f"✅ Hoàn thành tạo {count} recruiter!")

    async def create_candidate_accounts(self) -> None:
        """Tạo accounts cho candidates"""
        print("Bắt đầu quá trình tạo 2000 user candidate...")
        try:
            async with get_connection() as conn:
                existing_count = await self.check_existing_users(conn, 'candidate')
                print(f"Hiện có {existing_count} user candidate với pattern 'candidate*'")

                if existing_count >= 2000:
                    print("✅ Đã có đủ 2000 candidate. Bỏ qua bước này.")
                else:
                    remaining = 2000 - existing_count
                    start_index = existing_count + 1
                    print(f"Sẽ tạo {remaining} candidate còn lại từ candidate{start_index}")
                    await self.create_fake_candidates(conn, start_index=start_index, count=remaining)

                final_count = await self.check_existing_users(conn, 'candidate')
                print(f"Tổng cộng hiện có {final_count} user candidate trong database")

        except Exception as e:
            print(f"Lỗi trong quá trình thực thi: {e}")
            raise

    async def create_recruiter_accounts(self) -> None:
        """Tạo accounts cho recruiters"""
        print("Bắt đầu quá trình tạo 2000 user recruiter...")
        try:
            async with get_connection() as conn:
                existing_count = await self.check_existing_users(conn, 'recruiter')
                print(f"Hiện có {existing_count} user recruiter với pattern 'recruiter*'")

                if existing_count >= 2000:
                    print("✅ Đã có đủ 2000 recruiter. Bỏ qua bước này.")
                else:
                    remaining = 2000 - existing_count
                    start_index = existing_count + 1
                    print(f"Sẽ tạo {remaining} recruiter còn lại từ recruiter{start_index}")
                    await self.create_fake_recruiters(conn, start_index=start_index, count=remaining)

                final_count = await self.check_existing_users(conn, 'recruiter')
                print(f"Tổng cộng hiện có {final_count} user recruiter trong database")

        except Exception as e:
            print(f"Lỗi trong quá trình thực thi: {e}")
            raise

    async def create_all_accounts(self) -> None:
        """Tạo tất cả accounts"""
        print("Bắt đầu quá trình tạo tất cả accounts...")
        try:
            await self.create_candidate_accounts()
            print("\n" + "=" * 50 + "\n")
            await self.create_recruiter_accounts()
            print("\n✅ Hoàn thành tạo tất cả accounts!")
        except Exception as e:
            print(f"Lỗi trong quá trình thực thi: {e}")
            raise
        finally:
            await close_connection_pool()
            print("🔒 Đã đóng kết nối database")


async def main():
    """Main function để chạy quá trình tạo accounts"""
    processor = CreateAccount()
    
    try:
        # Tạo tất cả accounts
        await processor.create_all_accounts()
        print("Successfully completed account creation!")
    except Exception as e:
        print(f"Failed to create accounts: {e}")
    finally:
        await close_connection_pool()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
