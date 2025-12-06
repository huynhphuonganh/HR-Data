import os
import asyncpg
from dotenv import load_dotenv
from contextlib import asynccontextmanager

# Load biến môi trường
load_dotenv()

DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL_CLOUD")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL (SQLALCHEMY_DATABASE_URL_CLOUD) is not set in .env")

_pool = None

async def init_connection_pool():
    """
    Khởi tạo connection pool nếu chưa có
    """
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return _pool


@asynccontextmanager
async def get_connection():
    """
    Context manager để lấy connection từ pool
    Dùng: async with get_connection() as conn:
    """
    pool = await init_connection_pool()
    conn = await pool.acquire()
    try:
        yield conn
    finally:
        await pool.release(conn)


async def close_connection_pool():
    """
    Đóng connection pool khi kết thúc chương trình
    """
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
