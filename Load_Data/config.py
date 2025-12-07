import os
import asyncpg
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from google.cloud.sql.connector import Connector

# Load biến môi trường
load_dotenv()


# Lấy thông tin kết nối
DB_USER = "Admin"
DB_PASSWORD = "Grouphr-smartcv-2025"
DB_NAME = "HRSmartCV"
DB_HOST = "136.114.248.105"
DB_PORT = "5432"

if not all([DB_USER, DB_PASSWORD, DB_NAME, DB_HOST]):
    raise ValueError("❌ Missing database connection info in .env")

_pool = None

async def init_connection_pool():
    """
    Khởi tạo connection pool
    """
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            host=DB_HOST,
            port=DB_PORT,
            min_size=1,
            max_size=10
        )
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
