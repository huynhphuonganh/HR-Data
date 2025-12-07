import os
import asyncpg
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from google.cloud.sql.connector import Connector

# Load biến môi trường
load_dotenv()

# Lấy các biến môi trường cần thiết
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
INSTANCE_CONNECTION_NAME = os.getenv("CLOUD_SQL_CONNECTION_NAME")
# DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL_CLOUD")

if not all([DB_USER, DB_PASS, DB_NAME, INSTANCE_CONNECTION_NAME]):
    raise ValueError("❌ Các biến DB_USER, DB_PASS, DB_NAME, CLOUD_SQL_CONNECTION_NAME chưa được set.")

_pool = None

async def init_connection_pool():
    """
    Khởi tạo connection pool nếu chưa có
    """
   global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            min_size=1, 
            max_size=10,
            host="127.0.0.1",
            loop=asyncio.get_event_loop(),
            socket_factory=lambda: connector.connect(
                INSTANCE_CONNECTION_NAME,
                "asyncpg", # Chỉ định driver
            )
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
