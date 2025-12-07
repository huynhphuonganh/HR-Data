import os
import asyncpg
from dotenv import load_dotenv
from contextlib import asynccontextmanager

# Load biến môi trường
load_dotenv()

# Lấy thông tin kết nối TỪ ENVIRONMENT VARIABLES
DB_USER = os.getenv("DB_USER", "Admin")
DB_PASSWORD = os.getenv("DB_PASS", "Grouphr-smartcv-2025")
DB_NAME = os.getenv("DB_NAME", "HRSmartCV")
DB_HOST = os.getenv("DB_HOST", "136.114.248.105")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

if not DB_PASSWORD:
    raise ValueError("❌ DB_PASSWORD not set in environment variables")

_pool = None

async def init_connection_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            host=DB_HOST,
            port=DB_PORT,
            min_size=1,
            max_size=10,
            command_timeout=60
        )
        print(f"✅ Connected to database: {DB_NAME} at {DB_HOST}")
    return _pool

@asynccontextmanager
async def get_connection():
    pool = await init_connection_pool()
    conn = await pool.acquire()
    try:
        yield conn
    finally:
        await pool.release(conn)

async def close_connection_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        print("✅ Database connection pool closed")
