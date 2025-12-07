import os
import asyncpg
from dotenv import load_dotenv
from contextlib import asynccontextmanager

load_dotenv()

DB_USER = "Admin"
DB_PASSWORD = "Grouphr-smartcv-2025"
DB_NAME = "HRSmartCV"

# Connection qua Unix socket - Cloud SQL tự động xử lý
INSTANCE_CONNECTION_NAME = "smartcv-data-pipeline:us-central1:grouphrsmartcv"

_pool = None

async def init_connection_pool():
    global _pool
    if _pool is None:
        unix_socket = f"/cloudsql/{INSTANCE_CONNECTION_NAME}"
        print(f"🔌 Connecting via: {unix_socket}")
        
        _pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            host=unix_socket,
            min_size=1,
            max_size=5,
            command_timeout=60
        )
        print(f"✅ Connected to {DB_NAME}")
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
