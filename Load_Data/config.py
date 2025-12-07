import os
import asyncpg
from dotenv import load_dotenv
from contextlib import asynccontextmanager

load_dotenv()

DB_USER = "Admin"
DB_PASSWORD = "Grouphr-smartcv-2025"
DB_NAME = "HRSmartCV"
DB_HOST = "10.29.48.3" 
DB_PORT = 5432

_pool = None

async def init_connection_pool():
    global _pool
    if _pool is None:
        print(f"🔌 Connecting to {DB_HOST} (Private IP)...")
        _pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            host=DB_HOST,
            port=DB_PORT,
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
