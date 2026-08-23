import pytest
import pytest_asyncio
import os
import asyncio
from finctrl.backend.database.database import init_db

@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    # Set SQLite if not provided, just for tests
    os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

@pytest_asyncio.fixture(autouse=True)
async def db_setup():
    await init_db()
    yield
