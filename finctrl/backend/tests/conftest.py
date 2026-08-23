import pytest
import pytest_asyncio
import os
import asyncio

@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    # Set SQLite if not provided, just for tests
    os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

@pytest_asyncio.fixture(autouse=True)
async def db_setup(request):
    # Only run DB setup if the test is actually hitting the database
    # Check if the test is one of the phase 2 DB-reliant modules
    if "test_synthetic_data" not in request.module.__name__:
        from finctrl.backend.database.database import init_db
        await init_db()
    yield
