import os
# Set default API keys and DB URL for tests at import time before other modules load config
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
os.environ["APP_MODE"] = "test"
os.environ["ADMIN_API_KEY"] = "test_admin_key"
os.environ["READ_ONLY_API_KEY"] = "test_readonly_key"
os.environ["RAZORPAY_WEBHOOK_SECRET"] = "test_webhook_secret"

import pytest
import pytest_asyncio
import asyncio

@pytest_asyncio.fixture(autouse=True)
async def db_setup(request):
    # Only run DB setup if the test is actually hitting the database
    # Check if the test is one of the phase 2 DB-reliant modules
    if "test_synthetic_data" not in request.module.__name__:
        from finctrl.backend.database.database import init_db
        await init_db()
    yield
