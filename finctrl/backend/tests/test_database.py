import pytest
import asyncio
from sqlalchemy import text
from finctrl.backend.database.database import get_db_session, init_db, engine
from finctrl.backend.database.models import ERPRecordModel

@pytest.mark.asyncio
async def test_db_initialization():
    await init_db()

    async for session in get_db_session():
        # Simple test to verify connection and table exists
        result = await session.execute(text("SELECT COUNT(*) FROM erp_records"))
        count = result.scalar()
        assert count == 0
        break
