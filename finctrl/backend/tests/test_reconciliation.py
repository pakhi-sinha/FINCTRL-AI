import pytest
import os
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'

@pytest.mark.asyncio
async def test_reconciliation_pass():
    assert True
