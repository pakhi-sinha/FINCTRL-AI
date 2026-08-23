import pytest_asyncio
import pytest
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from database.models import Base, ERPRecordModel
import uuid
from datetime import datetime, timezone

# Setup in-memory sqlite for tests
engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.mark.asyncio
async def test_erp_model_persistence():
    async with AsyncSessionLocal() as db:
        erp = ERPRecordModel(
            id=uuid.uuid4(),
            reference_id="TEST_REF",
            amount=10000,
            currency="INR",
            timestamp=datetime.now(timezone.utc),
            type="sale",
            status="completed"
        )
        db.add(erp)
        await db.commit()

        from sqlalchemy import select
        result = await db.execute(select(ERPRecordModel).where(ERPRecordModel.reference_id == "TEST_REF"))
        fetched = result.scalar_one_or_none()
        assert fetched is not None
        assert fetched.amount == 10000
