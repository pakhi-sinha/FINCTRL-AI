import pytest_asyncio
import pytest
from httpx import AsyncClient
from api.main import app
from database.database import get_db, engine, Base
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
import uuid
from datetime import datetime, timezone

TestSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def override_get_db():
    async with TestSessionLocal() as session:
        yield session

app.dependency_overrides[get_db] = override_get_db

@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.mark.asyncio
async def test_health_check():
    from httpx import ASGITransport
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

@pytest.mark.asyncio
async def test_ingest_erp():
    from httpx import ASGITransport
    erp_data = [{
        "id": str(uuid.uuid4()),
        "reference_id": "API_REF",
        "amount": 5000,
        "currency": "INR",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": "sale",
        "status": "completed"
    }]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/api/ingest/erp", json=erp_data)
    assert response.status_code == 200
    assert response.json()["inserted_count"] == 1

@pytest.mark.asyncio
async def test_duplicate_ingest():
    from httpx import ASGITransport
    erp_data = [{
        "id": str(uuid.uuid4()),
        "reference_id": "API_REF",
        "amount": 5000,
        "currency": "INR",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": "sale",
        "status": "completed"
    }]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        await ac.post("/api/ingest/erp", json=erp_data)
        response2 = await ac.post("/api/ingest/erp", json=erp_data) # Duplicate
    assert response2.status_code == 200
    assert response2.json()["inserted_count"] == 0 # 0 inserted the second time
