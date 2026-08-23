import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from database.models import Base

# Default to SQLite for easy local setup and tests if Postgres URL is not provided.
# Actually, the user asked for Postgres: "Build the production-quality backend foundation... 1. PostgreSQL database"
# Let's use PostgreSQL locally.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/finctrl")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

async def init_db():
    async with engine.begin() as conn:
        # Caution: in a real production environment we would use Alembic.
        await conn.run_sync(Base.metadata.create_all)
