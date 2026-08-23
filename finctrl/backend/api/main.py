from fastapi import FastAPI
from contextlib import asynccontextmanager
from finctrl.backend.database.database import init_db, close_db
from finctrl.backend.api.routes import router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Depending on how the environment handles this,
    # we might choose not to auto-init in production without migrations.
    # But for Phase 2 test environments, this is safe.
    await init_db()
    yield
    await close_db()

app = FastAPI(
    title="FINCTRL AI - Phase 2 API",
    lifespan=lifespan
)

app.include_router(router)
