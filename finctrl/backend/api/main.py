from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from contextlib import asynccontextmanager
from finctrl.backend.database.database import init_db, close_db
from finctrl.backend.api.routes import router
from finctrl.backend.api.middleware import correlation_id_middleware
from finctrl.backend.logger import setup_logging

# Initialize logging
setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Depending on how the environment handles this,
    # we might choose not to auto-init in production without migrations.
    # But for Phase 2 test environments, this is safe.
    await init_db()
    yield
    await close_db()

app = FastAPI(
    title="FINCTRL AI - Phase 5 API",
    lifespan=lifespan
)

app.add_middleware(BaseHTTPMiddleware, dispatch=correlation_id_middleware)
app.include_router(router)
