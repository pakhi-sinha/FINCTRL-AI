from fastapi import FastAPI
from contextlib import asynccontextmanager
from finctrl.backend.database.database import init_db, close_db
from finctrl.backend.api.routes import router
from finctrl.backend.middleware import CorrelationIDMiddleware
from finctrl.backend.logging_config import setup_logging
from finctrl.backend.config import settings

# Setup structured logging
setup_logging(level="INFO")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database
    # In production with Alembic, you would run migrations separately
    # But for test environments, this ensures the DB is ready
    if settings.APP_MODE == "test":
        await init_db()
    yield
    await close_db()

app = FastAPI(
    title="FINCTRL AI - Production API",
    version="5.0.0",
    lifespan=lifespan
)

# Add correlation ID middleware
app.add_middleware(CorrelationIDMiddleware)

# Include routes
app.include_router(router)
