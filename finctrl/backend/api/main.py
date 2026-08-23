from fastapi import FastAPI
from api.routes import router

app = FastAPI(title="FINCTRL AI Backend", version="1.0.0")

from api.reconciliation_routes import router as rec_router

app.include_router(router)
app.include_router(rec_router)

@app.get("/health")
async def health_check():
    return {"status": "ok"}
