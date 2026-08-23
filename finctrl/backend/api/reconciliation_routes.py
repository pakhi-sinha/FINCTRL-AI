from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from database.database import get_db
from reconciliation.engine import DeterministicReconciliationEngine

router = APIRouter(prefix="/api/reconciliation")

@router.post("/run")
async def run_reconciliation(db: AsyncSession = Depends(get_db)):
    engine = DeterministicReconciliationEngine(db)
    results = await engine.run_all_passes()
    return results

@router.get("/matches")
async def get_matches(db: AsyncSession = Depends(get_db)):
    engine = DeterministicReconciliationEngine(db)
    return await engine.get_matches()

@router.get("/unresolved")
async def get_unresolved(db: AsyncSession = Depends(get_db)):
    engine = DeterministicReconciliationEngine(db)
    return await engine.get_unresolved()
