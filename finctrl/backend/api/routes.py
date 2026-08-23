from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List, Dict, Any
from database.database import get_db
from database.models import ERPRecordModel, RazorpayRecordModel, BankRecordModel
from schemas.models import ERPRecord, RazorpayRecord, BankRecord

router = APIRouter(prefix="/api")

@router.post("/ingest/erp", response_model=Dict[str, Any])
async def ingest_erp(records: List[ERPRecord], db: AsyncSession = Depends(get_db)):
    inserted = 0
    for record in records:
        stmt = select(ERPRecordModel).where(ERPRecordModel.id == record.id)
        result = await db.execute(stmt)
        if result.scalar_one_or_none():
            continue # Skip duplicates
        db_record = ERPRecordModel(**record.model_dump())
        db.add(db_record)
        inserted += 1
    await db.commit()
    return {"status": "success", "inserted_count": inserted}

@router.post("/ingest/rzp", response_model=Dict[str, Any])
async def ingest_rzp(records: List[RazorpayRecord], db: AsyncSession = Depends(get_db)):
    inserted = 0
    for record in records:
        stmt = select(RazorpayRecordModel).where(RazorpayRecordModel.id == record.id)
        result = await db.execute(stmt)
        if result.scalar_one_or_none():
            continue
        db_record = RazorpayRecordModel(**record.model_dump())
        db.add(db_record)
        inserted += 1
    await db.commit()
    return {"status": "success", "inserted_count": inserted}

@router.post("/ingest/bank", response_model=Dict[str, Any])
async def ingest_bank(records: List[BankRecord], db: AsyncSession = Depends(get_db)):
    inserted = 0
    for record in records:
        stmt = select(BankRecordModel).where(BankRecordModel.id == record.id)
        result = await db.execute(stmt)
        if result.scalar_one_or_none():
            continue
        db_record = BankRecordModel(**record.model_dump())
        db.add(db_record)
        inserted += 1
    await db.commit()
    return {"status": "success", "inserted_count": inserted}

@router.get("/records/erp", response_model=List[ERPRecord])
async def get_erp(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ERPRecordModel))
    return result.scalars().all()

@router.get("/records/rzp", response_model=List[RazorpayRecord])
async def get_rzp(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(RazorpayRecordModel))
    return result.scalars().all()

@router.get("/records/bank", response_model=List[BankRecord])
async def get_bank(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(BankRecordModel))
    return result.scalars().all()
