import pytest
import uuid
import asyncio
from datetime import datetime, timezone, timedelta
from database.database import engine, Base
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from database.models import ERPRecordModel, RazorpayRecordModel, BankRecordModel
from reconciliation.engine import DeterministicReconciliationEngine
import json
import os
import dateutil.parser
import pytest_asyncio

TestSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.mark.asyncio
async def test_exact_1_to_1():
    async with TestSessionLocal() as db:
        ts = datetime.now(timezone.utc)
        erp_id = uuid.uuid4()
        rzp_id = uuid.uuid4()
        bank_id = uuid.uuid4()

        db.add(ERPRecordModel(id=erp_id, reference_id="REF1", amount=1000, type="sale", status="comp", timestamp=ts))
        db.add(RazorpayRecordModel(id=rzp_id, rzp_payment_id="pay1", rzp_settlement_id="set1", order_receipt="REF1", gross_amount=1000, fee=20, tax=3, net_amount=977, type="payment", status="captured", timestamp=ts))
        db.add(BankRecordModel(id=bank_id, transaction_ref="set1", description="desc", amount=977, type="credit", status="processed", timestamp=ts + timedelta(days=1)))
        await db.commit()

        engine_rec = DeterministicReconciliationEngine(db)
        matches = await engine_rec.run_pass_1_exact_1_to_1()
        assert matches == 1

@pytest.mark.asyncio
async def test_wrong_amount_does_not_match():
    async with TestSessionLocal() as db:
        ts = datetime.now(timezone.utc)
        db.add(ERPRecordModel(id=uuid.uuid4(), reference_id="REF2", amount=1000, type="sale", status="comp", timestamp=ts))
        db.add(RazorpayRecordModel(id=uuid.uuid4(), rzp_payment_id="pay2", rzp_settlement_id="set2", order_receipt="REF2", gross_amount=1000, fee=20, tax=3, net_amount=977, type="payment", status="captured", timestamp=ts))
        db.add(BankRecordModel(id=uuid.uuid4(), transaction_ref="set2", description="desc", amount=900, type="credit", status="processed", timestamp=ts + timedelta(days=1)))
        await db.commit()

        engine_rec = DeterministicReconciliationEngine(db)
        matches = await engine_rec.run_pass_1_exact_1_to_1()
        assert matches == 0

@pytest.mark.asyncio
async def test_1_to_n():
    async with TestSessionLocal() as db:
        ts = datetime.now(timezone.utc)
        db.add(ERPRecordModel(id=uuid.uuid4(), reference_id="REF1", amount=1000, type="sale", status="comp", timestamp=ts))
        db.add(ERPRecordModel(id=uuid.uuid4(), reference_id="REF2", amount=2000, type="sale", status="comp", timestamp=ts))

        db.add(RazorpayRecordModel(id=uuid.uuid4(), rzp_payment_id="p1", rzp_settlement_id="set_multi", order_receipt="REF1", gross_amount=1000, fee=0, tax=0, net_amount=1000, type="payment", status="captured", timestamp=ts))
        db.add(RazorpayRecordModel(id=uuid.uuid4(), rzp_payment_id="p2", rzp_settlement_id="set_multi", order_receipt="REF2", gross_amount=2000, fee=0, tax=0, net_amount=2000, type="payment", status="captured", timestamp=ts))

        db.add(BankRecordModel(id=uuid.uuid4(), transaction_ref="set_multi", description="desc", amount=3000, type="credit", status="processed", timestamp=ts + timedelta(days=1)))
        await db.commit()

        engine_rec = DeterministicReconciliationEngine(db)
        matches = await engine_rec.run_pass_2_consolidated_1_to_n()
        assert matches == 1

@pytest.mark.asyncio
async def test_candidates_generation():
    async with TestSessionLocal() as db:
        ts = datetime.now(timezone.utc)
        db.add(ERPRecordModel(id=uuid.uuid4(), reference_id="REF_ORPHAN", amount=1000, type="sale", status="comp", timestamp=ts))
        await db.commit()

        engine_rec = DeterministicReconciliationEngine(db)
        await engine_rec.run_candidate_generation()
        unresolved = await engine_rec.get_unresolved()
        assert len(unresolved) == 1
        assert unresolved[0].status == "PENDING_INVESTIGATION"

@pytest.mark.asyncio
async def test_e2e_with_dev_dataset():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dataset_path = os.path.join(base_dir, "data", "dev", "dataset.json")

    with open(dataset_path, "r") as f:
        data = json.load(f)

    async with TestSessionLocal() as db:
        for erp in data["erp_records"]:
            db.add(ERPRecordModel(**{**erp, "id": uuid.UUID(erp["id"]), "timestamp": dateutil.parser.isoparse(erp["timestamp"])}))
        for rzp in data["rzp_records"]:
            db.add(RazorpayRecordModel(**{**rzp, "id": uuid.UUID(rzp["id"]), "timestamp": dateutil.parser.isoparse(rzp["timestamp"])}))
        for bank in data["bank_records"]:
            db.add(BankRecordModel(**{**bank, "id": uuid.UUID(bank["id"]), "timestamp": dateutil.parser.isoparse(bank["timestamp"])}))
        await db.commit()

        engine_rec = DeterministicReconciliationEngine(db)
        results = await engine_rec.run_all_passes()

        assert results["pass_1_matches"] > 0
        assert results["pass_2_matches"] >= 0
        assert results["exceptions_created"] > 0
