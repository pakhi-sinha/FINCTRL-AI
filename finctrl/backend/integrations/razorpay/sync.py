"""Idempotent persistence of authoritative Razorpay read results."""
import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from time import monotonic

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from finctrl.backend.database.models import (
    AuditLogModel, FinancialEventModel, RazorpayOrderModel, RazorpayPaymentModel,
    RazorpayRefundModel, RazorpaySettlementModel, RazorpaySyncStateModel,
    financial_event_id, razorpay_source_event_key,
)
from finctrl.backend.integrations.razorpay.client import RazorpayClient

logger = logging.getLogger(__name__)


class RazorpayIdentityConflict(ValueError):
    def __init__(self, message, *, entity_id=None, provider_id=None):
        super().__init__(message)
        self.entity_id, self.provider_id = entity_id, provider_id


@dataclass
class SyncStatistics:
    resource_type: str
    from_ts: int | None = None
    to_ts: int | None = None
    fetched: int = 0
    created: int = 0
    updated: int = 0
    duplicates_ignored: int = 0
    failures: int = 0
    duration_ms: int = 0


RESOURCE_CONFIG = {
    "orders": (RazorpayOrderModel, "rzp_order_id", "id"),
    "payments": (RazorpayPaymentModel, "rzp_payment_id", "id"),
    "refunds": (RazorpayRefundModel, "rzp_refund_id", "id"),
    "settlements": (RazorpaySettlementModel, "rzp_settlement_id", "id"),
}


class RazorpaySyncService:
    def __init__(self, db: AsyncSession, connector: RazorpayClient | None = None):
        self.db, self.connector = db, connector or RazorpayClient()

    @staticmethod
    def _event_payload(resource, raw):
        return {"source": "razorpay_api", "entity": resource[:-1], "payload": raw}

    async def _ledger(self, resource, raw):
        provider_id = raw["id"]
        event_key = razorpay_source_event_key(resource[:-1], provider_id)
        existing = await self.db.scalar(select(FinancialEventModel).where(
            FinancialEventModel.provider == "razorpay", FinancialEventModel.provider_event_id == event_key))
        if existing: return existing, False
        payload = self._event_payload(resource, raw)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        event = FinancialEventModel(id=financial_event_id("razorpay", event_key), provider="razorpay",
            provider_event_id=event_key, event_type=f"{resource[:-1]}.api_sync",
            payload_hash=hashlib.sha256(encoded).hexdigest(), raw_payload=payload,
            processing_status="PROCESSED", attempt_count=1,
            processed_at=datetime.now(timezone.utc))
        try:
            async with self.db.begin_nested():
                self.db.add(event)
                await self.db.flush([event])
        except IntegrityError:
            existing = await self.db.scalar(select(FinancialEventModel).where(
                FinancialEventModel.provider == "razorpay",
                FinancialEventModel.provider_event_id == event_key,
            ))
            if existing is None:
                raise
            return existing, False
        return event, True

    @staticmethod
    def _values(resource, raw):
        if resource == "orders":
            return dict(rzp_order_id=raw["id"], receipt=raw.get("receipt") or raw["id"],
                amount=raw.get("amount", 0), amount_paid=raw.get("amount_paid", 0),
                amount_due=raw.get("amount_due", 0), currency=raw.get("currency", "INR"),
                status=raw.get("status", ""), created_at_ts=raw.get("created_at", 0))
        if resource == "payments":
            return dict(rzp_payment_id=raw["id"], rzp_order_id=raw.get("order_id"),
                rzp_settlement_id=raw.get("settlement_id"), amount=raw.get("amount", 0),
                currency=raw.get("currency", "INR"), status=raw.get("status", ""),
                method=raw.get("method"), amount_refunded=raw.get("amount_refunded", 0),
                refund_status=raw.get("refund_status"), captured=int(bool(raw.get("captured", False))),
                email=raw.get("email"), contact=raw.get("contact"), fee=raw.get("fee"), tax=raw.get("tax"),
                error_code=raw.get("error_code"), error_description=raw.get("error_description"),
                created_at_ts=raw.get("created_at", 0))
        if resource == "refunds":
            return dict(rzp_refund_id=raw["id"], rzp_payment_id=raw.get("payment_id"),
                amount=raw.get("amount", 0), currency=raw.get("currency", "INR"),
                status=raw.get("status", ""), receipt=raw.get("receipt"), created_at_ts=raw.get("created_at", 0))
        return dict(rzp_settlement_id=raw["id"], amount=raw.get("amount", 0),
            status=raw.get("status", ""), fees=raw.get("fees", 0), tax=raw.get("tax", 0),
            utr=raw.get("utr"), created_at_ts=raw.get("created_at", 0))

    async def _persist(self, resource, raw, stats):
        model, id_field, _ = RESOURCE_CONFIG[resource]
        provider_id = raw.get("id")
        event, event_created = await self._ledger(resource, raw)
        existing = (await self.db.scalars(select(model).where(getattr(model, id_field) == provider_id))).first()
        values = self._values(resource, raw)
        if existing:
            immutable_fields = (id_field, "created_at_ts", "amount") + (("currency",) if "currency" in values else ())
            conflicts = [field for field in immutable_fields
                         if getattr(existing, field) != values[field]]
            if conflicts:
                raise RazorpayIdentityConflict(f"Immutable Razorpay identity conflict for {provider_id}",
                                                entity_id=existing.id, provider_id=provider_id)
            mutable = set(values) - {id_field, "created_at_ts", "amount", "currency"}
            changed = False
            for field in mutable:
                if getattr(existing, field) != values[field]:
                    setattr(existing, field, values[field]); changed = True
            if changed: stats.updated += 1
            else: stats.duplicates_ignored += 1
            return
        if not event_created:
            # A concurrent API/webhook winner owns the canonical event and must
            # also own the provider fact. Re-read after the unique-key gate.
            existing = (await self.db.scalars(select(model).where(getattr(model, id_field) == provider_id))).first()
            if existing is not None:
                stats.duplicates_ignored += 1
                return
        self.db.add(model(source_event_id=event.id, **values)); stats.created += 1

    async def sync_resource(self, resource, *, from_ts=None, to_ts=None):
        if resource not in RESOURCE_CONFIG: raise ValueError(f"Unsupported resource: {resource}")
        if from_ts is not None and to_ts is not None and from_ts > to_ts:
            raise ValueError("from_ts must not exceed to_ts")
        stats, started = SyncStatistics(resource, from_ts, to_ts), monotonic()
        fetch = getattr(self.connector, f"fetch_{resource}")
        try:
            records = fetch(from_ts=from_ts, to_ts=to_ts); stats.fetched = len(records)
            for raw in records: await self._persist(resource, raw, stats)
            await self._record_state(stats, "SUCCESS", None, records)
            await self.db.commit()
        except Exception as error:
            await self.db.rollback(); stats.failures = 1
            if isinstance(error, RazorpayIdentityConflict):
                self.db.add(AuditLogModel(entity_type="RAZORPAY_SYNC", entity_id=error.entity_id,
                    action="IMMUTABLE_IDENTITY_CONFLICT", actor="SYSTEM",
                    changes={"resource": resource, "provider_id": error.provider_id}))
            await self._record_state(stats, "FAILED", str(error), [])
            await self.db.commit(); raise
        finally:
            stats.duration_ms = int((monotonic() - started) * 1000)
            logger.info("Razorpay sync completed", extra={"razorpay_sync": asdict(stats)})
        return asdict(stats)

    async def _record_state(self, stats, status, error, records):
        state = await self.db.scalar(select(RazorpaySyncStateModel).where(
            RazorpaySyncStateModel.resource_type == stats.resource_type))
        if state is None:
            state = RazorpaySyncStateModel(resource_type=stats.resource_type); self.db.add(state)
        state.last_from_ts, state.last_to_ts, state.last_status = stats.from_ts, stats.to_ts, status
        state.last_error = error[:2000] if error else None
        state.records_fetched, state.records_created = stats.fetched, stats.created
        state.records_updated, state.duplicates_ignored = stats.updated, stats.duplicates_ignored
        state.last_provider_timestamp = max((r.get("created_at", 0) for r in records), default=state.last_provider_timestamp)
        state.last_run_at = datetime.now(timezone.utc)

    async def sync_all(self, *, from_ts=None, to_ts=None):
        return {resource: await self.sync_resource(resource, from_ts=from_ts, to_ts=to_ts)
                for resource in RESOURCE_CONFIG}
