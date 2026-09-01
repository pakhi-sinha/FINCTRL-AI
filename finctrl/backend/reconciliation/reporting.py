"""Deterministic period reporting and close-readiness controls."""
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload

from finctrl.backend.database.models import (
    AuditLogModel, BankRecordModel, ERPRecordModel, ExceptionEvidenceModel,
    RazorpayOrderModel, RazorpayPaymentModel, RazorpayRefundModel,
    RazorpaySettlementModel, ReconciliationCandidateModel,
    ReconciliationExceptionModel, ReconciliationMatchModel,
    ReconciliationPeriodModel, ReconciliationRunModel,
)


def reconciliation_period_key(from_ts: int, to_ts: int) -> str:
    canonical = json.dumps({"from_ts": from_ts, "to_ts": to_ts}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def reconciliation_period_id(period_key: str):
    return uuid5(NAMESPACE_URL, f"finctrl:reconciliation-period:{period_key}")


def _epoch(value: datetime | None) -> int | None:
    if value is None: return None
    value = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    return int(value.timestamp())


def _utc_datetime(value: datetime) -> datetime:
    return (value.replace(tzinfo=timezone.utc) if value.tzinfo is None
            else value.astimezone(timezone.utc))


def _evidence_uuid(payload, field):
    try:
        return UUID(str((payload or {}).get(field)))
    except (TypeError, ValueError, AttributeError):
        return None


class ClosedPeriodViolation(ValueError):
    """A financial fact would enter an administratively closed period."""


async def assert_timestamps_not_closed(db, timestamps, *, operation):
    epochs = []
    for value in timestamps:
        if value is None:
            continue
        epochs.append(_epoch(value) if isinstance(value, datetime) else int(value))
    if not epochs:
        return
    conditions = [
        (ReconciliationPeriodModel.from_ts <= value) & (ReconciliationPeriodModel.to_ts >= value)
        for value in set(epochs)
    ]
    period = await db.scalar(select(ReconciliationPeriodModel).where(
        ReconciliationPeriodModel.status == "CLOSED", or_(*conditions)))
    if period is not None:
        raise ClosedPeriodViolation(
            f"{operation} contains a record in closed reconciliation period {period.id}")


async def assert_window_not_closed(db, from_ts, to_ts, *, operation):
    lower = from_ts if from_ts is not None else -9223372036854775808
    upper = to_ts if to_ts is not None else 9223372036854775807
    period = await db.scalar(select(ReconciliationPeriodModel).where(
        ReconciliationPeriodModel.status == "CLOSED",
        ReconciliationPeriodModel.to_ts >= lower,
        ReconciliationPeriodModel.from_ts <= upper,
    ))
    if period is not None:
        raise ClosedPeriodViolation(f"{operation} overlaps closed reconciliation period {period.id}")


class ReconciliationReportingService:
    def __init__(self, db):
        self.db = db

    async def create_period(self, from_ts, to_ts, *, actor=None, correlation_id=None, notes=None):
        if from_ts > to_ts: raise ValueError("from_ts must not exceed to_ts")
        key = reconciliation_period_key(from_ts, to_ts)
        existing = await self.db.scalar(select(ReconciliationPeriodModel).where(
            ReconciliationPeriodModel.period_key == key))
        if existing: return existing, False
        period = ReconciliationPeriodModel(id=reconciliation_period_id(key), period_key=key,
            from_ts=from_ts, to_ts=to_ts, status="OPEN", created_by=actor,
            correlation_id=correlation_id, notes=notes)
        try:
            async with self.db.begin_nested():
                self.db.add(period); await self.db.flush([period])
        except IntegrityError:
            period = await self.db.scalar(select(ReconciliationPeriodModel).where(
                ReconciliationPeriodModel.period_key == key))
            if period is None: raise
            return period, False
        self.db.add(AuditLogModel(entity_type="RECONCILIATION_PERIOD", entity_id=period.id,
            action="RECONCILIATION_PERIOD_CREATED", actor=actor or "SYSTEM",
            changes={"from_ts": from_ts, "to_ts": to_ts, "correlation_id": correlation_id}))
        await self.db.commit()
        return period, True

    async def get_period(self, period_id):
        return await self.db.get(ReconciliationPeriodModel, period_id)

    async def list_periods(self, from_ts=None, to_ts=None):
        query = select(ReconciliationPeriodModel)
        if from_ts is not None: query = query.where(ReconciliationPeriodModel.to_ts >= from_ts)
        if to_ts is not None: query = query.where(ReconciliationPeriodModel.from_ts <= to_ts)
        return list((await self.db.scalars(query.order_by(ReconciliationPeriodModel.from_ts))).all())

    @staticmethod
    def _in_period(period, timestamp):
        return timestamp is not None and period.from_ts <= timestamp <= period.to_ts

    async def _populations(self, period):
        start = datetime.fromtimestamp(period.from_ts, timezone.utc)
        end = datetime.fromtimestamp(period.to_ts, timezone.utc)
        erps = list((await self.db.scalars(select(ERPRecordModel).where(
            ERPRecordModel.timestamp >= start, ERPRecordModel.timestamp <= end))).all())
        orders = list((await self.db.scalars(select(RazorpayOrderModel).where(
            RazorpayOrderModel.created_at_ts >= period.from_ts,
            RazorpayOrderModel.created_at_ts <= period.to_ts))).all())
        payments = list((await self.db.scalars(select(RazorpayPaymentModel).where(
            RazorpayPaymentModel.created_at_ts >= period.from_ts,
            RazorpayPaymentModel.created_at_ts <= period.to_ts))).all())
        refunds = list((await self.db.scalars(select(RazorpayRefundModel).where(
            RazorpayRefundModel.created_at_ts >= period.from_ts,
            RazorpayRefundModel.created_at_ts <= period.to_ts))).all())
        settlements = list((await self.db.scalars(select(RazorpaySettlementModel).where(
            RazorpaySettlementModel.created_at_ts >= period.from_ts,
            RazorpaySettlementModel.created_at_ts <= period.to_ts))).all())
        banks = list((await self.db.scalars(select(BankRecordModel).where(
            BankRecordModel.timestamp >= start, BankRecordModel.timestamp <= end))).all())
        return {"erp": erps, "orders": orders, "payments": payments,
                "refunds": refunds, "settlements": settlements, "bank": banks}

    async def _runs(self, period):
        return list((await self.db.scalars(select(ReconciliationRunModel).where(
            ReconciliationRunModel.from_ts == period.from_ts,
            ReconciliationRunModel.to_ts == period.to_ts,
        ).order_by(ReconciliationRunModel.requested_at.desc(),
                   ReconciliationRunModel.run_key.desc()))).all())

    async def list_period_runs(self, period):
        return await self._runs(period)

    async def _exceptions(self, period, source_ids):
        exceptions = list((await self.db.scalars(select(ReconciliationExceptionModel)
            .options(selectinload(ReconciliationExceptionModel.evidence)))).all())
        return [item for item in exceptions if
                any(evidence.record_id in source_ids for evidence in item.evidence)
                or self._in_period(period, _epoch(item.created_at))]

    async def report(self, period):
        populations = await self._populations(period)
        source_ids = {record.id for records in populations.values() for record in records}
        runs = await self._runs(period)
        successful = [run for run in runs if run.status == "SUCCEEDED"]
        latest_successful = successful[0] if successful else None
        exceptions = await self._exceptions(period, source_ids)
        candidates = list((await self.db.scalars(select(ReconciliationCandidateModel))).all())
        candidates = [candidate for candidate in candidates if
                      _evidence_uuid(candidate.evidence_payload, "erp_id") in source_ids
                      or _evidence_uuid(candidate.evidence_payload, "rzp_id") in source_ids]
        matches = list((await self.db.scalars(select(ReconciliationMatchModel)
            .options(selectinload(ReconciliationMatchModel.evidence)))).all())
        matches = [match for match in matches if any(evidence.record_id in source_ids for evidence in match.evidence)]
        amounts = {}
        for population, records in populations.items():
            grouped = defaultdict(int)
            for record in records:
                if hasattr(record, "amount") and hasattr(record, "currency"):
                    grouped[record.currency] += record.amount
            if grouped: amounts[population] = dict(sorted(grouped.items()))
        return {
            "period_id": str(period.id), "period_key": period.period_key,
            "from_ts": period.from_ts, "to_ts": period.to_ts, "status": period.status,
            "counts": {"erp_records": len(populations["erp"]), "razorpay_orders": len(populations["orders"]),
                "razorpay_payments": len(populations["payments"]), "razorpay_refunds": len(populations["refunds"]),
                "razorpay_settlements": len(populations["settlements"]), "bank_records": len(populations["bank"]),
                "reconciled_matches": len(matches),
                "unresolved_candidates": sum(candidate.status == "PENDING_INVESTIGATION" for candidate in candidates),
                "open_exceptions": sum(item.status in {"OPEN", "INVESTIGATING"} for item in exceptions),
                "resolved_exceptions": sum(item.status == "RESOLVED" for item in exceptions),
                "dismissed_exceptions": sum(item.status == "DISMISSED" for item in exceptions),
                "critical_high_exceptions": sum(item.status in {"OPEN", "INVESTIGATING"} and item.severity in {"CRITICAL", "HIGH"} for item in exceptions),
                "reconciliation_runs": len(runs)},
            "match_types": dict(sorted(Counter(match.match_type for match in matches).items())),
            "exception_types": dict(sorted(Counter(item.exception_type for item in exceptions).items())),
            "exception_severities": dict(sorted(Counter(item.severity for item in exceptions).items())),
            "run_statuses": dict(sorted(Counter(run.status for run in runs).items())),
            "latest_successful_run_id": str(latest_successful.id) if latest_successful else None,
            "amounts_by_population_and_currency": amounts,
        }

    async def exception_report(self, period, *, status=None, severity=None, exception_type=None,
                               source=None,
                               evaluated_at=None):
        populations = await self._populations(period)
        source_ids = {record.id for records in populations.values() for record in records}
        exceptions = await self._exceptions(period, source_ids)
        runs = await self._runs(period)
        successful = next((run for run in runs if run.status == "SUCCEEDED"), None)
        evaluated_at = _utc_datetime(evaluated_at or datetime.now(timezone.utc))
        result = []
        for item in exceptions:
            if status and item.status != status: continue
            if severity and item.severity != severity: continue
            if exception_type and item.exception_type != exception_type: continue
            if source and not any(e.record_type == source or e.source_id == source for e in item.evidence):
                continue
            created = _utc_datetime(item.created_at)
            age_seconds = max(0, int((evaluated_at - created).total_seconds()))
            result.append({"id": str(item.id), "exception_key": item.exception_key,
                "exception_type": item.exception_type, "status": item.status, "severity": item.severity,
                "created_at": item.created_at, "age_seconds": age_seconds, "age_days": age_seconds // 86400,
                "latest_successful_run_id": str(successful.id) if successful else None,
                "evidence_available": bool(item.evidence),
                "candidate_available": any(e.record_type == "RECONCILIATION_CANDIDATE" for e in item.evidence)})
        return result

    async def readiness(self, period, *, evaluated_at=None):
        report = await self.report(period)
        runs = await self._runs(period)
        latest = runs[0] if runs else None
        reasons = []
        if report["latest_successful_run_id"] is None: reasons.append("NO_SUCCESSFUL_RECONCILIATION_RUN")
        if latest is not None and latest.status != "SUCCEEDED": reasons.append("LATEST_RECONCILIATION_RUN_NOT_SUCCESSFUL")
        exceptions = await self.exception_report(period, evaluated_at=evaluated_at)
        if any(item["status"] in {"OPEN", "INVESTIGATING"} and item["severity"] == "CRITICAL" for item in exceptions):
            reasons.append("OPEN_CRITICAL_EXCEPTION")
        if report["counts"]["unresolved_candidates"]:
            reasons.append("UNRESOLVED_CANDIDATES")
        if any(item["status"] in {"OPEN", "INVESTIGATING"} and not item["evidence_available"] for item in exceptions):
            reasons.append("OPEN_EXCEPTION_MISSING_EVIDENCE")
        return {"ready": not reasons, "status": "READY" if not reasons else "BLOCKED",
                "blocking_reasons": reasons, "summary_counts": report["counts"],
                "evaluated_at": evaluated_at or datetime.now(timezone.utc),
                "source_run_id": report["latest_successful_run_id"]}

    async def close_period(self, period, *, actor, correlation_id=None):
        if period.status == "CLOSED": raise ValueError("Period is already closed")
        readiness = await self.readiness(period)
        if not readiness["ready"]: raise ValueError("Period is not close-ready: " + ", ".join(readiness["blocking_reasons"]))
        result = await self.db.execute(update(ReconciliationPeriodModel).where(
            ReconciliationPeriodModel.id == period.id,
            ReconciliationPeriodModel.status != "CLOSED",
        ).values(status="CLOSED", closed_at=datetime.now(timezone.utc), closed_by=actor,
                 latest_run_id=UUID(readiness["source_run_id"])))
        if result.rowcount != 1:
            await self.db.rollback(); raise ValueError("Period is already closed")
        self.db.add(AuditLogModel(entity_type="RECONCILIATION_PERIOD", entity_id=period.id,
            action="RECONCILIATION_PERIOD_CLOSE_READINESS", actor=actor,
            changes={"ready": True, "run_id": readiness["source_run_id"], "correlation_id": correlation_id}))
        self.db.add(AuditLogModel(entity_type="RECONCILIATION_PERIOD", entity_id=period.id,
            action="RECONCILIATION_PERIOD_CLOSED", actor=actor,
            changes={"run_id": readiness["source_run_id"], "correlation_id": correlation_id}))
        await self.db.commit()
        await self.db.refresh(period)
        return period

    async def reopen_period(self, period, *, actor, correlation_id=None, reason=None):
        if period.status != "CLOSED":
            raise ValueError("Only a closed period can be reopened")
        result = await self.db.execute(update(ReconciliationPeriodModel).where(
            ReconciliationPeriodModel.id == period.id,
            ReconciliationPeriodModel.status == "CLOSED",
        ).values(status="OPEN", closed_at=None, closed_by=None, latest_run_id=None))
        if result.rowcount != 1:
            await self.db.rollback()
            raise ValueError("Period is no longer closed")
        self.db.add(AuditLogModel(entity_type="RECONCILIATION_PERIOD", entity_id=period.id,
            action="RECONCILIATION_PERIOD_REOPENED", actor=actor,
            changes={"reason": reason, "correlation_id": correlation_id}))
        await self.db.commit()
        await self.db.refresh(period)
        return period
