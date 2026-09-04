"""Durable operational control around the deterministic reconciliation engine."""
import hashlib
import json
import logging
import inspect
from datetime import datetime, timezone
from time import monotonic
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload

from finctrl.backend.database.database import async_session_maker
from finctrl.backend.database.models import (
    AuditLogModel, BankRecordModel, ERPRecordModel, ExceptionModel,
    RazorpayOrderModel, RazorpayPaymentModel, RazorpayRefundModel,
    RazorpaySettlementModel, ReconciliationCandidateModel,
    ReconciliationExceptionModel, ReconciliationRunModel,
    ReconciliationStageRunModel,
)
from finctrl.backend.reconciliation.engine import (
    ReconciliationScope, scoped_select,
    stage_a_exact_match, stage_b_payment_arithmetic,
    stage_c_settlement_reconciliation, stage_d_refund_aware_reconciliation,
    stage_e_candidates_and_exceptions,
)
from finctrl.backend.reconciliation.reporting import assert_window_not_closed
from finctrl.backend.config import settings
from finctrl.backend.recovery.leases import Lease, claim, db_now, heartbeat_loop, owned

logger = logging.getLogger(__name__)
RUN_STATUSES = {"REQUESTED", "RUNNING", "SUCCEEDED", "PARTIAL", "FAILED", "CANCELLED"}
RETRYABLE_STATUSES = {"FAILED", "PARTIAL"}
MAX_ATTEMPTS = 3


def reconciliation_run_key(from_ts=None, to_ts=None, request_key=None):
    canonical = json.dumps({
        "operation": "reconciliation",
        "from_ts": from_ts,
        "to_ts": to_ts,
        "request_key": "full" if request_key is None else request_key,
    }, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def reconciliation_run_id(run_key):
    return uuid5(NAMESPACE_URL, f"finctrl:reconciliation-run:{run_key}")


class ReconciliationRunService:
    def __init__(self, session_factory=async_session_maker, stage_functions=None,
                 execution_session_factory=None):
        self.session_factory = session_factory
        self.execution_session_factory = execution_session_factory or session_factory
        self.stage_functions = stage_functions or (
            ("STAGE_A_EXACT_MATCH", stage_a_exact_match),
            ("STAGE_B_PAYMENT_ARITHMETIC", stage_b_payment_arithmetic),
            ("STAGE_C_SETTLEMENT", stage_c_settlement_reconciliation),
            ("STAGE_D_REFUND", stage_d_refund_aware_reconciliation),
            ("STAGE_E_CANDIDATES_EXCEPTIONS", stage_e_candidates_and_exceptions),
        )

    async def request_and_run(self, *, actor=None, correlation_id=None, from_ts=None, to_ts=None, request_key=None):
        if from_ts is not None and to_ts is not None and from_ts > to_ts:
            raise ValueError("from_ts must not exceed to_ts")
        async with self.session_factory() as db:
            await assert_window_not_closed(db, from_ts, to_ts, operation="reconciliation run")
        key = reconciliation_run_key(from_ts, to_ts, request_key)
        run, created = await self._create_run(key, actor, correlation_id, from_ts, to_ts)
        if created:
            await self._execute(run.id)
        return await self.get_run(run.id)

    async def _assert_scope_open(self, from_ts, to_ts):
        async with self.session_factory() as db:
            await assert_window_not_closed(db, from_ts, to_ts, operation="reconciliation run")

    async def retry(self, run_id, *, actor=None, correlation_id=None):
        original = await self.get_run(run_id)
        if original is None: raise LookupError("Reconciliation run not found")
        if original.status not in RETRYABLE_STATUSES:
            raise ValueError(f"Run status {original.status} cannot be retried")
        attempt = original.attempt + 1
        if attempt > MAX_ATTEMPTS: raise ValueError("Maximum reconciliation attempts exceeded")
        await self._assert_scope_open(original.from_ts, original.to_ts)
        key = f"{original.run_key}:retry:{attempt}"
        run, created = await self._create_run(key, actor, correlation_id,
                                              original.from_ts, original.to_ts,
                                              retry_of_id=original.id, attempt=attempt)
        if created:
            await self._audit(run.id, "RECONCILIATION_RUN_RETRIED", actor,
                              {"original_run_id": str(original.id), "attempt": attempt, "correlation_id": correlation_id})
            await self._execute(run.id)
        return await self.get_run(run.id)

    async def _create_run(self, key, actor, correlation_id, from_ts, to_ts, retry_of_id=None, attempt=1):
        run = ReconciliationRunModel(id=reconciliation_run_id(key), run_key=key, status="REQUESTED",
            initiated_by=actor, correlation_id=correlation_id, from_ts=from_ts, to_ts=to_ts,
            retry_of_id=retry_of_id, attempt=attempt)
        created = True
        async with self.session_factory() as db:
            try:
                db.add(run); await db.commit()
            except IntegrityError:
                await db.rollback(); created = False
                run = await db.scalar(select(ReconciliationRunModel).where(ReconciliationRunModel.run_key == key))
                if run is None: raise
        if created:
            await self._audit(run.id, "RECONCILIATION_RUN_REQUESTED", actor,
                              {"run_key": key, "correlation_id": correlation_id, "from_ts": from_ts, "to_ts": to_ts})
        return run, created

    async def _execute(self, run_id, *, worker_id="api"):
        lease = Lease.new(worker_id)
        before = await self.get_run(run_id)
        async with self.session_factory() as db:
            if not await claim(db, ReconciliationRunModel, run_id, lease,
                    settings.RECONCILIATION_LEASE_SECONDS,
                    eligible_statuses={"REQUESTED"}, active_status="RUNNING"):
                await db.rollback()
                return False
            await db.execute(update(ReconciliationRunModel).where(
                ReconciliationRunModel.id == run_id,
                ReconciliationRunModel.execution_attempt_id == lease.attempt_id,
            ).values(started_at=db_now()))
            action = "RECONCILIATION_RUN_TAKEN_OVER" if before and before.status == "RUNNING" else "RECONCILIATION_RUN_CLAIMED"
            db.add(AuditLogModel(entity_type="RECONCILIATION_RUN", entity_id=run_id,
                action=action, actor=worker_id,
                changes={"attempt_id": lease.attempt_id}))
            await db.commit()
        async with heartbeat_loop(self.session_factory, ReconciliationRunModel, run_id,
                lease, settings.RECONCILIATION_LEASE_SECONDS,
                settings.RECONCILIATION_HEARTBEAT_SECONDS, "RUNNING") as ownership_lost:
            try:
                return await self._execute_owned(run_id, lease, ownership_lost)
            except Exception as error:
                await self._terminalize_execution_failure(
                    run_id, monotonic(), error, [], [], None, 0, None, None, lease)
                return False

    async def _execute_owned(self, run_id, lease, ownership_lost):
        run_scope = await self.get_run(run_id)
        await self._assert_scope_open(run_scope.from_ts, run_scope.to_ts)
        scope = ReconciliationScope(run_scope.from_ts, run_scope.to_ts)
        started = monotonic()
        await self._audit(run_id, "RECONCILIATION_RUN_STARTED", "SYSTEM", {})
        totals = {"matches_created": 0, "candidates_created": 0,
                  "exceptions_created": 0, "records_examined": 0}
        stage_results = []
        stage_audits = []
        current_stage = None
        current_sequence = 0
        current_started = None
        current_started_at = None
        async with self.execution_session_factory() as execution_db:
            try:
                for sequence, (name, function) in enumerate(self.stage_functions, 1):
                    current_stage, current_sequence = name, sequence
                    stage_started = monotonic()
                    stage_started_at = datetime.now(timezone.utc)
                    current_started, current_started_at = stage_started, stage_started_at
                    stage_audits.append((stage_started_at, "RECONCILIATION_STAGE_STARTED",
                                         {"stage": name}))
                    examined = await self._records_examined(execution_db, sequence, scope)
                    try:
                        result = (await function(execution_db, scope)
                                  if len(inspect.signature(function).parameters) >= 2
                                  else await function(execution_db))
                        counts = self._stage_counts(sequence, result)
                        counts["records_examined"] = examined
                        for key, value in counts.items(): totals[key] += value
                        stage_results.append((name, sequence, "SUCCEEDED", stage_started_at,
                                              datetime.now(timezone.utc),
                                              int((monotonic()-stage_started)*1000), counts, None))
                        stage_audits.append((stage_results[-1][4], "RECONCILIATION_STAGE_SUCCEEDED",
                                             {"stage": name, **counts}))
                    except Exception as error:
                        message = f"{type(error).__name__}: reconciliation stage failed"
                        await execution_db.rollback()
                        stage_results.append((name, sequence, "FAILED", stage_started_at,
                                              datetime.now(timezone.utc),
                                              int((monotonic()-stage_started)*1000),
                                              {"records_examined": examined}, message))
                        stage_audits.append((stage_results[-1][4], "RECONCILIATION_STAGE_FAILED",
                                             {"stage": name, "records_examined": examined}))
                        for skipped_sequence, (skipped_name, _) in enumerate(self.stage_functions, 1):
                            if skipped_sequence > sequence:
                                stage_results.append((skipped_name, skipped_sequence, "SKIPPED",
                                                      None, None, 0, {}, None))
                                stage_audits.append((datetime.now(timezone.utc),
                                                     "RECONCILIATION_STAGE_SKIPPED",
                                                     {"stage": skipped_name}))
                        await self._terminalize_execution_failure(
                            run_id, started, error, stage_results, stage_audits,
                            current_stage, current_sequence, current_started,
                            current_started_at, lease)
                        return False
                if ownership_lost.is_set():
                    raise RuntimeError("Reconciliation ownership lost")
                self._add_stage_records(execution_db, run_id, stage_results, stage_audits)
                terminal = await execution_db.execute(update(ReconciliationRunModel).where(
                    owned(ReconciliationRunModel, run_id, lease, active_status="RUNNING")
                ).values(status="SUCCEEDED", current_stage=None,
                    completed_at=datetime.now(timezone.utc),
                    duration_ms=int((monotonic()-started)*1000), **totals,
                    lease_owner=None, lease_expires_at=None))
                if terminal.rowcount != 1:
                    raise RuntimeError("Reconciliation ownership lost")
                execution_db.add(AuditLogModel(entity_type="RECONCILIATION_RUN", entity_id=run_id,
                    action="RECONCILIATION_RUN_SUCCEEDED", actor=lease.owner,
                    changes={"attempt_id": lease.attempt_id, **totals}))
                await execution_db.commit()
            except Exception as error:
                await execution_db.rollback()
                await self._terminalize_execution_failure(
                    run_id, started, error, stage_results, stage_audits,
                    current_stage, current_sequence, current_started, current_started_at, lease,
                )
                return False
        completed_run = await self.get_run(run_id)
        logger.info("Reconciliation run completed", extra={"reconciliation_run": {
            "run_id": str(run_id), "status": "SUCCEEDED",
            "correlation_id": completed_run.correlation_id, **totals}})
        return True

    async def _terminalize_execution_failure(self, run_id, run_started, error,
                                             stage_results, stage_audits,
                                             current_stage, current_sequence,
                                             current_started, current_started_at, lease):
        """Durably terminate failures outside the per-stage handler."""
        message = next((result[7] for result in reversed(stage_results)
                        if result[2] == "FAILED" and result[7]),
                       f"{type(error).__name__}: reconciliation transaction failed")
        recorded_sequences = {result[1] for result in stage_results}
        if current_stage and current_sequence not in recorded_sequences:
            completed_at = datetime.now(timezone.utc)
            stage_results.append((current_stage, current_sequence, "FAILED",
                current_started_at, completed_at,
                int((monotonic()-current_started)*1000) if current_started is not None else 0,
                {}, message))
            stage_audits.append((completed_at, "RECONCILIATION_STAGE_FAILED",
                                 {"stage": current_stage}))
            recorded_sequences.add(current_sequence)
        for sequence, (name, _) in enumerate(self.stage_functions, 1):
            if sequence not in recorded_sequences:
                stage_results.append((name, sequence, "SKIPPED", None, None, 0, {}, None))
                stage_audits.append((datetime.now(timezone.utc),
                                     "RECONCILIATION_STAGE_SKIPPED", {"stage": name}))
        async with self.session_factory() as db:
            terminal = await db.execute(update(ReconciliationRunModel).where(
                owned(ReconciliationRunModel, run_id, lease, active_status="RUNNING")
            ).values(status="FAILED", current_stage=current_stage,
                completed_at=datetime.now(timezone.utc), errors_count=1,
                duration_ms=int((monotonic()-run_started)*1000), error_message=message,
                matches_created=0, candidates_created=0, exceptions_created=0,
                lease_owner=None, lease_expires_at=None))
            if terminal.rowcount == 1:
                self._add_stage_records(db, run_id, stage_results, stage_audits)
                db.add(AuditLogModel(entity_type="RECONCILIATION_RUN", entity_id=run_id,
                    action="RECONCILIATION_RUN_FAILED", actor=lease.owner,
                    changes={"stage": current_stage, "error": message,
                             "attempt_id": lease.attempt_id}))
                await db.commit()
            else:
                await db.rollback()

    @staticmethod
    def _add_stage_records(db, run_id, results, events):
        for name, sequence, status, started_at, completed_at, duration_ms, counts, error in results:
            stage = ReconciliationStageRunModel(run_id=run_id, stage_name=name,
                sequence=sequence, status=status, started_at=started_at,
                completed_at=completed_at, duration_ms=duration_ms,
                error_message=error)
            for key, value in counts.items():
                setattr(stage, key, value)
            db.add(stage)
        for timestamp, action, changes in events:
            db.add(AuditLogModel(entity_type="RECONCILIATION_RUN", entity_id=run_id,
                action=action, actor="SYSTEM", timestamp=timestamp, changes=changes))

    async def recover_eligible(self, worker_id, limit=None):
        """Claim and re-run REQUESTED or database-expired RUNNING operations."""
        limit = limit or settings.RECOVERY_BATCH_SIZE
        async with self.session_factory() as db:
            ids = list((await db.scalars(select(ReconciliationRunModel.id).where(or_(
                ReconciliationRunModel.status == "REQUESTED",
                (ReconciliationRunModel.status == "RUNNING") &
                (or_(ReconciliationRunModel.lease_expires_at.is_(None),
                     ReconciliationRunModel.lease_expires_at <= db_now())),
            )).order_by(ReconciliationRunModel.requested_at).limit(limit))).all())
        recovered = 0
        for run_id in ids:
            recovered += int(await self._execute(run_id, worker_id=worker_id))
        return recovered

    @staticmethod
    def _stage_counts(sequence, result):
        counts = {"matches_created": 0, "candidates_created": 0, "exceptions_created": 0}
        if sequence == 1: counts["matches_created"] = result
        elif sequence == 2: counts["exceptions_created"] = result
        elif sequence in (3, 4): counts["matches_created"], counts["exceptions_created"] = result
        else: counts["candidates_created"], counts["exceptions_created"] = result
        return counts

    async def _records_examined(self, db, sequence, scope):
        models = {
            1: (ERPRecordModel, RazorpayOrderModel, RazorpayPaymentModel, BankRecordModel),
            2: (RazorpayPaymentModel,),
            3: (RazorpaySettlementModel, RazorpayPaymentModel, ERPRecordModel, BankRecordModel),
            4: (RazorpayRefundModel, RazorpayPaymentModel),
            5: (ERPRecordModel, RazorpayPaymentModel),
        }[sequence]
        examined = 0
        for model in models:
            examined += (await db.scalar(scoped_select(model, scope).with_only_columns(func.count(model.id)))) or 0
        return examined

    async def _persist_stage_results(self, run_id, results):
        async with self.session_factory() as db:
            for name, sequence, status, stage_started_at, stage_completed_at, duration_ms, counts, error in results:
                stage = ReconciliationStageRunModel(run_id=run_id, stage_name=name,
                    sequence=sequence, status=status,
                    started_at=stage_started_at,
                    completed_at=stage_completed_at,
                    duration_ms=duration_ms,
                    error_message=error)
                for key, value in counts.items(): setattr(stage, key, value)
                db.add(stage)
            await db.commit()

    async def _persist_stage_audits(self, run_id, events):
        """Persist the timeline captured around execution without committing financial work."""
        async with self.session_factory() as db:
            for timestamp, action, changes in events:
                db.add(AuditLogModel(entity_type="RECONCILIATION_RUN", entity_id=run_id,
                    action=action, actor="SYSTEM", timestamp=timestamp, changes=changes))
            await db.commit()

    async def _update_run(self, run_id, **values):
        async with self.session_factory() as db:
            run = await db.get(ReconciliationRunModel, run_id)
            if run.status in {"SUCCEEDED", "FAILED", "PARTIAL", "CANCELLED"} and values.get("status") == "RUNNING":
                raise ValueError(f"Invalid run transition: {run.status} -> RUNNING")
            for key, value in values.items(): setattr(run, key, value)
            await db.commit()

    async def _audit(self, run_id, action, actor, changes):
        async with self.session_factory() as db:
            db.add(AuditLogModel(entity_type="RECONCILIATION_RUN", entity_id=run_id,
                                 action=action, actor=actor or "SYSTEM", changes=changes))
            await db.commit()

    async def get_run(self, run_id):
        async with self.session_factory() as db:
            return await db.scalar(select(ReconciliationRunModel).where(ReconciliationRunModel.id == run_id)
                                   .options(selectinload(ReconciliationRunModel.stages)))

    async def list_runs(self):
        async with self.session_factory() as db:
            return list((await db.scalars(select(ReconciliationRunModel)
                .options(selectinload(ReconciliationRunModel.stages))
                .order_by(ReconciliationRunModel.requested_at.desc()))).all())
