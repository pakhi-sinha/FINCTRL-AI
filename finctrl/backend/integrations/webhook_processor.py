"""
Webhook processing service for reliable, idempotent webhook handling.
Includes duplicate detection, concurrent safety, and replay capabilities.
"""
import hashlib
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError

from finctrl.backend.database.models import (
    FinancialEventModel,
    financial_event_id,
    razorpay_payload_event_key,
    RazorpayOrderModel,
    RazorpayPaymentModel,
    RazorpaySettlementModel,
    RazorpayRefundModel,
    AuditLogModel
)
from finctrl.backend.config import settings
from finctrl.backend.database.database import async_session_maker
from finctrl.backend.recovery.leases import Lease, claim, db_now, heartbeat_loop, owned
from finctrl.backend.reconciliation.reporting import assert_timestamps_not_closed

logger = logging.getLogger(__name__)


class RazorpayWebhookIdentityConflict(ValueError):
    pass


def _sanitized_processing_error(error: Exception) -> str:
    return f"{type(error).__name__}: webhook processing failed"


def _assert_immutable_provider_fields(existing, incoming, fields):
    conflicts = [model_field for model_field, payload_field in fields
                 if incoming.get(payload_field) is not None
                 and getattr(existing, model_field) != incoming.get(payload_field)]
    if conflicts:
        raise RazorpayWebhookIdentityConflict("Immutable Razorpay provider identity conflict")


class WebhookProcessor:
    """
    Handles webhook processing with idempotency guarantees.
    """

    def __init__(self, db: AsyncSession, session_factory=async_session_maker, worker_id="api"):
        self.db, self.session_factory, self.worker_id = db, session_factory, worker_id

    async def process_razorpay_webhook(
        self,
        body_bytes: bytes,
        signature: str,
        event_id: str
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Process a Razorpay webhook with idempotency guarantees.

        Returns: (already_processed, event_uuid, error_message)
        """
        payload_hash = hashlib.sha256(body_bytes).hexdigest()

        # Preserve Phase 6A delivery-ID idempotency for object-less events.
        delivery_event = await self.db.scalar(select(FinancialEventModel).where(
            FinancialEventModel.provider == "razorpay",
            FinancialEventModel.provider_event_id == event_id,
        ))
        if delivery_event:
            if delivery_event.payload_hash != payload_hash:
                return False, str(delivery_event.id), "Event ID payload conflict"
            if delivery_event.processing_status == "FAILED":
                success, replayed_id, error = await self.replay_event(str(delivery_event.id))
                return False, replayed_id, error if not success else None
            if delivery_event.processing_status in {"PROCESSING", "RETRYING"}:
                return False, str(delivery_event.id), "Webhook processing already in progress"
            return True, str(delivery_event.id), None

        # 1. Verify signature
        if not self._verify_signature(body_bytes, signature):
            logger.warning(f"Invalid signature for webhook: event_id={event_id}")
            return False, None, "Invalid signature"

        # 2. Parse payload
        try:
            payload = json.loads(body_bytes)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON payload for webhook: event_id={event_id}")
            return False, None, "Invalid JSON payload"

        # 3. Resolve the same object-level ledger identity used by API sync.
        canonical_event_id = razorpay_payload_event_key(payload, event_id)
        existing_event = await self.db.scalar(select(FinancialEventModel).where(
            FinancialEventModel.provider == "razorpay",
            FinancialEventModel.provider_event_id == canonical_event_id,
        ))
        if existing_event:
            is_delivery_fallback = canonical_event_id == event_id
            if is_delivery_fallback and existing_event.payload_hash != payload_hash:
                return False, str(existing_event.id), "Event ID payload conflict"
            if existing_event.processing_status == "FAILED":
                success, replayed_id, error = await self.replay_event(str(existing_event.id))
                return False, replayed_id, error if not success else None
            if existing_event.processing_status in {"PROCESSING", "RETRYING"}:
                return False, str(existing_event.id), "Webhook processing already in progress"
            try:
                await self._process_event_payload(existing_event, payload)
            except RazorpayWebhookIdentityConflict:
                return False, str(existing_event.id), "Provider identity conflict"
            await self._record_delivery(existing_event, event_id)
            await self.db.commit()
            return True, str(existing_event.id), None

        # 4. Create event with atomic transaction
        return await self._create_and_process_event(event_id, body_bytes, payload)

    async def _record_delivery(self, event_model, delivery_event_id):
        self.db.add(AuditLogModel(
            entity_type="FINANCIAL_EVENT", entity_id=event_model.id,
            action="RAZORPAY_WEBHOOK_DELIVERY", actor="RAZORPAY",
            changes={"delivery_event_id": delivery_event_id},
        ))

    async def _create_and_process_event(
        self,
        event_id: str,
        body_bytes: bytes,
        payload: Dict[str, Any]
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Create event record and process it in a single transaction.
        Uses database-level constraints to prevent concurrent duplicates.
        """
        payload_hash = hashlib.sha256(body_bytes).hexdigest()
        event_type = payload.get("event", "unknown")
        canonical_event_id = razorpay_payload_event_key(payload, event_id)

        lease = Lease.new(self.worker_id)
        event_model = FinancialEventModel(
            id=financial_event_id("razorpay", canonical_event_id),
            provider="razorpay",
            provider_event_id=canonical_event_id,
            event_type=event_type,
            payload_hash=payload_hash,
            raw_payload=payload,
            processing_status="PROCESSING", attempt_count=1,
            lease_owner=lease.owner, execution_attempt_id=lease.attempt_id,
        )

        try:
            self.db.add(event_model)
            await self.db.flush()
            persisted_event_id = event_model.id
            await self.db.execute(update(FinancialEventModel).where(
                FinancialEventModel.id == event_model.id).values(
                    heartbeat_at=db_now(),
                    lease_expires_at=self._expiry(settings.WEBHOOK_LEASE_SECONDS)))
            self.db.add(AuditLogModel(entity_type="WEBHOOK", entity_id=event_model.id,
                action="WEBHOOK_CLAIMED", actor=lease.owner,
                changes={"attempt_id": lease.attempt_id, "status": "PROCESSING"}))
            await self.db.commit()
            success, error = await self._process_owned(event_model.id, payload, lease,
                                                        delivery_event_id=event_id)
            return False, str(persisted_event_id), error if not success else None

        except IntegrityError:
            # Concurrent insertion - rollback and check if another process succeeded
            await self.db.rollback()

            # Check if another process already processed it
            existing = await self.db.execute(
                select(FinancialEventModel).filter_by(
                    provider="razorpay",
                    provider_event_id=canonical_event_id
                )
            )
            existing_event = existing.scalar_one_or_none()

            if existing_event and existing_event.processing_status == "PROCESSED":
                await self._record_delivery(existing_event, event_id)
                await self.db.commit()
                logger.info(
                    f"Webhook processed concurrently: event_id={event_id}",
                    extra={"event_id": event_id, "event_uuid": str(existing_event.id)}
                )
                return True, str(existing_event.id), None
            return False, str(existing_event.id) if existing_event else None, "Concurrent processing conflict"

        except Exception as e:
            # Processing failed - mark as FAILED
            await self.db.rollback()
            error_message = _sanitized_processing_error(e)
            logger.error(
                f"Webhook processing failed: event_id={event_id}",
                extra={"event_id": event_id, "error": error_message}
            )

            existing = await self.db.get(FinancialEventModel,
                financial_event_id("razorpay", canonical_event_id), populate_existing=True)
            if existing is not None:
                return False, str(existing.id), error_message
            # Failure before the durable PROCESSING claim was committed.
            failed_event = FinancialEventModel(
                id=financial_event_id("razorpay", canonical_event_id),
                provider="razorpay",
                provider_event_id=canonical_event_id,
                event_type=event_type,
                payload_hash=payload_hash,
                raw_payload=payload,
                processing_status="FAILED",
                attempt_count=1,
                error_message=error_message
            )
            self.db.add(failed_event)
            await self.db.commit()

            return False, str(failed_event.id), error_message

    def _expiry(self, seconds):
        from finctrl.backend.recovery.leases import db_expiry
        return db_expiry(self.db, seconds)

    async def _process_owned(self, event_id, payload, lease, delivery_event_id=None):
        async with heartbeat_loop(self.session_factory, FinancialEventModel, event_id, lease,
                settings.WEBHOOK_LEASE_SECONDS, settings.WEBHOOK_HEARTBEAT_SECONDS,
                "PROCESSING" if delivery_event_id is not None else "RETRYING",
                status_field="processing_status") as ownership_lost:
            try:
                event = await self.db.get(FinancialEventModel, event_id)
                await self._process_event_payload(event, payload)
                if delivery_event_id is not None:
                    await self._record_delivery(event, delivery_event_id)
                if ownership_lost.is_set():
                    raise RuntimeError("Webhook ownership lost")
                active_status = "PROCESSING" if delivery_event_id is not None else "RETRYING"
                terminal = await self.db.execute(update(FinancialEventModel).where(
                    owned(FinancialEventModel, event_id, lease, active_status=active_status,
                          status_field="processing_status")
                ).values(processing_status="PROCESSED", processed_at=db_now(),
                    error_message=None, lease_owner=None, lease_expires_at=None))
                if terminal.rowcount != 1:
                    await self.db.rollback()
                    return False, "Webhook ownership lost"
                self.db.add(AuditLogModel(entity_type="WEBHOOK", entity_id=event_id,
                    action="WEBHOOK_PROCESSED", actor=lease.owner,
                    changes={"attempt_id": lease.attempt_id}))
                await self.db.commit()
                return True, None
            except Exception as error:
                await self.db.rollback()
                message = _sanitized_processing_error(error)
                active_status = "PROCESSING" if delivery_event_id is not None else "RETRYING"
                terminal = await self.db.execute(update(FinancialEventModel).where(
                    owned(FinancialEventModel, event_id, lease, active_status=active_status,
                          status_field="processing_status")
                ).values(processing_status="FAILED", error_message=message,
                    lease_owner=None, lease_expires_at=None))
                if terminal.rowcount == 1:
                    self.db.add(AuditLogModel(entity_type="WEBHOOK", entity_id=event_id,
                        action="WEBHOOK_FAILED", actor=lease.owner,
                        changes={"attempt_id": lease.attempt_id, "error": message}))
                    await self.db.commit()
                    return False, message
                await self.db.rollback()
                return False, "Webhook ownership lost"

    async def _process_event_payload(self, event_model: FinancialEventModel, payload: Dict[str, Any]):
        """
        Process webhook payload based on event type.
        """
        event_type = payload.get("event", event_model.event_type)

        if event_type == "order.paid":
            await self._process_order_paid(event_model, payload)
        elif event_type == "payment.captured":
            await self._process_payment_captured(event_model, payload)
        elif event_type == "settlement.processed":
            await self._process_settlement_processed(event_model, payload)
        elif event_type == "refund.processed":
            await self._process_refund_processed(event_model, payload)
        else:
            logger.warning(f"Unhandled event type: {event_type}")

    async def _process_order_paid(self, event_model: FinancialEventModel, payload: Dict[str, Any]):
        """Process order.paid event."""
        order_data = payload.get("payload", {}).get("order", {}).get("entity", {})
        if order_data:
            await assert_timestamps_not_closed(self.db, [order_data.get("created_at")], operation="Razorpay webhook")
            existing = (await self.db.scalars(select(RazorpayOrderModel).where(
                RazorpayOrderModel.rzp_order_id == order_data.get("id")))).first()
            if existing:
                _assert_immutable_provider_fields(existing, order_data, (
                    ("rzp_order_id", "id"), ("amount", "amount"),
                    ("created_at_ts", "created_at"),
                ))
                return
            om = RazorpayOrderModel(
                source_event_id=event_model.id,
                rzp_order_id=order_data.get("id"),
                # Razorpay may legitimately return null here. Match API-sync
                # normalization with the stable provider order ID as the
                # non-null reconciliation fallback.
                receipt=order_data.get("receipt") or order_data.get("id"),
                amount=order_data.get("amount", 0),
                amount_due=order_data.get("amount_due", 0),
                status=order_data.get("status"),
                created_at_ts=order_data.get("created_at", 0)
            )
            self.db.add(om)

    async def _process_payment_captured(self, event_model: FinancialEventModel, payload: Dict[str, Any]):
        """Process payment.captured event."""
        payment_data = payload.get("payload", {}).get("payment", {}).get("entity", {})
        if payment_data:
            await assert_timestamps_not_closed(self.db, [payment_data.get("created_at")], operation="Razorpay webhook")
            existing = (await self.db.scalars(select(RazorpayPaymentModel).where(
                RazorpayPaymentModel.rzp_payment_id == payment_data.get("id")))).first()
            if existing:
                _assert_immutable_provider_fields(existing, payment_data, (
                    ("rzp_payment_id", "id"), ("amount", "amount"),
                    ("currency", "currency"), ("created_at_ts", "created_at"),
                ))
                return
            pm = RazorpayPaymentModel(
                source_event_id=event_model.id,
                rzp_payment_id=payment_data.get("id"),
                rzp_order_id=payment_data.get("order_id"),
                amount=payment_data.get("amount", 0),
                currency=payment_data.get("currency", "INR"),
                status=payment_data.get("status"),
                created_at_ts=payment_data.get("created_at", 0)
            )
            self.db.add(pm)

    async def _process_settlement_processed(self, event_model: FinancialEventModel, payload: Dict[str, Any]):
        """Process settlement.processed event."""
        settlement_data = payload.get("payload", {}).get("settlement", {}).get("entity", {})
        if settlement_data:
            await assert_timestamps_not_closed(self.db, [settlement_data.get("created_at")], operation="Razorpay webhook")
            existing = (await self.db.scalars(select(RazorpaySettlementModel).where(
                RazorpaySettlementModel.rzp_settlement_id == settlement_data.get("id")))).first()
            if existing:
                _assert_immutable_provider_fields(existing, settlement_data, (
                    ("rzp_settlement_id", "id"), ("amount", "amount"),
                    ("created_at_ts", "created_at"),
                ))
                return
            sm = RazorpaySettlementModel(
                source_event_id=event_model.id,
                rzp_settlement_id=settlement_data.get("id"),
                amount=settlement_data.get("amount", 0),
                status=settlement_data.get("status"),
                fees=settlement_data.get("fees", 0),
                tax=settlement_data.get("tax", 0),
                created_at_ts=settlement_data.get("created_at", 0)
            )
            self.db.add(sm)

    async def _process_refund_processed(self, event_model: FinancialEventModel, payload: Dict[str, Any]):
        """Process refund.processed event."""
        refund_data = payload.get("payload", {}).get("refund", {}).get("entity", {})
        if refund_data:
            await assert_timestamps_not_closed(self.db, [refund_data.get("created_at")], operation="Razorpay webhook")
            existing = (await self.db.scalars(select(RazorpayRefundModel).where(
                RazorpayRefundModel.rzp_refund_id == refund_data.get("id")))).first()
            if existing:
                _assert_immutable_provider_fields(existing, refund_data, (
                    ("rzp_refund_id", "id"), ("rzp_payment_id", "payment_id"),
                    ("amount", "amount"), ("currency", "currency"),
                    ("created_at_ts", "created_at"),
                ))
                return
            rm = RazorpayRefundModel(
                source_event_id=event_model.id,
                rzp_refund_id=refund_data.get("id"),
                rzp_payment_id=refund_data.get("payment_id"),
                amount=refund_data.get("amount", 0),
                currency=refund_data.get("currency", "INR"),
                status=refund_data.get("status"),
                receipt=refund_data.get("receipt"),
                created_at_ts=refund_data.get("created_at", 0)
            )
            self.db.add(rm)

    async def replay_event(self, event_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Replay a FAILED webhook event.

        Returns: (success, event_uuid, error_message)
        """
        event = await self.db.get(FinancialEventModel, event_id)
        if event is None:
            return False, None, "Event not found"
        persisted_event_id = event.id
        raw_payload = event.raw_payload
        if event.attempt_count >= 5:
            return False, None, "Max retry attempts (5) exceeded"
        lease = Lease.new(self.worker_id)
        previous_attempt = event.attempt_count
        was_takeover = event.processing_status == "RETRYING"
        if not await claim(self.db, FinancialEventModel, persisted_event_id, lease,
                settings.WEBHOOK_LEASE_SECONDS, eligible_statuses={"FAILED"},
                active_status="RETRYING", status_field="processing_status",
                conditions=(FinancialEventModel.attempt_count < 5,)):
            await self.db.rollback()
            current = await self.db.get(FinancialEventModel, persisted_event_id)
            if current and current.processing_status == "PROCESSED":
                return False, str(current.id), "Event already processed"
            return False, str(persisted_event_id), "Webhook replay already in progress"
        await self.db.execute(update(FinancialEventModel).where(
            owned(FinancialEventModel, persisted_event_id, lease, active_status="RETRYING",
                  status_field="processing_status")
        ).values(attempt_count=FinancialEventModel.attempt_count + 1, error_message=None))
        self.db.add(AuditLogModel(entity_type="WEBHOOK", entity_id=persisted_event_id,
            action="WEBHOOK_REPLAY_TAKEN_OVER" if was_takeover else "WEBHOOK_REPLAY_CLAIMED",
            actor=lease.owner,
            changes={"attempt_id": lease.attempt_id,
                     "attempt_count": previous_attempt + 1}))
        await self.db.commit()
        success, error = await self._process_owned(persisted_event_id, raw_payload, lease)
        return success, str(persisted_event_id), error

    async def recover_eligible(self, worker_id, limit=None):
        limit = limit or settings.RECOVERY_BATCH_SIZE
        ids = list((await self.db.scalars(select(FinancialEventModel.id).where(or_(
            (FinancialEventModel.processing_status == "PROCESSING") &
            (or_(FinancialEventModel.lease_expires_at.is_(None),
                 FinancialEventModel.lease_expires_at <= db_now())),
            (FinancialEventModel.processing_status == "RETRYING") &
            (or_(FinancialEventModel.lease_expires_at.is_(None),
                 FinancialEventModel.lease_expires_at <= db_now())),
        )).order_by(FinancialEventModel.received_at).limit(limit))).all())
        recovered = 0
        self.worker_id = worker_id
        for event_id in ids:
            event = await self.db.get(FinancialEventModel, event_id)
            if event.processing_status == "PROCESSING":
                # Initial delivery recovery uses the same atomic expired-active
                # claim but preserves its attempt counter and status.
                lease = Lease.new(worker_id)
                won = await claim(self.db, FinancialEventModel, event_id, lease,
                    settings.WEBHOOK_LEASE_SECONDS, eligible_statuses=set(),
                    active_status="PROCESSING", status_field="processing_status")
                if won:
                    self.db.add(AuditLogModel(entity_type="WEBHOOK", entity_id=event_id,
                        action="WEBHOOK_PROCESSING_TAKEN_OVER", actor=worker_id,
                        changes={"attempt_id": lease.attempt_id}))
                    await self.db.commit()
                    success, _ = await self._process_owned(event_id, event.raw_payload, lease,
                                                           delivery_event_id=event.provider_event_id)
                    recovered += int(success)
                else:
                    await self.db.rollback()
            else:
                success, _, _ = await self.replay_event(str(event_id))
                recovered += int(success)
        return recovered

    def _verify_signature(self, body_bytes: bytes, signature: str) -> bool:
        """Verify Razorpay webhook signature."""
        if not settings.RAZORPAY_WEBHOOK_SECRET:
            raise ValueError("RAZORPAY_WEBHOOK_SECRET not configured")

        import hmac
        expected_signature = hmac.new(
            key=settings.RAZORPAY_WEBHOOK_SECRET.encode('utf-8'),
            msg=body_bytes,
            digestmod=hashlib.sha256
        ).hexdigest()

        import hmac
        return hmac.compare_digest(expected_signature, signature)
