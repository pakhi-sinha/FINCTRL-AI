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
from sqlalchemy import select
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

logger = logging.getLogger(__name__)


class WebhookProcessor:
    """
    Handles webhook processing with idempotency guarantees.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

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

        # Create event model
        event_model = FinancialEventModel(
            id=financial_event_id("razorpay", canonical_event_id),
            provider="razorpay",
            provider_event_id=canonical_event_id,
            event_type=event_type,
            payload_hash=payload_hash,
            raw_payload=payload,
            processing_status="PROCESSING",
            attempt_count=1
        )

        try:
            # Add to session and flush to get ID
            self.db.add(event_model)
            await self.db.flush()

            # Try to process the webhook
            await self._process_event_payload(event_model, payload)
            await self._record_delivery(event_model, event_id)

            # Success - commit the transaction
            event_model.processing_status = "PROCESSED"
            event_model.processed_at = datetime.utcnow()
            await self.db.commit()

            logger.info(
                f"Webhook processed successfully: event_id={event_id}, type={event_type}",
                extra={"event_id": event_id, "event_type": event_type, "event_uuid": str(event_model.id)}
            )

            return False, str(event_model.id), None

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
            else:
                # This shouldn't happen with unique constraint, but handle gracefully
                logger.error(
                    f"Integrity error without successful processing: event_id={event_id}",
                    extra={"event_id": event_id}
                )
                return False, None, "Concurrent processing conflict"

        except Exception as e:
            # Processing failed - mark as FAILED
            await self.db.rollback()
            logger.error(
                f"Webhook processing failed: event_id={event_id}, error={str(e)}",
                extra={"event_id": event_id, "error": str(e)}
            )

            # Create failed event
            failed_event = FinancialEventModel(
                id=financial_event_id("razorpay", canonical_event_id),
                provider="razorpay",
                provider_event_id=canonical_event_id,
                event_type=event_type,
                payload_hash=payload_hash,
                raw_payload=payload,
                processing_status="FAILED",
                attempt_count=1,
                error_message=str(e)
            )
            self.db.add(failed_event)
            await self.db.commit()

            return False, str(failed_event.id), str(e)

    async def _process_event_payload(self, event_model: FinancialEventModel, payload: Dict[str, Any]):
        """
        Process webhook payload based on event type.
        """
        event_type = event_model.event_type

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
            existing = (await self.db.scalars(select(RazorpayOrderModel).where(
                RazorpayOrderModel.rzp_order_id == order_data.get("id")))).first()
            if existing:
                return
            om = RazorpayOrderModel(
                source_event_id=event_model.id,
                rzp_order_id=order_data.get("id"),
                receipt=order_data.get("receipt"),
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
            existing = (await self.db.scalars(select(RazorpayPaymentModel).where(
                RazorpayPaymentModel.rzp_payment_id == payment_data.get("id")))).first()
            if existing:
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
            existing = (await self.db.scalars(select(RazorpaySettlementModel).where(
                RazorpaySettlementModel.rzp_settlement_id == settlement_data.get("id")))).first()
            if existing:
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
            existing = (await self.db.scalars(select(RazorpayRefundModel).where(
                RazorpayRefundModel.rzp_refund_id == refund_data.get("id")))).first()
            if existing:
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
        # Get the failed event
        event = await self.db.execute(
            select(FinancialEventModel).filter_by(id=event_id)
        )
        event = event.scalar_one_or_none()

        if not event:
            return False, None, "Event not found"

        if event.processing_status != "FAILED":
            return False, None, "Only FAILED events can be replayed"

        if event.attempt_count >= 5:
            return False, None, "Max retry attempts (5) exceeded"

        persisted_event_id = event.id
        provider_event_id = event.provider_event_id
        raw_payload = event.raw_payload

        # Increment attempt count
        next_attempt = event.attempt_count + 1
        event.attempt_count = next_attempt
        event.processing_status = "RETRYING"
        event.error_message = None

        # Create audit log
        audit_log = AuditLogModel(
            entity_type="WEBHOOK",
            entity_id=event_id,
            action="REPLAY_ATTEMPT",
            actor="ADMIN",
            changes={
                "attempt_count": event.attempt_count,
                "previous_status": "FAILED",
                "new_status": "RETRYING"
            }
        )
        self.db.add(audit_log)

        try:
            # Re-process the payload
            await self._process_event_payload(event, raw_payload)

            # Success
            event.processing_status = "PROCESSED"
            event.processed_at = datetime.utcnow()
            await self.db.commit()

            logger.info(
                f"Webhook replay successful: event_id={event.provider_event_id}, attempt={event.attempt_count}",
                extra={
                    "event_id": event.provider_event_id,
                    "event_uuid": str(event.id),
                    "attempt_count": event.attempt_count
                }
            )

            return True, str(event.id), None

        except Exception as e:
            await self.db.rollback()
            event = await self.db.scalar(
                select(FinancialEventModel).where(
                    FinancialEventModel.id == persisted_event_id
                )
            )
            if event is None:
                return False, str(persisted_event_id), str(e)
            event.processing_status = "FAILED"
            event.attempt_count = next_attempt
            event.error_message = str(e)
            await self.db.commit()

            logger.error(
                f"Webhook replay failed: event_id={provider_event_id}, attempt={event.attempt_count}, error={str(e)}",
                extra={
                    "event_id": provider_event_id,
                    "event_uuid": str(persisted_event_id),
                    "attempt_count": event.attempt_count,
                    "error": str(e)
                }
            )

            return False, str(persisted_event_id), str(e)

    def _verify_signature(self, body_bytes: bytes, signature: str) -> bool:
        """Verify Razorpay webhook signature."""
        if not settings.RAZORPAY_KEY_SECRET:
            raise ValueError("RAZORPAY_KEY_SECRET not configured")

        import hmac
        expected_signature = hmac.new(
            key=settings.RAZORPAY_KEY_SECRET.encode('utf-8'),
            msg=body_bytes,
            digestmod=hashlib.sha256
        ).hexdigest()

        import hmac
        return hmac.compare_digest(expected_signature, signature)
