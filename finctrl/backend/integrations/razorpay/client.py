"""Bounded, read-only Razorpay SDK adapter."""
import time
from typing import Callable
import razorpay
from finctrl.backend.config import settings
from finctrl.backend.integrations.razorpay.schemas import RazorpayEvidence


class RazorpayConnectorError(RuntimeError):
    def __init__(self, message, *, status_code=None, transient=False, category="provider_error"):
        super().__init__(message)
        self.status_code, self.transient, self.category = status_code, transient, category


class RazorpayMalformedResponse(RazorpayConnectorError):
    pass


class RazorpayClient:
    MAX_PAGE_SIZE, MAX_PAGES = 100, 10_000

    def __init__(self, sdk_client=None, *, page_size=100, max_retries=3,
                 backoff_seconds=0.25, sleep: Callable[[float], None] = time.sleep):
        if not 1 <= page_size <= self.MAX_PAGE_SIZE:
            raise ValueError("page_size must be between 1 and 100")
        if not 0 <= max_retries <= 5:
            raise ValueError("max_retries must be between 0 and 5")
        self.mode, self.page_size, self.max_retries = settings.RAZORPAY_MODE, page_size, max_retries
        self.backoff_seconds, self._sleep = backoff_seconds, sleep
        self.client = sdk_client
        if self.client is None and settings.RAZORPAY_KEY_ID and settings.RAZORPAY_KEY_SECRET:
            self.client = razorpay.Client(auth=(settings.RAZORPAY_KEY_ID, settings.RAZORPAY_KEY_SECRET))

    @staticmethod
    def _status(error):
        value = getattr(error, "status_code", getattr(error, "code", None))
        return value if isinstance(value, int) else None

    def _read(self, operation, *args):
        self._require_client()
        for attempt in range(self.max_retries + 1):
            try:
                return operation(*args)
            except Exception as error:
                status = self._status(error)
                transient = isinstance(error, (ConnectionError, TimeoutError)) or status == 429 or bool(status and status >= 500)
                if not transient or attempt == self.max_retries:
                    category = self._error_category(error, status)
                    status_suffix = f" (HTTP {status})" if status is not None else ""
                    raise RazorpayConnectorError(
                        f"Razorpay read failed: {category}{status_suffix}",
                        status_code=status, transient=transient, category=category,
                    ) from error
                self._sleep(self.backoff_seconds * (2 ** attempt))

    def _require_client(self):
        if self.client is None:
            raise RazorpayConnectorError(
                "Razorpay client is not configured", status_code=401,
                transient=False, category="authentication",
            )

    @staticmethod
    def _error_category(error, status):
        if isinstance(error, TimeoutError): return "timeout"
        if isinstance(error, ConnectionError): return "connection"
        if status in (401, 403): return "authentication"
        if status == 429: return "rate_limit"
        if status is not None and status >= 500: return "provider_server"
        if status is not None and status >= 400: return "provider_client"
        return "unexpected_provider_error"

    def _pages(self, operation, *, from_ts=None, to_ts=None):
        seen, skip = set(), 0
        for _ in range(self.MAX_PAGES):
            params = {"count": self.page_size, "skip": skip}
            if from_ts is not None: params["from"] = from_ts
            if to_ts is not None: params["to"] = to_ts
            response = self._read(operation, params)
            if not isinstance(response, dict) or not isinstance(response.get("items", []), list):
                raise RazorpayMalformedResponse("Razorpay collection response must contain an items list")
            items = response.get("items", [])
            if not items: return
            progress = 0
            for item in items:
                if not isinstance(item, dict) or not item.get("id"):
                    raise RazorpayMalformedResponse("Razorpay item is missing a stable id")
                if item["id"] not in seen:
                    seen.add(item["id"]); progress += 1; yield item
            if len(items) < self.page_size: return
            if not progress:
                raise RazorpayMalformedResponse("Razorpay pagination repeated without progress")
            skip += len(items)
        raise RazorpayMalformedResponse("Razorpay pagination exceeded safety bound")

    def _collection(self, resource, from_ts=None, to_ts=None):
        self._require_client()
        return list(self._pages(resource, from_ts=from_ts, to_ts=to_ts))

    @staticmethod
    def _collection_items(response):
        if not isinstance(response, dict) or not isinstance(response.get("items", []), list):
            raise RazorpayMalformedResponse("Razorpay collection response must contain an items list")
        seen, result = set(), []
        for item in response.get("items", []):
            if not isinstance(item, dict) or not item.get("id"):
                raise RazorpayMalformedResponse("Razorpay item is missing a stable id")
            if item["id"] not in seen:
                seen.add(item["id"]); result.append(item)
        return result

    def fetch_orders(self, *, from_ts=None, to_ts=None):
        return self._collection(self.client.order.all if self.client else None, from_ts, to_ts)
    def fetch_payments(self, *, from_ts=None, to_ts=None):
        return self._collection(self.client.payment.all if self.client else None, from_ts, to_ts)
    def fetch_order_payments(self, order_id, *, from_ts=None, to_ts=None):
        self._require_client()
        if from_ts is not None or to_ts is not None:
            raise ValueError("Razorpay order-payments does not support from/to filters")
        return self._collection_items(self._read(self.client.order.payments, order_id))
    def fetch_refunds(self, *, from_ts=None, to_ts=None):
        return self._collection(self.client.refund.all if self.client else None, from_ts, to_ts)
    def fetch_settlements(self, *, from_ts=None, to_ts=None):
        return self._collection(self.client.settlement.all if self.client else None, from_ts, to_ts)

    def fetch_order(self, object_id):
        return self._read(self.client.order.fetch, object_id) if self.client else None
    def fetch_payment(self, object_id):
        return self._normalize_payment(self._read(self.client.payment.fetch, object_id)) if self.client else None
    def fetch_settlement(self, object_id):
        return self._read(self.client.settlement.fetch, object_id) if self.client else None
    def fetch_refund(self, object_id):
        return self._read(self.client.refund.fetch, object_id) if self.client else None

    @staticmethod
    def _normalize_payment(raw):
        fee, tax = raw.get("fee", 0) or 0, raw.get("tax", 0) or 0
        return RazorpayEvidence(payment_id=raw.get("id", ""), order_id=raw.get("order_id"),
            settlement_id=raw.get("settlement_id"), amount=raw.get("amount", 0), fee=fee, tax=tax,
            net_amount=raw.get("amount", 0)-fee-tax, currency=raw.get("currency", "INR"),
            status=raw.get("status", ""), created_at=raw.get("created_at", 0),
            metadata={"method": raw.get("method"), "email": raw.get("email"), "contact": raw.get("contact"), "notes": raw.get("notes", {})})


razorpay_client = RazorpayClient()
