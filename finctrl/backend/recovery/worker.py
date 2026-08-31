"""Dedicated bounded recovery worker for durable in-flight operations."""
from __future__ import annotations

import asyncio
import logging
import os
import socket
from uuid import uuid4

from finctrl.backend.config import settings
from finctrl.backend.database.database import async_session_maker
from finctrl.backend.integrations.webhook_processor import WebhookProcessor
from finctrl.backend.reconciliation.investigation import InvestigationService
from finctrl.backend.reconciliation.run_control import ReconciliationRunService

logger = logging.getLogger(__name__)


class RecoveryWorker:
    def __init__(self, worker_id=None, session_factory=async_session_maker):
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid4()}"
        self.session_factory = session_factory

    async def scan_once(self):
        counts = {"reconciliation": 0, "investigation": 0, "webhook": 0}
        try:
            counts["reconciliation"] = await ReconciliationRunService(
                session_factory=self.session_factory).recover_eligible(self.worker_id)
            async with self.session_factory() as db:
                counts["investigation"] = await InvestigationService(
                    db, session_factory=self.session_factory,
                    worker_id=self.worker_id).recover_eligible(self.worker_id)
            async with self.session_factory() as db:
                counts["webhook"] = await WebhookProcessor(
                    db, session_factory=self.session_factory,
                    worker_id=self.worker_id).recover_eligible(self.worker_id)
        except Exception as error:
            logger.error("Recovery scan failed", extra={"recovery": {
                "worker_id": self.worker_id, "error": f"{type(error).__name__}: recovery scan failed"}})
        else:
            logger.info("Recovery scan completed", extra={"recovery": {
                "worker_id": self.worker_id, **counts}})
        return counts

    async def run(self, stop_event=None):
        stop_event = stop_event or asyncio.Event()
        while not stop_event.is_set():
            await self.scan_once()  # initial scan occurs before the first wait
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=settings.RECOVERY_POLL_SECONDS)
            except asyncio.TimeoutError:
                pass


def main():
    asyncio.run(RecoveryWorker().run())


if __name__ == "__main__":
    main()
