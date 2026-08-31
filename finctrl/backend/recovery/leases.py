"""Database-time lease primitives used as the ownership authority.

Production lease decisions use PostgreSQL CURRENT_TIMESTAMP. SQLite support is
kept only for deterministic isolated tests; host wall-clock time is never used
to decide claim, heartbeat, expiry, or takeover eligibility.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import and_, func, or_, update


@dataclass(frozen=True)
class Lease:
    owner: str
    attempt_id: str

    @classmethod
    def new(cls, worker_id: str):
        attempt = str(uuid4())
        return cls(owner=worker_id, attempt_id=attempt)


def db_now():
    return func.current_timestamp()


def db_expiry(session, seconds: int):
    """Construct expiry from database time for the active SQL dialect."""
    if session.bind.dialect.name == "sqlite":
        return func.datetime(func.current_timestamp(), f"+{int(seconds)} seconds")
    return func.current_timestamp() + func.make_interval(0, 0, 0, 0, 0, 0, int(seconds))


async def claim(session, model, entity_id, lease: Lease, lease_seconds: int,
                *, eligible_statuses, active_status: str, allow_expired_active=True,
                status_field="status", conditions=()):
    status = getattr(model, status_field)
    eligible = status.in_(eligible_statuses)
    if allow_expired_active:
        eligible = or_(eligible, and_(status == active_status, or_(
            model.lease_owner.is_(None), model.lease_expires_at.is_(None),
            model.lease_expires_at <= db_now())))
    result = await session.execute(update(model).where(
        model.id == entity_id,
        *conditions,
        eligible,
        or_(model.lease_owner.is_(None), model.lease_expires_at.is_(None),
            model.lease_expires_at <= db_now()),
    ).values(**{status_field: active_status}, lease_owner=lease.owner,
        execution_attempt_id=lease.attempt_id, heartbeat_at=db_now(),
        lease_expires_at=db_expiry(session, lease_seconds)))
    return result.rowcount == 1


async def heartbeat(session, model, entity_id, lease: Lease, lease_seconds: int,
                    *, active_status: str, status_field="status"):
    status = getattr(model, status_field)
    result = await session.execute(update(model).where(
        model.id == entity_id, status == active_status,
        model.lease_owner == lease.owner,
        model.execution_attempt_id == lease.attempt_id,
        model.lease_expires_at > db_now(),
    ).values(heartbeat_at=db_now(), lease_expires_at=db_expiry(session, lease_seconds)))
    return result.rowcount == 1


def owned(model, entity_id, lease: Lease, *, active_status: str, status_field="status"):
    status = getattr(model, status_field)
    return and_(model.id == entity_id, status == active_status,
        model.lease_owner == lease.owner,
        model.execution_attempt_id == lease.attempt_id,
        model.lease_expires_at > db_now())


@asynccontextmanager
async def heartbeat_loop(session_factory, model, entity_id, lease, lease_seconds,
                         heartbeat_seconds, active_status, status_field="status"):
    """Heartbeat only while the caller's operation is actively executing."""
    stopped = asyncio.Event()
    ownership_lost = asyncio.Event()

    async def run():
        while True:
            try:
                await asyncio.wait_for(stopped.wait(), timeout=heartbeat_seconds)
                return
            except asyncio.TimeoutError:
                async with session_factory() as session:
                    if not await heartbeat(session, model, entity_id, lease, lease_seconds,
                                           active_status=active_status,
                                           status_field=status_field):
                        await session.rollback()
                        ownership_lost.set()
                        return
                    await session.commit()

    task = asyncio.create_task(run())
    try:
        yield ownership_lost
    finally:
        stopped.set()
        await task
