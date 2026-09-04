"""
FastAPI middleware for request correlation IDs and logging
"""
import uuid
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
import time
import logging

logger = logging.getLogger("finctrl.middleware")
MAX_CORRELATION_ID_LENGTH = 128


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """
    Middleware that generates a correlation ID for each request
    and includes it in response headers and logs.
    """

    def __init__(self, app: ASGIApp):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        # Generate or extract correlation ID
        supplied = request.headers.get("X-Correlation-ID")
        correlation_id = supplied if supplied and len(supplied) <= MAX_CORRELATION_ID_LENGTH and supplied.isprintable() else str(uuid.uuid4())

        # Store in request state
        request.state.correlation_id = correlation_id

        # Log request
        start_time = time.time()

        logger.info(
            f"Request started: {request.method} {request.url.path}",
            extra={"correlation_id": correlation_id, "method": request.method, "path": request.url.path}
        )

        # Process request
        response = await call_next(request)

        # Add correlation ID to response headers
        response.headers["X-Correlation-ID"] = correlation_id

        # Log response
        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            f"Request completed: {request.method} {request.url.path} - {response.status_code}",
            extra={
                "correlation_id": correlation_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": round(duration_ms, 2)
            }
        )

        return response
