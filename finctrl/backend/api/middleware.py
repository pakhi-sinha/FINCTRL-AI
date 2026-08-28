from uuid import uuid4
from fastapi import Request
from finctrl.backend.logger import correlation_id_ctx, logger

async def correlation_id_middleware(request: Request, call_next):
    # Retrieve existing correlation ID from headers or generate a new one
    corr_id = request.headers.get("X-Correlation-ID")
    if not corr_id:
        corr_id = str(uuid4())

    # Set it in the context variable
    token = correlation_id_ctx.set(corr_id)

    # Log incoming request
    logger.info(f"Incoming request: {request.method} {request.url.path}")

    try:
        response = await call_next(request)

        # Add the correlation ID to the response headers
        response.headers["X-Correlation-ID"] = corr_id

        return response
    finally:
        # Reset the context variable
        correlation_id_ctx.reset(token)
