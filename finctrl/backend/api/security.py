"""API Security - X-API-Key Authentication with RBAC."""
import secrets
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from typing import Literal
from finctrl.backend.config import settings

# Define API key header
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

Role = Literal["ADMIN", "READ_ONLY"]


async def verify_api_key(api_key: str = Security(api_key_header)) -> Role:
    """
    Verify X-API-Key and return the role.

    Raises HTTPException if the key is invalid or missing.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header"
        )

    if settings.ADMIN_API_KEY and secrets.compare_digest(api_key, settings.ADMIN_API_KEY):
        return "ADMIN"
    elif settings.READ_ONLY_API_KEY and secrets.compare_digest(api_key, settings.READ_ONLY_API_KEY):
        return "READ_ONLY"
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid X-API-Key"
        )


async def require_admin(role: Role = Security(verify_api_key)) -> Role:
    """
    Dependency that requires ADMIN role.
    """
    if role != "ADMIN":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="ADMIN role required"
        )
    return role


async def require_read_only(role: Role = Security(verify_api_key)) -> Role:
    """
    Dependency that requires at least READ_ONLY role (ADMIN also allowed).
    """
    # Both ADMIN and READ_ONLY can read
    return role
