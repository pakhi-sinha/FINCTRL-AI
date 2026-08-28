from fastapi import Security, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
from finctrl.backend.config import settings
from enum import Enum

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

class Role(str, Enum):
    ADMIN = "admin"
    READ_ONLY = "read_only"

def get_current_role(api_key: str = Security(api_key_header)) -> Role:
    if settings.APP_MODE == "test" and not settings.ADMIN_API_KEY and not settings.READ_ONLY_API_KEY:
        # If in test mode and no keys are configured, allow mock auth to not break old tests.
        return Role.ADMIN

    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API Key",
        )

    if settings.ADMIN_API_KEY and api_key == settings.ADMIN_API_KEY:
        return Role.ADMIN
    elif settings.READ_ONLY_API_KEY and api_key == settings.READ_ONLY_API_KEY:
        return Role.READ_ONLY
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
        )

def require_admin(role: Role = Security(get_current_role)):
    if role != Role.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )
    return role

def require_read(role: Role = Security(get_current_role)):
    # Both ADMIN and READ_ONLY can read
    return role
