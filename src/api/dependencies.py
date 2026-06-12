"""
API authentication dependencies.

Validates the X-API-Key header against the configured API_SECRET_KEY.
"""
from fastapi import Header, HTTPException, status

from src.config import get_settings


async def validate_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> None:
    """
    Dependency that validates the API key from the request header.

    Usage:
        @router.post("/jobs/parse", dependencies=[Depends(validate_api_key)])

    Args:
        x_api_key: The value of the X-API-Key header (required)

    Raises:
        HTTPException 401: If the API key is missing or invalid
    """
    settings = get_settings()

    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )

    if x_api_key != settings.api_secret_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )