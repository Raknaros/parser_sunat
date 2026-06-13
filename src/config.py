"""
Settings and configuration management using Pydantic BaseSettings.
All environment variables are loaded from .env file automatically.
"""
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Cloudflare R2 (S3-compatible) ──
    s3_bucket_name: str = "app-bucket"
    s3_access_key: str
    s3_secret_key: str
    s3_endpoint_url: Optional[str] = None  # Optional: for R2/MinIO. Uses AWS default if None.
    s3_region: str = "auto"  # Cloudflare R2 uses "auto". AWS would use "us-east-1"

    # ── API Security ──
    api_secret_key: str

    # ── Uvicorn Server ──
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # ── Database (PostgreSQL) ──
    db_uri: str

    # ── Pipeline Performance ──
    max_workers: int = 10

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
    }


# Singleton-like module-level instance (NOT a class Singleton, just a cached instance)
_settings: Settings | None = None


def get_settings() -> Settings:
    """Return a cached Settings instance (loads from .env on first call)."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings