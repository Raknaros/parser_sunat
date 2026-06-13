"""
FastAPI ASGI entry point for the SUNAT Parser API.

This is the new cloud-native API entry point (Phase 1).
The legacy CLI has been moved to src/legacy/cli.py.

Usage:
    uvicorn src.main:app --reload
    # or
    python -m src.main
"""
import logging
import uvicorn
from fastapi import FastAPI

from src.api.routers import router
from src.config import get_settings
from src.utils.logger import configure_root_logger

# ── Application Definition ─────────────────────────────────────────────────

app = FastAPI(
    title="SUNAT Parser API",
    description=(
        "Cloud-native ELT pipeline for processing SUNAT electronic documents "
        "(invoices, credit notes, SIRE books, payroll). "
        "Accepts parsing jobs via REST API and processes them asynchronously."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


# ── Public Endpoints (no authentication required) ──────────────────────────


@app.get("/health", status_code=200)
async def health_check():
    """Simple health check endpoint (public, no API key required)."""
    return {"status": "healthy"}


# ── Router Registration (authenticated endpoints) ─────────────────────────

app.include_router(router, prefix="/api/v1")


# ── Startup / Shutdown Events ─────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    """Configure logging and log application startup."""
    # Configure root logger so all logging.getLogger(__name__) calls
    # are visible in Docker logs via stderr
    configure_root_logger()
    settings = get_settings()
    logging.getLogger(__name__).info(
        "Starting on %s:%s...", settings.api_host, settings.api_port
    )


@app.on_event("shutdown")
async def shutdown_event():
    """Log application shutdown."""
    print("[SUNAT Parser API] Shutting down...")


# ── Direct Execution ──────────────────────────────────────────────────────

if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "src.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
    )