"""
Pydantic models for API request/response schemas.
"""
from datetime import datetime
from typing import Any, Dict, Optional, Literal

from pydantic import BaseModel, AnyHttpUrl


# ── Allowed document type values ───────────────────────────────────────────

TIPO_ARCHIVO_VALUES = Literal[
    "factura", "boleta", "nota_credito", "nota_debito",
    "guia_remision", "sire_ventas", "sire_compras", "planilla"
]


# ── Request Schemas ────────────────────────────────────────────────────────

class ParseFilters(BaseModel):
    """Optional filters to narrow down which files to process."""
    ruc: Optional[str] = None
    tipo_archivo: Optional[TIPO_ARCHIVO_VALUES] = None


class ParseJobRequest(BaseModel):
    """
    Request body for POST /api/v1/jobs/parse.

    The prefix is not configurable — all files are scanned from the
    'unparsed/' S3 prefix (configured internally).

    Attributes:
        webhook_url: URL to POST job completion notification.
                     Can be None for testing without an orchestrator.
        filters: Optional filters (RUC and/or file type).
        job_metadata: Arbitrary metadata from the orchestrator (passed through).
    """
    webhook_url: Optional[AnyHttpUrl] = None
    filters: Optional[ParseFilters] = None
    job_metadata: Optional[Dict[str, Any]] = None


# ── Response Schemas ───────────────────────────────────────────────────────

class JobAcceptedResponse(BaseModel):
    """
    Immediate response returned when a job is accepted (HTTP 202).

    The actual processing happens asynchronously via BackgroundTasks.
    """
    status: str = "processing"
    job_id: str
    message: str = "Job accepted. You will be notified via webhook upon completion."


class ErrorResponse(BaseModel):
    """Standard error response."""
    detail: str