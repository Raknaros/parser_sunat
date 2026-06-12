"""
API router definitions for SUNAT Parser.

Main endpoint:
- POST /api/v1/jobs/parse → accepts a parse job, returns 202 immediately
"""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, status

from src.api.dependencies import validate_api_key
from src.api.schemas import (
    ParseFilters,
    ParseJobRequest,
    JobAcceptedResponse,
)
from src.core.engine import run_pipeline

router = APIRouter(dependencies=[Depends(validate_api_key)])


def generate_job_id(filters: Optional[ParseFilters]) -> str:
    """
    Generate a human-readable, timestamped job ID.

    Format:
        scan_full_YYYYMMDD_HHMMSS                → No filters
        scan_{ruc}_YYYYMMDD_HHMMSS               → Only RUC filter
        scan_{tipo_archivo}_YYYYMMDD_HHMMSS      → Only tipo_archivo filter
        scan_{ruc}_{tipo_archivo}_YYYYMMDD_HHMMSS → Both filters

    All variants start with 'scan_' for consistent searching/grouping.
    The timestamp reflects job creation time for traceability.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if not filters:
        return f"scan_full_{timestamp}"

    parts = ["scan"]
    if filters.ruc:
        parts.append(filters.ruc)
    if filters.tipo_archivo:
        parts.append(filters.tipo_archivo)

    parts.append(timestamp)
    return "_".join(parts)


@router.post(
    "/jobs/parse",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobAcceptedResponse,
    summary="Submit a new file parsing job",
    description=(
        "Accepts a parsing job request, generates a job ID, "
        "and queues the pipeline for background execution. "
        "The caller will be notified via webhook upon completion."
    ),
)
async def create_parse_job(
    request: ParseJobRequest,
    background_tasks: BackgroundTasks,
):
    """
    Submit a new SUNAT file parsing job.

    The actual processing (S3 download, parsing, DB insertion) runs
    in a background task to avoid HTTP timeouts.

    Args:
        request: Job request with optional webhook URL and optional filters
        background_tasks: FastAPI BackgroundTasks injection

    Returns:
        JobAcceptedResponse with status='processing' and a generated job_id
    """
    # Generate the job ID based on filters
    job_id = generate_job_id(request.filters)

    # Build the full request data dict for the pipeline engine
    # The 'unparsing/' prefix is an internal constant, not user-configurable
    request_data = {
        "job_id": job_id,
        "prefix": "unparsing/",
        "webhook_url": str(request.webhook_url) if request.webhook_url else None,
        "filters": request.filters.model_dump() if request.filters else None,
        "job_metadata": request.job_metadata,
    }

    # Queue the pipeline in background tasks
    background_tasks.add_task(run_pipeline, request_data)

    return JobAcceptedResponse(
        status="processing",
        job_id=job_id,
    )
