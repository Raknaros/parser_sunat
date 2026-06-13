"""
Pipeline engine for the ELT process.

Orchestrates the full pipeline:
1. List files from S3 (prefix is always 'unparsed/')
2. Filter by regex rules (DOCUMENT_RULES) and request filters
3. Download and process files in parallel (ThreadPoolExecutor)
4. Bulk insert results into PostgreSQL
5. Move files to parsed/{job_id}/ or failed/{job_id}/ in S3
6. Upload audit CSV to audit/{job_id}.csv
7. Send webhook notification (if webhook_url is provided)
"""
import asyncio
import csv
import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from src.config import get_settings
from src.core.document_rules import identify_document_type
from src.core.notifier import send_webhook
from src.core.processor_registry import (
    get_processor,
    get_rule_keys_for_tipo,
    has_processor,
)
from src.storage.s3_storage import S3Storage
from src.utils.db_manager import DatabaseManager


def run_pipeline(request_data: dict) -> None:
    """
    Execute the full ELT pipeline for a given job request.

    Args:
        request_data: Dictionary with keys:
            - job_id: str
            - prefix: str (S3 prefix to scan, typically "unparsed/")
            - webhook_url: Optional[str]
            - filters: Optional[dict] with 'ruc' and/or 'tipo_archivo'
            - job_metadata: Optional[dict]
    """
    logger = logging.getLogger(__name__)
    try:
        _run_pipeline_internal(request_data)
    except Exception as e:
        logger.error(
            f"[{request_data.get('job_id', 'unknown')}] "
            f"Pipeline crashed with unhandled exception: {e}",
            exc_info=True,
        )


def _run_pipeline_internal(request_data: dict) -> None:
    """
    Internal pipeline execution (wrapped by run_pipeline for error logging).

    Args:
        request_data: Dictionary with keys:
            - job_id: str
            - prefix: str (S3 prefix to scan, typically "unparsed/")
            - webhook_url: Optional[str]
            - filters: Optional[dict] with 'ruc' and/or 'tipo_archivo'
            - job_metadata: Optional[dict]
    """
    logger = logging.getLogger(__name__)
    settings = get_settings()
    start_time = time.time()

    job_id = request_data.get("job_id", "unknown")
    prefix = request_data.get("prefix", "unparsed/")
    webhook_url: Optional[str] = request_data.get("webhook_url")
    filters: Optional[dict] = request_data.get("filters")
    job_metadata: Optional[dict] = request_data.get("job_metadata")

    logger.info(f"[{job_id}] Pipeline started. Prefix: '{prefix}'")

    # ── Initialize S3 and DB ─────────────────────────────────────────────
    try:
        s3 = S3Storage(
            bucket_name=settings.s3_bucket_name,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.s3_region,
            logger=logger,
        )
    except Exception as e:
        logger.error(f"[{job_id}] Failed to initialize S3Storage: {e}", exc_info=True)
        return

    db = DatabaseManager(db_uri=settings.db_uri, logger=logger)
    try:
        db.connect()
    except ConnectionError:
        logger.error(f"[{job_id}] Database connection failed. Aborting pipeline.")
        return

    # ── 1. List all keys ─────────────────────────────────────────────────
    all_keys = s3.list_keys(prefix)
    total_files_scanned = len(all_keys)
    logger.info(f"[{job_id}] Listed {total_files_scanned} files under '{prefix}'.")

    # ── 2. Filter keys in memory (no downloads yet) ──────────────────────
    matched_files: List[Tuple[str, str]] = []  # (s3_key, doc_type)
    ignored_unknown = 0

    for key in all_keys:
        # Extract filename from S3 key
        filename = key.split("/")[-1]
        if not filename:
            continue

        doc_type = identify_document_type(filename)

        # Unknown type → skip (don't move, don't process)
        if doc_type == "desconocido":
            ignored_unknown += 1
            continue

        # No processor registered → skip (recibo, declaraciones, etc.)
        if not has_processor(doc_type):
            ignored_unknown += 1
            continue

        # Apply filters if present
        if filters:
            ruc_filter = filters.get("ruc")
            tipo_filter = filters.get("tipo_archivo")

            if ruc_filter:
                # Check if the regex match contains a RUC group
                from src.core.document_rules import DOCUMENT_RULES
                pattern, group_names = DOCUMENT_RULES.get(doc_type, (None, []))
                if pattern:
                    match = pattern.match(filename)
                    if match and "ruc" in group_names:
                        ruc_idx = group_names.index("ruc") + 1
                        extracted_ruc = match.group(ruc_idx)
                        if extracted_ruc != ruc_filter:
                            continue  # RUC doesn't match → skip
                    else:
                        # No RUC group in regex → can't filter by RUC
                        continue

            if tipo_filter:
                expected_rule_key = get_rule_keys_for_tipo(tipo_filter)
                if expected_rule_key and doc_type != expected_rule_key:
                    continue  # Tipo doesn't match → skip

        matched_files.append((key, doc_type))

    total_files_matched = len(matched_files)
    logger.info(
        f"[{job_id}] Filtering complete: "
        f"{total_files_matched} matched, {ignored_unknown} ignored (unknown type)."
    )

    # If nothing matched after filtering, finish early
    if not matched_files:
        logger.info(f"[{job_id}] No files matched the filters. Pipeline finished.")
        _send_webhook_if_needed(
            webhook_url, job_id, job_metadata, start_time,
            total_files_scanned, 0, 0, 0, ignored_unknown, [], []
        )
        return

    # ── 3. Process files in parallel ─────────────────────────────────────
    max_workers = settings.max_workers
    successful_keys: List[str] = []
    failed_keys: List[str] = []
    audit_rows: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {}

        for s3_key, doc_type in matched_files:
            future = executor.submit(
                _process_single_file, s3, db, doc_type, s3_key, logger
            )
            future_to_key[future] = (s3_key, doc_type)

        for future in as_completed(future_to_key):
            s3_key, doc_type = future_to_key[future]
            try:
                result = future.result()
                if result["status"] == "parsed":
                    successful_keys.append(s3_key)
                else:
                    failed_keys.append(s3_key)
                audit_rows.append(result)
            except Exception as e:
                logger.error(f"[{job_id}] Unexpected error processing '{s3_key}': {e}")
                failed_keys.append(s3_key)
                audit_rows.append({
                    "s3_key": s3_key,
                    "status": "failed",
                    "doc_type": doc_type,
                    "error": str(e),
                    "rows_inserted": 0,
                    "duration_ms": 0,
                })

    successfully_parsed = len(successful_keys)
    failed = len(failed_keys)

    # ── 4. Move files in S3 ──────────────────────────────────────────────
    for s3_key in successful_keys:
        dest_key = s3_key.replace("unparsed/", f"parsed/{job_id}/", 1)
        s3.move_object(s3_key, dest_key)

    for s3_key in failed_keys:
        dest_key = s3_key.replace("unparsed/", f"failed/{job_id}/", 1)
        s3.move_object(s3_key, dest_key)

    logger.info(
        f"[{job_id}] Files moved: {successfully_parsed} to parsed/, "
        f"{failed} to failed/."
    )

    # ── 5. Upload audit CSV ──────────────────────────────────────────────
    audit_csv_content = _build_audit_csv(audit_rows)
    audit_key = f"audit/{job_id}.csv"
    s3.put_bytes(audit_key, audit_csv_content.encode("utf-8"))
    logger.info(f"[{job_id}] Audit CSV uploaded to '{audit_key}'.")

    # ── 6. Send webhook (if configured) ──────────────────────────────────
    _send_webhook_if_needed(
        webhook_url, job_id, job_metadata, start_time,
        total_files_scanned, total_files_matched,
        successfully_parsed, failed, ignored_unknown,
        successful_keys, failed_keys,
    )

    logger.info(f"[{job_id}] Pipeline completed.")


# ── Internal helpers ───────────────────────────────────────────────────────


def _process_single_file(
    s3: S3Storage,
    db: DatabaseManager,
    doc_type: str,
    s3_key: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Download, parse, and insert a single file.

    Returns:
        Dict with keys: s3_key, status, doc_type, error, rows_inserted, duration_ms
    """
    filename = s3_key.split("/")[-1]
    start = time.time()
    result: Dict[str, Any] = {
        "s3_key": s3_key,
        "status": "failed",
        "doc_type": doc_type,
        "error": "",
        "rows_inserted": 0,
        "duration_ms": 0,
    }

    try:
        # Download
        file_bytes = s3.get_bytes(s3_key)
        if file_bytes is None:
            result["error"] = "Failed to download from S3"
            result["duration_ms"] = int((time.time() - start) * 1000)
            return result

        # Get processor
        processor_cls = get_processor(doc_type)
        if processor_cls is None:
            result["error"] = f"No processor for document type '{doc_type}'"
            result["duration_ms"] = int((time.time() - start) * 1000)
            return result

        processor = processor_cls(logger)

        # Parse
        dataframes = processor.process_content(filename, file_bytes)
        if dataframes is None:
            result["error"] = "Processor returned None (parsing failed)"
            result["duration_ms"] = int((time.time() - start) * 1000)
            return result

        # Insert into DB
        db_mapping = processor.get_db_mapping()
        total_rows = 0
        for table_key, df in dataframes.items():
            if df.empty:
                continue
            mapping = db_mapping.get(table_key)
            if mapping is None:
                continue
            db.insert_dataframe(
                df=df,
                schema=mapping["schema"],
                table=mapping["table"],
                column_mapping=mapping["columns"],
            )
            total_rows += len(df)

        result["status"] = "parsed"
        result["rows_inserted"] = total_rows

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Error processing '{s3_key}': {e}", exc_info=True)

    result["duration_ms"] = int((time.time() - start) * 1000)
    return result


def _build_audit_csv(rows: List[Dict[str, Any]]) -> str:
    """Build a CSV string from audit rows."""
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["s3_key", "status", "doc_type", "error", "rows_inserted", "duration_ms"],
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def _send_webhook_if_needed(
    webhook_url: Optional[str],
    job_id: str,
    job_metadata: Optional[dict],
    start_time: float,
    total_scanned: int,
    total_matched: int,
    success: int,
    failed: int,
    ignored: int,
    successful_keys: List[str],
    failed_keys: List[str],
) -> None:
    """Send webhook notification if a webhook_url was configured."""
    if not webhook_url:
        return

    duration = round(time.time() - start_time, 2)

    payload = {
        "status": "completed",
        "job_id": job_id,
        "job_metadata": job_metadata,
        "metrics": {
            "total_files_scanned": total_scanned,
            "total_files_matched_filters": total_matched,
            "successfully_parsed": success,
            "failed": failed,
            "ignored_unknown_type": ignored,
            "duration_seconds": duration,
        },
        "details": {
            "successful_keys": [
                k.replace("unparsed/", f"parsed/{job_id}/", 1)
                for k in successful_keys
            ],
            "failed_keys": [
                k.replace("unparsed/", f"failed/{job_id}/", 1)
                for k in failed_keys
            ],
        },
    }

    try:
        # Since we're in a synchronous context (BackgroundTasks), run the
        # async send_webhook in a new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(send_webhook(webhook_url, payload))
        loop.close()
    except Exception as e:
        logging.getLogger(__name__).error(
            f"Failed to send webhook for job '{job_id}': {e}", exc_info=True
        )