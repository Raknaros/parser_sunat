"""
Abstract base class for all document processors.

This class defines the contract that all SUNAT document processors must fulfill.
It supports both:
- Legacy disk-based processing (process_file) for backward compatibility
- In-memory processing (process_content) for the new S3-based pipeline
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional
import logging
import pandas as pd


class BaseDocumentProcessor(ABC):
    """
    Abstract base for SUNAT document processors.

    Subclasses must implement:
    - process_file(file_path)     → Legacy: reads from disk
    - process_content(file_name, file_content)  → New: reads from bytes
    - get_db_mapping()           → Returns table/column mapping for DB insertion
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    # ── Legacy interface (disk-based) ─────────────────────────────────────

    @abstractmethod
    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Process a document from a local file path.

        This method is preserved for backward compatibility (CLI mode).
        New code should use process_content() instead.

        Args:
            file_path: Absolute path to the file on disk

        Returns:
            Dict of {table_key: DataFrame} or None if processing failed
        """
        ...

    # ── New interface (in-memory, for S3 pipeline) ────────────────────────

    @abstractmethod
    def process_content(
        self, file_name: str, file_content: bytes
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Process a document from raw bytes (in-memory).

        This is the primary method for the cloud-native pipeline.
        Subclasses must not write to disk or use path-based operations.

        Args:
            file_name: Original filename (for type detection and logging)
            file_content: Raw file bytes (XML, ZIP, TXT, etc.)

        Returns:
            Dict of {table_key: DataFrame} or None if processing failed
        """
        ...

    # ── Database mapping ──────────────────────────────────────────────────

    @abstractmethod
    def get_db_mapping(self) -> Dict[str, Dict]:
        """
        Return the mapping from DataFrame keys to database tables/columns.

        Example:
        {
            'header': {
                'table': 'stg_xml_headers',
                'schema': 'meta',
                'columns': {'CUI': 'cui', 'numero': 'serie_numero', ...}
            },
            'lines': { ... }
        }
        """
        ...

    # ── Utilities ─────────────────────────────────────────────────────────

    def log_operation(self, operation: str, status: str, details: str, level: int = logging.INFO):
        """Log a structured operation record."""
        self.logger.log(level, f"Operación: {operation} - Estado: {status} - Detalles: {details}")