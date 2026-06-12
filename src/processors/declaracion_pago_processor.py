"""
DeclaracionPagoProcessor - Stub processor for SUNAT Payment Declarations (Excel).

This processor exists so the system can identify /declaraciones_pagos/ files in
the pipeline, but it does not parse them yet. All calls return None (unprocessed).
"""
import logging
from typing import Dict, Optional

import pandas as pd

from .base_processor import BaseDocumentProcessor


class DeclaracionPagoProcessor(BaseDocumentProcessor):
    """
    Stub processor for SUNAT payment declarations (DetalleDeclaraciones).

    Logs that the document was identified but not processed.
    """

    # Marker for the registry to identify this as a stub
    _is_stub = True

    def __init__(self, logger: logging.Logger):
        super().__init__(logger)

    def get_db_mapping(self) -> Dict[str, Dict]:
        return {}

    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        self.logger.info(
            f"DeclaracionPagoProcessor (stub): archivo ignorado '{file_path}'"
        )
        return None

    def process_content(self, file_name: str, file_content: bytes) -> Optional[Dict[str, pd.DataFrame]]:
        self.logger.info(
            f"DeclaracionPagoProcessor (stub): archivo ignorado '{file_name}'"
        )
        return None