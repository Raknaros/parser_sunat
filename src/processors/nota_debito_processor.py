"""
NotaDebitoProcessor - Processes SUNAT Debit Notes (UBL 2.1 XML).

Supports both disk-based (legacy) and in-memory (S3 pipeline) processing.
"""
import io
import zipfile
from pathlib import Path
from typing import Dict, Optional
import logging

import pandas as pd
import xml.etree.ElementTree as ET

from .base_processor import BaseDocumentProcessor
from src.utils.xml_utils import get_xml_encoding

# TODO: Migrate to lxml.etree.fromstring for better performance and namespace support
#       (as specified in ARCHITECTURE_BLUEPRINT.md Phase 4)


class NotaDebitoProcessor(BaseDocumentProcessor):
    def get_db_mapping(self) -> Dict[str, Dict]:
        """
        Retorna el mapeo de los DataFrames generados a las tablas y columnas de la BD.
        """
        return {
            'header': {
                'table': 'cabeceras',
                'schema': 'public',
                'columns': {
                    'tipo_documento': 'tipo_documento_id',
                    'numero': 'numero_documento',
                    'fecha_emision': 'fecha_emision',
                    'ruc_emisor': 'ruc_emisor',
                    'doc_referencia': 'documento_referencia',
                    'motivo': 'motivo',
                    'total': 'importe_total',
                }
            }
        }

    # ── Legacy: process_file (disk-based) ─────────────────────────────────

    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        """Procesa un archivo XML de Nota de Débito y extrae sus datos principales."""
        file_name = Path(file_path).name
        self.log_operation("Procesamiento", "Iniciado", f"Archivo: {file_name}")

        try:
            encoding = get_xml_encoding(file_path)
            with open(file_path, 'r', encoding=encoding) as f:
                xml_content = f.read()

            return self._extract_data(xml_content, file_name)

        except ET.ParseError as e_parse:
            self.log_operation("Procesamiento", "Error", f"Archivo XML mal formado: {file_name}, Error: {e_parse}")
            return None
        except Exception as e:
            self.log_operation("Procesamiento", "Error", f"Error inesperado procesando archivo: {file_name}, Error: {str(e)}")
            return None

    # ── New: process_content (in-memory, for S3 pipeline) ────────────────

    def process_content(self, file_name: str, file_content: bytes) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Process a Nota de Débito from raw bytes (in-memory, for S3 pipeline).

        Args:
            file_name: Original filename (for type detection and logging)
            file_content: Raw file bytes (XML or ZIP containing XML)

        Returns:
            Dict with 'header' DataFrame, or None if processing failed.
        """
        self.log_operation("Procesamiento", "Iniciado", f"Archivo (memoria): {file_name}")

        try:
            xml_content = None

            if file_name.lower().endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(file_content), 'r') as zip_ref:
                    xml_filename = next(
                        (name for name in zip_ref.namelist() if name.lower().endswith('.xml')),
                        None
                    )
                    if xml_filename:
                        with zip_ref.open(xml_filename) as f:
                            content_bytes = f.read()
                            try:
                                xml_content = content_bytes.decode('utf-8')
                            except UnicodeDecodeError:
                                xml_content = content_bytes.decode('ISO-8859-1')
                    else:
                        self.logger.warning(f"El archivo ZIP {file_name} no contiene ningún XML.")
                        return None

            elif file_name.lower().endswith('.xml'):
                try:
                    xml_content = file_content.decode('utf-8')
                except UnicodeDecodeError:
                    xml_content = file_content.decode('ISO-8859-1')

            if xml_content is None:
                return None

            return self._extract_data(xml_content, file_name)

        except Exception as e:
            self.log_operation(
                "Procesamiento", "Error",
                f"Error procesando archivo (memoria): {file_name}, Error: {str(e)}",
                level=logging.ERROR
            )
            return None

    # ── Core parsing logic (shared by both interfaces) ────────────────────

    def _extract_data(self, xml_content: str, file_name: str) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Core parsing logic shared by process_file and process_content.

        NOTE: This is a simplified parser. It should be updated to match the
        same level of detail as the FacturaProcessor, including namespaces.
        """
        root = ET.fromstring(xml_content)

        data = {
            'tipo_documento': ['NotaDebito'],
            'numero': [root.find('.//numeroDocumento').text if root.find('.//numeroDocumento') is not None else ''],
            'fecha_emision': [root.find('.//fechaEmision').text if root.find('.//fechaEmision') is not None else ''],
            'ruc_emisor': [root.find('.//rucEmisor').text if root.find('.//rucEmisor') is not None else ''],
            'doc_referencia': [root.find('.//documentoReferencia').text if root.find('.//documentoReferencia') is not None else ''],
            'motivo': [root.find('.//motivo').text if root.find('.//motivo') is not None else ''],
            'total': [root.find('.//importeTotal').text if root.find('.//importeTotal') is not None else '0.00']
        }

        df_header = pd.DataFrame(data)
        self.log_operation("Procesamiento", "Éxito", f"Archivo: {file_name}")

        return {'header': df_header}