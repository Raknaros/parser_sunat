from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Optional
import logging
from xml.etree import ElementTree as ET

class BaseXMLProcessor(ABC):
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    @abstractmethod
    def process_file(self, xml_content: str, file_name: str) -> Optional[Dict[str, pd.DataFrame]]:
        """Procesa un archivo XML y retorna un diccionario de DataFrames con los datos extraídos"""
        pass

    def validate_xml(self, xml_content: str, file_name: str) -> bool:
        """Valida la estructura básica del archivo XML"""
        try:
            ET.fromstring(xml_content)
            self.log_operation("Validación", "Éxito", f"Archivo: {file_name}")
            return True
        except ET.ParseError as e:
            self.logger.error(f"No se pudo leer el archivo XML {file_name}: {e}")
            return False

    def log_operation(self, operation: str, status: str, details: str, level: int = logging.INFO):
        """Registra una operación en el log"""
        self.logger.log(level, f"Operación: {operation} - Estado: {status} - Detalles: {details}") 