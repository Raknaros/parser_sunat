from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
import logging
from xml.etree import ElementTree as ET

class BaseXMLProcessor(ABC):
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        
    @abstractmethod
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Procesa un archivo XML y retorna un DataFrame con los datos extraídos"""
        pass
        
    def validate_xml(self, file_path: Path) -> bool:
        """Valida la estructura básica del archivo XML"""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            self.log_operation("Validación", "Éxito", f"Archivo: {file_path}")
            return True
        except Exception as e:
            self.log_operation("Validación", "Error", f"Archivo: {file_path}, Error: {str(e)}")
            return False
        
    def log_operation(self, operation: str, status: str, details: str):
        """Registra una operación en el log"""
        self.logger.info(f"Operación: {operation} - Estado: {status} - Detalles: {details}") 