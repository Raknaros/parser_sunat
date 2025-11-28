from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Optional
import logging

class BaseDocumentProcessor(ABC):
    """
    Clase base abstracta para procesadores de documentos.
    Cada procesador debe implementar un método para procesar un archivo
    y devolver un diccionario de DataFrames.
    """
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    @abstractmethod
    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Procesa un archivo a partir de su ruta y retorna un diccionario de DataFrames.
        """
        pass

    @abstractmethod
    def get_db_mapping(self) -> Dict[str, Dict]:
        """
        Retorna el mapeo de los DataFrames generados a las tablas y columnas de la BD.
        
        Ejemplo:
        {
            'header': {
                'table': 'cabeceras', 
                'schema': 'public',
                'columns': {'df_col_1': 'db_col_1', ...}
            },
            'lines': { ... }
        }
        """
        pass

    def log_operation(self, operation: str, status: str, details: str, level: int = logging.INFO):
        """Registra una operación en el log."""
        self.logger.log(level, f"Operación: {operation} - Estado: {status} - Detalles: {details}")
