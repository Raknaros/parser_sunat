from .base_processor import BaseDocumentProcessor
from utils.xml_utils import get_xml_encoding
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Dict, Optional

class BoletaVentaProcessor(BaseDocumentProcessor):
    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        """Procesa un archivo XML de Boleta de Venta y extrae sus datos principales."""
        file_name = Path(file_path).name
        self.log_operation("Procesamiento", "Iniciado", f"Archivo: {file_name}")

        try:
            encoding = get_xml_encoding(file_path)
            with open(file_path, 'r', encoding=encoding) as f:
                xml_content = f.read()

            root = ET.fromstring(xml_content)
            
            # NOTE: This is a simplified parser. It should be updated to match the
            # same level of detail as the FacturaProcessor, including namespaces.
            data = {
                'tipo_documento': ['BoletaVenta'],
                'numero': [root.find('.//numeroDocumento').text if root.find('.//numeroDocumento') is not None else ''],
                'fecha_emision': [root.find('.//fechaEmision').text if root.find('.//fechaEmision') is not None else ''],
                'ruc_emisor': [root.find('.//rucEmisor').text if root.find('.//rucEmisor') is not None else ''],
                'dni_cliente': [root.find('.//dniCliente').text if root.find('.//dniCliente') is not None else ''],
                'nombre_cliente': [root.find('.//nombreCliente').text if root.find('.//nombreCliente') is not None else ''],
                'total': [root.find('.//importeTotal').text if root.find('.//importeTotal') is not None else '0.00']
            }
            
            df_header = pd.DataFrame(data)
            self.log_operation("Procesamiento", "Éxito", f"Archivo: {file_name}")
            
            return {'header': df_header}

        except ET.ParseError as e_parse:
            self.log_operation("Procesamiento", "Error", f"Archivo XML mal formado: {file_name}, Error: {e_parse}")
            return None
        except Exception as e:
            self.log_operation("Procesamiento", "Error", f"Error inesperado procesando archivo: {file_name}, Error: {str(e)}")
            return None
