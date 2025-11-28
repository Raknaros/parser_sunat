from .base_processor import BaseDocumentProcessor
from utils.xml_utils import get_xml_encoding
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Dict, Optional

class GuiaRemisionProcessor(BaseDocumentProcessor):
    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        """Procesa un archivo XML de Guía de Remisión y extrae sus datos principales."""
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
                'tipo_documento': ['GuiaRemision'],
                'numero': [root.find('.//numeroDocumento').text if root.find('.//numeroDocumento') is not None else ''],
                'fecha_emision': [root.find('.//fechaEmision').text if root.find('.//fechaEmision') is not None else ''],
                'fecha_traslado': [root.find('.//fechaTraslado').text if root.find('.//fechaTraslado') is not None else ''],
                'ruc_emisor': [root.find('.//rucEmisor').text if root.find('.//rucEmisor') is not None else ''],
                'ruc_destinatario': [root.find('.//rucDestinatario').text if root.find('.//rucDestinatario') is not None else ''],
                'direccion_partida': [root.find('.//direccionPartida').text if root.find('.//direccionPartida') is not None else ''],
                'direccion_llegada': [root.find('.//direccionLlegada').text if root.find('.//direccionLlegada') is not None else '']
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
