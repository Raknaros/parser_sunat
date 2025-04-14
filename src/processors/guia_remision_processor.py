from .base_processor import BaseXMLProcessor
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET

class GuiaRemisionProcessor(BaseXMLProcessor):
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Procesa un archivo XML de Guía de Remisión y extrae sus datos principales"""
        self.log_operation("Procesamiento", "Iniciado", f"Archivo: {file_path}")
        
        try:
            if not self.validate_xml(file_path):
                return pd.DataFrame()
                
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Extraer datos básicos de la guía de remisión (ajustar según la estructura real)
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
            
            df = pd.DataFrame(data)
            self.log_operation("Procesamiento", "Éxito", f"Archivo: {file_path}")
            return df
            
        except Exception as e:
            self.log_operation("Procesamiento", "Error", f"Archivo: {file_path}, Error: {str(e)}")
            return pd.DataFrame() 