from .base_processor import BaseXMLProcessor
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET

class NotaCreditoProcessor(BaseXMLProcessor):
    def process_file(self, xml_content: str, file_name: str) -> pd.DataFrame:
        """Procesa un archivo XML de Nota de Crédito y extrae sus datos principales"""
        self.log_operation("Procesamiento", "Iniciado", f"Archivo: {file_name}")
        
        try:
            if not self.validate_xml(xml_content, file_name):
                return pd.DataFrame()
                
            root = ET.fromstring(xml_content)
            
            # Extraer datos básicos de la nota de crédito (ajustar según la estructura real)
            data = {
                'tipo_documento': ['NotaCredito'],
                'numero': [root.find('.//numeroDocumento').text if root.find('.//numeroDocumento') is not None else ''],
                'fecha_emision': [root.find('.//fechaEmision').text if root.find('.//fechaEmision') is not None else ''],
                'ruc_emisor': [root.find('.//rucEmisor').text if root.find('.//rucEmisor') is not None else ''],
                'doc_referencia': [root.find('.//documentoReferencia').text if root.find('.//documentoReferencia') is not None else ''],
                'total': [root.find('.//importeTotal').text if root.find('.//importeTotal') is not None else '0.00']
            }
            
            df = pd.DataFrame(data)
            self.log_operation("Procesamiento", "Éxito", f"Archivo: {file_name}")
            return df
            
        except Exception as e:
            self.log_operation("Procesamiento", "Error", f"Archivo: {file_name}, Error: {str(e)}")
            return pd.DataFrame()