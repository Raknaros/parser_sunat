from .base_processor import BaseXMLProcessor
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET

class BoletaVentaProcessor(BaseXMLProcessor):
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Procesa un archivo XML de Boleta de Venta y extrae sus datos principales"""
        self.log_operation("Procesamiento", "Iniciado", f"Archivo: {file_path}")
        
        try:
            if not self.validate_xml(file_path):
                return pd.DataFrame()
                
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Extraer datos básicos de la boleta de venta (ajustar según la estructura real)
            data = {
                'tipo_documento': ['BoletaVenta'],
                'numero': [root.find('.//numeroDocumento').text if root.find('.//numeroDocumento') is not None else ''],
                'fecha_emision': [root.find('.//fechaEmision').text if root.find('.//fechaEmision') is not None else ''],
                'ruc_emisor': [root.find('.//rucEmisor').text if root.find('.//rucEmisor') is not None else ''],
                'dni_cliente': [root.find('.//dniCliente').text if root.find('.//dniCliente') is not None else ''],
                'nombre_cliente': [root.find('.//nombreCliente').text if root.find('.//nombreCliente') is not None else ''],
                'total': [root.find('.//importeTotal').text if root.find('.//importeTotal') is not None else '0.00']
            }
            
            df = pd.DataFrame(data)
            self.log_operation("Procesamiento", "Éxito", f"Archivo: {file_path}")
            return df
            
        except Exception as e:
            self.log_operation("Procesamiento", "Error", f"Archivo: {file_path}, Error: {str(e)}")
            return pd.DataFrame() 