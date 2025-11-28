import os
import zipfile
import logging
import numpy as np
import pandas as pd
from io import StringIO
from typing import Optional, Dict

from .base_processor import BaseDocumentProcessor

class SireVentasProcessor(BaseDocumentProcessor):
    def __init__(self, logger: logging.Logger):
        super().__init__(logger)
        self.COLUMN_MAPPING = {
            'Ruc': 'ruc_emisor',
            'Periodo': 'periodo_tributario',
            'CAR SUNAT': 'observaciones',
            'Fecha de emisión': 'fecha_emision',
            'Fecha Vcto/Pago': 'fecha_vencimiento',
            'Tipo CP/Doc.': 'tipo_documento_id',
            'Serie del CDP': 'serie_documento',
            'Nro CP o Doc. Nro Inicial (Rango)': 'numero_documento',
            'Tipo Doc Identidad': 'tipo_doc_receptor',
            'Nro Doc Identidad': 'ruc_receptor',
            'Apellidos Nombres/ Razón Social': 'nombre_receptor',
            'Valor Facturado Exportación': 'valor_exportacion',
            'BI Gravada': 'total_bi_gravado',
            'Dscto BI': 'total_dscto_bi',
            'IGV / IPM': 'total_igv',
            'Dscto IGV / IPM': 'total_dscto_igv',
            'Mto Exonerado': 'total_exonerado',
            'Mto Inafecto': 'total_inafecto',
            'ISC': 'total_isc',
            'BI Grav IVAP': 'total_bi_ivap',
            'IVAP': 'total_ivap',
            'ICBPER': 'total_icbper',
            'Otros Tributos': 'total_otros_tributos',
            'Importe Total': 'importe_total',
            'Moneda': 'moneda_id',
            'Tipo de Cambio': 'tipo_cambio',
            'Tipo CP Modificado': 'tipo_doc_modificado',
            'Serie CP Modificado': 'serie_modificada',
            'Nro CP Modificado': 'numero_modificado',
            'CUI': 'cui'
        }

    def get_db_mapping(self) -> Dict[str, Dict]:
        return {
            'sire_ventas': {
                'table': 'cabeceras',
                'schema': 'public',
                'columns': self.COLUMN_MAPPING
            }
        }

    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        file_name = os.path.basename(file_path)
        self.log_operation("Procesamiento SIRE Ventas", "Iniciado", f"Archivo: {file_name}")
        
        try:
            raw_df = self._extract_data(file_path)
            if raw_df is None: return None
            if raw_df.empty:
                self.logger.warning(f"El archivo SIRE '{file_name}' no contiene comprobantes. Proceso exitoso (vacío).")
                return {'sire_ventas': pd.DataFrame()}

            transformed_df = self._transform_data(raw_df)
            self.log_operation("Procesamiento SIRE Ventas", "Éxito", f"Archivo: {file_name}, Filas: {len(transformed_df)}")
            return {'sire_ventas': transformed_df}
        except Exception as e:
            self.log_operation("Procesamiento SIRE Ventas", "Error", f"Error en {file_path}: {e}", level=logging.ERROR)
            return None

    def _extract_data(self, file_path: str) -> Optional[pd.DataFrame]:
        # ... (la lógica de extracción no cambia)
        lista_dataframes = []
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"Archivo no encontrado: {file_path}")
                return None
            if file_path.lower().endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    for nombre_archivo in zip_ref.namelist():
                        if nombre_archivo.lower().endswith('.txt'):
                            with zip_ref.open(nombre_archivo) as file:
                                content = file.read().decode('latin-1', errors='replace')
                                if len(content.splitlines()) < 2: continue
                                df = pd.read_csv(StringIO(content), sep='|', header=0, dtype=str)
                                lista_dataframes.append(df)
            elif file_path.lower().endswith('.txt'):
                with open(file_path, 'r', encoding='latin-1') as f:
                    if len(f.readlines()) < 2: return pd.DataFrame()
                df = pd.read_csv(file_path, sep='|', header=0, dtype=str, encoding='latin-1')
                lista_dataframes.append(df)
            else:
                return None
            if not lista_dataframes: return pd.DataFrame()
            return pd.concat(lista_dataframes, ignore_index=True)
        except Exception as e:
            self.logger.error(f"Error en extracción de {file_path}: {e}", exc_info=True)
            return None

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df_transformado = df.copy()
        
        # Generar CUI
        df_transformado['CUI'] = df_transformado.apply(
            lambda row: self._generate_cui(
                row.get('Ruc'), 
                row.get('Tipo CP/Doc.'), 
                row.get('Serie del CDP'), 
                row.get('Nro CP o Doc. Nro Inicial (Rango)')
            ), 
            axis=1
        )
        
        # Renombrar columnas
        df_renamed = df_transformado.rename(columns=self.COLUMN_MAPPING)
        
        # Seleccionar columnas finales
        final_columns = list(self.COLUMN_MAPPING.values())
        df_final = df_renamed[[col for col in final_columns if col in df_renamed.columns]].copy()

        return df_final

    def _generate_cui(self, ruc, tipo_doc, serie, numero):
        if pd.isna(ruc) or pd.isna(tipo_doc) or pd.isna(serie) or pd.isna(numero):
            return None
        try:
            serie_fmt = str(serie).strip()
            numero_fmt = str(numero).strip()
            full_numero = f"{serie_fmt}-{numero_fmt}"
            
            return f"{hex(int(ruc))[2:].upper()}{int(tipo_doc):02d}{full_numero.replace('-', '')}"
        except (ValueError, TypeError):
            return None
