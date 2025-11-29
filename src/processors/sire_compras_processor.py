import os
import zipfile
import logging
import numpy as np
import pandas as pd
from io import StringIO
from typing import Optional, Dict, List

from .base_processor import BaseDocumentProcessor

class SireComprasProcessor(BaseDocumentProcessor):
    def __init__(self, logger: logging.Logger):
        super().__init__(logger)
        self.RENAME_MAP = {
            'RUC': 'ruc', 'Periodo': 'periodo_tributario', 'CAR SUNAT': 'observaciones',
            'Fecha de emisión': 'fecha_emision', 'Fecha Vcto/Pago': 'fecha_vencimiento',
            'Tipo CP/Doc.': 'tipo_comprobante', 'Serie del CDP': 'numero_serie',
            'Nro CP o Doc. Nro Inicial (Rango)': 'numero_correlativo',
            'Tipo Doc Identidad': 'tipo_documento', 'Nro Doc Identidad': 'numero_documento',
            'Apellidos y Nombres, Denominación o Razón Social': 'nombre_receptor',
            'BI Gravado DG': 'bi_gravado_dg', 'IGV / IPM DG': 'igv_dg',
            'BI Gravado DGNG': 'bi_gravado_dgng', 'IGV / IPM DGNG': 'igv_dgng',
            'BI Gravado DNG': 'bi_gravado_dng', 'IGV / IPM DNG': 'igv_dng',
            'Valor Adq. NG': 'valor_adq_ng', 'ISC': 'isc', 'ICBPER': 'icbp',
            'Otros Trib/ Cargos': 'otros_cargos_base', 'Importe Total': 'importe_total',
            'Moneda': 'tipo_moneda', 'Tipo de Cambio': 'tipo_cambio',
            'Tipo CP Modificado': 'tipo_comprobante_modificado',
            'Serie CP Modificado': 'numero_serie_modificado',
            'Nro CP Modificado': 'numero_correlativo_modificado',
            'Detracción': 'tasa_detraccion',
            'CUI': 'cui', 'destino': 'destino', 'valor': 'valor', 'igv': 'igv',
            'otros_cargos': 'otros_cargos', 'tipo_operacion': 'tipo_operacion'
        }
        self.FINAL_COLUMNS = [
            'ruc', 'periodo_tributario', 'observaciones', 'fecha_emision', 'fecha_vencimiento',
            'tipo_comprobante', 'numero_serie', 'numero_correlativo', 'tipo_documento',
            'numero_documento', 'nombre_receptor', 'importe_total', 'isc', 'icbp', 'tipo_moneda', 'tipo_cambio',
            'tipo_comprobante_modificado', 'numero_serie_modificado', 'numero_correlativo_modificado', 'tasa_detraccion',
            'destino', 'valor', 'igv', 'otros_cargos', 'tipo_operacion', 'cui'
        ]

    def get_db_mapping(self) -> Dict[str, Dict]:
        final_mapping = {col: col for col in self.FINAL_COLUMNS}
        return {'sire_compras': {'table': '_8', 'schema': 'acc', 'columns': final_mapping}}

    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        file_name = os.path.basename(file_path)
        self.log_operation("Procesamiento SIRE Compras", "Iniciado", f"Archivo: {file_name}")
        try:
            raw_df = self._extract_data(file_path)
            if raw_df is None: return None
            if raw_df.empty:
                self.logger.warning(f"El archivo SIRE '{file_name}' no contiene comprobantes. Proceso exitoso (vacío).")
                return {'sire_compras': pd.DataFrame()}
            transformed_df = self._transform_data(raw_df)
            self.log_operation("Procesamiento SIRE Compras", "Éxito", f"Archivo: {file_name}, Filas: {len(transformed_df)}")
            return {'sire_compras': transformed_df}
        except Exception as e:
            self.log_operation("Procesamiento SIRE Compras", "Error", f"Error en {file_path}: {e}", level=logging.ERROR)
            return None

    def _extract_data(self, file_path: str) -> Optional[pd.DataFrame]:
        all_dfs = []
        try:
            if file_path.lower().endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    for name in zip_ref.namelist():
                        if name.lower().endswith('.txt'):
                            with zip_ref.open(name) as file:
                                df = self._read_txt_content(file)
                                if df is not None: all_dfs.append(df)
            elif file_path.lower().endswith('.txt'):
                with open(file_path, 'rb') as file:
                    df = self._read_txt_content(file)
                    if df is not None: all_dfs.append(df)
            else: return None
            if not all_dfs: return pd.DataFrame()
            return pd.concat(all_dfs, ignore_index=True)
        except Exception as e:
            self.logger.error(f"Error en extracción de {file_path}: {e}", exc_info=True)
            return None

    def _read_txt_content(self, file_obj) -> Optional[pd.DataFrame]:
        try:
            content_bytes = file_obj.read()
            try:
                content = content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                content = content_bytes.decode('latin-1', errors='replace')
            
            lines = content.splitlines()
            if len(lines) < 2: return None
            
            header = [h.strip() for h in lines[0].split('|')]
            data = StringIO('\n'.join(lines[1:]))
            df = pd.read_csv(data, sep='|', header=None, names=header, dtype=str)
            return df
        except Exception as e:
            self.logger.error(f"Error leyendo contenido de archivo TXT: {e}")
            return None

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        self._aplicar_filtro_complejo(df)
        self._convert_data_types(df)
        
        df['CUI'] = df.apply(
            lambda row: self._generate_cui(
                row.get('Tipo CP/Doc.'),
                row.get('RUC'),
                row.get('Nro Doc Identidad'),
                row.get('Serie del CDP'),
                row.get('Nro CP o Doc. Nro Inicial (Rango)')
            ), axis=1)
        
        df.rename(columns=self.RENAME_MAP, inplace=True)
        
        final_df = df[[col for col in self.FINAL_COLUMNS if col in df.columns]].copy()
        return final_df

    def _aplicar_filtro_complejo(self, df: pd.DataFrame) -> None:
        columnas_valor = ['BI Gravado DG', 'IGV / IPM DG', 'BI Gravado DGNG', 'IGV / IPM DGNG',
                          'BI Gravado DNG', 'IGV / IPM DNG', 'Valor Adq. NG', 'Otros Trib/ Cargos']
        for col in columnas_valor:
            if col not in df.columns: df[col] = 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        cond_destino_5 = ((df['BI Gravado DG'] > 0) | (df['BI Gravado DGNG'] > 0) | (df['BI Gravado DNG'] > 0)) & (df['Valor Adq. NG'] > 0)
        cond_destino_1 = (df['BI Gravado DG'] > 0)
        cond_destino_2 = (df['BI Gravado DGNG'] > 0)
        cond_destino_3 = (df['BI Gravado DNG'] > 0)
        cond_destino_4 = (df['Valor Adq. NG'] > 0)
        condiciones = [cond_destino_5, cond_destino_1, cond_destino_2, cond_destino_3, cond_destino_4]
        
        resultados_destino = [5, 1, 2, 3, 4]
        resultados_valor = [df['BI Gravado DG'] + df['BI Gravado DGNG'] + df['BI Gravado DNG'], df['BI Gravado DG'], df['BI Gravado DGNG'], df['BI Gravado DNG'], df['Valor Adq. NG']]
        resultados_igv = [df['IGV / IPM DG'] + df['IGV / IPM DGNG'] + df['IGV / IPM DNG'], df['IGV / IPM DG'], df['IGV / IPM DGNG'], df['IGV / IPM DNG'], 0]
        resultados_otros = [df['Otros Trib/ Cargos'] + df['Valor Adq. NG'], df['Otros Trib/ Cargos'], df['Otros Trib/ Cargos'], df['Otros Trib/ Cargos'], df['Otros Trib/ Cargos']]
        
        df['destino'] = np.select(condiciones, resultados_destino, default=0)
        df['valor'] = np.select(condiciones, resultados_valor, default=0)
        df['igv'] = np.select(condiciones, resultados_igv, default=0)
        df['otros_cargos'] = np.select(condiciones, resultados_otros, default=0)
        df['tipo_operacion'] = 2

    def _convert_data_types(self, df: pd.DataFrame) -> None:
        if 'Fecha de emisión' in df.columns:
            df['Fecha de emisión'] = pd.to_datetime(df['Fecha de emisión'], format='%d/%m/%Y', errors='coerce')
        if 'Fecha Vcto/Pago' in df.columns:
            df['Fecha Vcto/Pago'] = pd.to_datetime(df['Fecha Vcto/Pago'], format='%d/%m/%Y', errors='coerce')
        if 'Periodo' in df.columns:
            df['Periodo'] = pd.to_datetime(df['Periodo'], format='%Y%m', errors='coerce').dt.strftime('%Y%m')
        if 'Detracción' in df.columns:
            df['Detracción'] = df['Detracción'].replace('D', 0)
            df['Detracción'] = pd.to_numeric(df['Detracción'], errors='coerce')

    def _generate_cui(self, tipo_comprobante, ruc_empresa, ruc_proveedor, serie, numero):
        if pd.isna(tipo_comprobante) or pd.isna(serie) or pd.isna(numero): return None
        try:
            tipo_comprobante_str = str(tipo_comprobante).strip()
            if tipo_comprobante_str == '53':
                if pd.isna(ruc_empresa): return None
                ruc_hex = hex(int(ruc_empresa))[2:].lower()
            else:
                if pd.isna(ruc_proveedor): return None
                ruc_hex = hex(int(ruc_proveedor))[2:].lower()
            serie_fmt = str(serie).strip()
            numero_fmt = str(numero).strip()
            full_numero = f"{serie_fmt}-{numero_fmt}"
            return f"{ruc_hex}{int(float(tipo_comprobante_str)):02d}{full_numero.replace('-', '')}"
        except (ValueError, TypeError):
            return None
