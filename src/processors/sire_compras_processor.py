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
            'RUC': 'ruc',
            'Apellidos y Nombres o Razón social': 'nombre_receptor',
            'Periodo': 'periodo_tributario',
            'CAR SUNAT': 'observaciones',
            'Fecha de emisión': 'fecha_emision',
            'Fecha Vcto/Pago': 'fecha_vencimiento',
            'Tipo CP/Doc.': 'tipo_comprobante',
            'Serie del CDP': 'numero_serie',
            'Año': 'ano',
            'Nro CP o Doc. Nro Inicial (Rango)': 'numero_correlativo',
            'Nro Final (Rango)': 'numero_final',
            'Tipo Doc Identidad': 'tipo_documento',
            'Nro Doc Identidad': 'numero_documento',
            'Apellidos Nombres/ Razón  Social': 'nombre_razon',
            'BI Gravado DG': 'bi_gravado_dg',
            'IGV / IPM DG': 'igv_gravado_dg',
            'BI Gravado DGNG': 'bi_gravado_dgng',
            'IGV / IPM DGNG': 'igv_gravado_dgng',
            'BI Gravado DNG': 'bi_gravado_dng',
            'IGV / IPM DNG': 'igv_gravado_dng',
            'Valor Adq. NG': 'valor_adq_ng',
            'ISC': 'isc',
            'ICBPER': 'icbp',
            'Otros Trib/ Cargos': 'otros_cargos',
            'Total CP': 'importe_total',
            'Moneda': 'tipo_moneda',
            'Tipo de Cambio': 'tipo_cambio',
            'Fecha Emisión Doc Modificado': 'fecha_comprobante_modificado',
            'Tipo CP Modificado': 'tipo_comprobante_modificado',
            'Serie CP Modificado': 'numero_serie_modificado',
            'COD. DAM O DSI': 'dam',
            'Nro CP Modificado': 'numero_correlativo_modificado',
            'Clasif de Bss y Sss': 'clasificacion_bienes_servicios',
            'ID Proyecto Operadores': 'proyecto_operadores',
            'PorcPart': 'porc_part',
            'IMB': 'imb',
            'CAR Orig/ Ind E o I': 'car_original',
            'Detracción': 'detraccion',
            'Tipo de Nota': 'tipo_nota',
            'Est. Comp.': 'estado_comprobante',
            'Incal': 'incal'
        }
        self.FINAL_COLUMNS = list(self.RENAME_MAP.values()) + ['cui']


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
                content = content_bytes.decode('utf-8-sig')
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
        df.rename(columns=self.RENAME_MAP, inplace=True)
        
        self._aplicar_filtro_complejo(df)
        self._convert_data_types(df)
        
        df['cui'] = df.apply(
            lambda row: self._generate_cui(
                row.get('tipo_comprobante'),
                row.get('ruc'),
                row.get('numero_documento'),
                row.get('numero_serie'),
                row.get('numero_correlativo')
            ), axis=1)
        
        final_df = df[[col for col in self.FINAL_COLUMNS if col in df.columns]].copy()
        return final_df

    def _aplicar_filtro_complejo(self, df: pd.DataFrame) -> None:
        columnas_valor = ['bi_gravado_dg', 'igv_gravado_dg', 'bi_gravado_dgng', 'igv_gravado_dgng',
                          'bi_gravado_dng', 'igv_dng', 'valor_adq_ng', 'otros_cargos']
        for col in columnas_valor:
            if col not in df.columns: df[col] = 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        cond_destino_5 = ((df['bi_gravado_dg'] > 0) | (df['bi_gravado_dgng'] > 0) | (df['bi_gravado_dng'] > 0)) & (df['valor_adq_ng'] > 0)
        cond_destino_1 = (df['bi_gravado_dg'] > 0)
        cond_destino_2 = (df['bi_gravado_dgng'] > 0)
        cond_destino_3 = (df['bi_gravado_dng'] > 0)
        cond_destino_4 = (df['valor_adq_ng'] > 0)
        condiciones = [cond_destino_5, cond_destino_1, cond_destino_2, cond_destino_3, cond_destino_4]
        
        resultados_destino = [5, 1, 2, 3, 4]
        resultados_valor = [df['bi_gravado_dg'] + df['bi_gravado_dgng'] + df['bi_gravado_dng'], df['bi_gravado_dg'], df['bi_gravado_dgng'], df['bi_gravado_dng'], df['valor_adq_ng']]
        resultados_igv = [df['igv_gravado_dg'] + df['igv_gravado_dgng'] + df['igv_dng'], df['igv_gravado_dg'], df['igv_gravado_dgng'], df['igv_dng'], 0]
        resultados_otros = [df['otros_cargos'] + df['valor_adq_ng'], df['otros_cargos'], df['otros_cargos'], df['otros_cargos'], df['otros_cargos']]
        
        df['destino'] = np.select(condiciones, resultados_destino, default=0)
        df['valor'] = np.select(condiciones, resultados_valor, default=0)
        df['igv'] = np.select(condiciones, resultados_igv, default=0)
        df['otros_cargos'] = np.select(condiciones, resultados_otros, default=0)
        df['tipo_operacion'] = 2

    def _convert_data_types(self, df: pd.DataFrame) -> None:
        if 'ruc' in df.columns: df['ruc'] = pd.to_numeric(df['ruc'], errors='coerce').astype('Int64')

        int_columns = ['periodo_tributario', 'tipo_comprobante', 'destino', 'ano', 'numero_final', 'tipo_documento', 'tipo_comprobante_modificado']
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        date_columns = ['fecha_emision', 'fecha_vencimiento', 'fecha_comprobante_modificado']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce').dt.date

        varchar_columns = ['numero_serie', 'numero_correlativo', 'numero_documento', 'nombre_receptor', 'nombre_razon', 
                           'tipo_moneda', 'numero_serie_modificado', 'numero_correlativo_modificado', 'dam', 
                           'clasificacion_bienes_servicios', 'proyecto_operadores', 'car_original', 'tipo_nota', 
                           'estado_comprobante', 'incal', 'cui']
        for col in varchar_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df.loc[df[col] == 'nan', col] = np.nan

        numeric_columns = ['bi_gravado_dg', 'igv_gravado_dg', 'bi_gravado_dgng', 'igv_gravado_dgng', 'bi_gravado_dng', 
                           'igv_dng', 'valor_adq_ng', 'isc', 'icbp', 'otros_cargos', 'importe_total', 'tipo_cambio', 
                           'porc_part', 'imb', 'detraccion', 'valor', 'igv']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round(2)


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
