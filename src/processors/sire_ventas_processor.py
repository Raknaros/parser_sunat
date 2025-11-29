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
        self.RENAME_MAP = {
            'Ruc': 'ruc',
            'Razon Social': 'nombre_emisor',
            'Periodo': 'periodo_tributario',
            'CAR SUNAT': 'observaciones',
            'Fecha de emisión': 'fecha_emision',
            'Fecha Vcto/Pago': 'fecha_vencimiento',
            'Tipo CP/Doc.': 'tipo_comprobante',
            'Serie del CDP': 'numero_serie',
            'Nro CP o Doc. Nro Inicial (Rango)': 'numero_correlativo',
            'Nro Final (Rango)': 'numero_final',
            'Tipo Doc Identidad': 'tipo_documento',
            'Nro Doc Identidad': 'numero_documento',
            'Apellidos Nombres/ Razón Social': 'nombre_razon',
            'Valor Facturado Exportación': 'exportacion',
            'BI Gravada': 'bi_gravada',
            'Dscto BI': 'descuento_bi',
            'IGV / IPM': 'base_igv',
            'Dscto IGV / IPM': 'descuento_igv',
            'Mto Exonerado': 'exonerado',
            'Mto Inafecto': 'inafecto',
            'ISC': 'isc',
            'BI Grav IVAP': 'bi_ivap',
            'IVAP': 'ivap',
            'ICBPER': 'icbp',
            'Otros Tributos': 'base_otros_cargos',
            'Total CP': 'importe_total',
            'Moneda': 'tipo_moneda',
            'Tipo Cambio': 'tipo_cambio',
            'Fecha Emisión Doc Modificado': 'fecha_comprobante_modificado',
            'Tipo CP Modificado': 'tipo_comprobante_modificado',
            'Serie CP Modificado': 'numero_serie_modificado',
            'Nro CP Modificado': 'numero_correlativo_modificado',
            'ID Proyecto Operadores Atribución': 'proyecto_operadores',
            'Tipo de Nota': 'tipo_nota',
            'Est. Comp': 'estado_comprobante',
            'Valor FOB Embarcado': 'fob_embarcado',
            'Valor OP Gratuitas': 'gratuitas',
            'Tipo Operación': 'tipo_operacion',
            'DAM / CP': 'dam',
            'CLU': 'clu'
        }
        self.FINAL_COLUMNS = list(self.RENAME_MAP.values()) + ['cui', 'destino', 'valor', 'igv', 'otros_cargos']

    def get_db_mapping(self) -> Dict[str, Dict]:
        final_mapping = {col: col for col in self.FINAL_COLUMNS}
        return {'sire_ventas': {'table': '_5', 'schema': 'acc', 'columns': final_mapping}}

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

        if 'observaciones' in df.columns:
            df = df[df['observaciones'].str.len() == 27].copy()

        if 'tipo_documento' in df.columns and 'numero_documento' in df.columns and 'nombre_razon' in df.columns:
            tipo_doc_mask = df['tipo_documento'] == '-'
            df.loc[tipo_doc_mask, 'tipo_documento'] = '0'
            nro_doc_mask = (df['tipo_documento'] == '0') & (df['numero_documento'] == '-')
            df.loc[nro_doc_mask, 'numero_documento'] = df.loc[nro_doc_mask, 'nombre_razon']
        
        self._aplicar_filtro_complejo(df)
        self._convert_data_types(df)
        
        df['cui'] = df.apply(
            lambda row: self._generate_cui(
                row.get('ruc'), row.get('tipo_comprobante'), row.get('numero_serie'),
                row.get('numero_correlativo')), axis=1)
        
        final_df = df[[col for col in self.FINAL_COLUMNS if col in df.columns]].copy()
        return final_df

    def _generate_cui(self, ruc, tipo_doc, serie, numero):
        if pd.isna(ruc) or pd.isna(tipo_doc) or pd.isna(serie) or pd.isna(numero): return None
        try:
            return f"{hex(ruc)[2:].lower()}{int(tipo_doc):02d}{str(serie).strip()}{str(numero).strip()}"
        except (ValueError, TypeError): return None

    def _aplicar_filtro_complejo(self, df: pd.DataFrame) -> None:
        columnas_valor = ['bi_gravada', 'descuento_bi', 'base_igv', 'descuento_igv', 'exonerado', 'inafecto', 'bi_ivap', 'ivap', 'base_otros_cargos', 'exportacion', 'tipo_comprobante']
        for col in columnas_valor:
            if col not in df.columns: df[col] = 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        suma_exo_inaf = df['exonerado'] + df['inafecto']
        condiciones = [
            (df['tipo_comprobante'] == 7) & (df['exportacion'] < 0),
            (df['tipo_comprobante'] == 7) & (df['exportacion'] == 0),
            (df['tipo_comprobante'] != 7) & (df['exportacion'] > 0) & (df['bi_gravada'] == 0) & (df['descuento_bi'] == 0) & (df['base_igv'] == 0) & (df['descuento_igv'] == 0) & (df['exonerado'] == 0) & (df['inafecto'] == 0) & (df['bi_ivap'] == 0) & (df['ivap'] == 0),
            (df['tipo_comprobante'] != 7) & (df['exportacion'] == 0) & (df['bi_gravada'] > 0) & (df['base_igv'] > 0) & (suma_exo_inaf > 0) & (df['bi_ivap'] == 0) & (df['ivap'] == 0),
            (df['tipo_comprobante'] != 7) & (df['exportacion'] == 0) & (df['bi_gravada'] > 0) & (df['base_igv'] > 0) & (suma_exo_inaf == 0) & (df['bi_ivap'] == 0) & (df['ivap'] == 0),
            (df['tipo_comprobante'] != 7) & (df['exportacion'] == 0) & (df['bi_gravada'] == 0) & (df['descuento_bi'] == 0) & (df['base_igv'] == 0) & (df['descuento_igv'] == 0) & (suma_exo_inaf > 0) & (df['bi_ivap'] == 0) & (df['ivap'] == 0),
            (df['tipo_comprobante'] != 7) & (df['exportacion'] == 0) & (df['bi_gravada'] == 0) & (df['descuento_bi'] == 0) & (df['base_igv'] == 0) & (df['descuento_igv'] == 0) & (suma_exo_inaf == 0) & (df['bi_ivap'] > 0) & (df['ivap'] > 0)
        ]
        resultados_tipo_op = [1, 1, 17, 1, 1, 1, 1]
        resultados_destino = [1, 1, 2, 3, 1, 2, 4]
        resultados_valor = [df['bi_gravada'] + df['descuento_bi'] + df['bi_ivap'], df['exportacion'], df['exportacion'], df['bi_gravada'], df['bi_gravada'], suma_exo_inaf, df['bi_ivap']]
        resultados_igv = [df['base_igv'] + df['descuento_igv'] + df['ivap'], 0, 0, df['base_igv'], df['base_igv'], 0, df['ivap']]
        resultados_otros = [df['base_otros_cargos'], df['base_otros_cargos'], df['base_otros_cargos'], df['base_otros_cargos'] + suma_exo_inaf, df['base_otros_cargos'], df['base_otros_cargos'], df['base_otros_cargos'] + suma_exo_inaf]
        
        df['tipo_operacion'] = np.select(condiciones, resultados_tipo_op, default=99)
        df['destino'] = np.select(condiciones, resultados_destino, default=99)
        df['valor'] = np.select(condiciones, resultados_valor, default=0)
        df['igv'] = np.select(condiciones, resultados_igv, default=0)
        df['otros_cargos'] = np.select(condiciones, resultados_otros, default=df['base_otros_cargos'])
        
        if 'observaciones' in df.columns:
            df.loc[df['destino'] == 99, 'observaciones'] = df['observaciones'].astype(str) + " | Revisar dinamica de destino"

    def _convert_data_types(self, df: pd.DataFrame) -> None:
        if 'ruc' in df.columns: df['ruc'] = pd.to_numeric(df['ruc'], errors='coerce').astype('Int64')
        
        int_columns = ['periodo_tributario', 'tipo_comprobante', 'destino', 'tipo_comprobante_modificado', 'numero_final']
        for col in int_columns:
            if col in df.columns:
                if col == 'periodo_tributario':
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y%m')
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        date_columns = ['fecha_emision', 'fecha_vencimiento', 'fecha_comprobante_modificado']
        for col in date_columns:
            if col in df.columns: 
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        varchar_columns = ['numero_serie', 'numero_correlativo', 'tipo_documento', 'numero_documento', 'tipo_moneda', 
                           'numero_serie_modificado', 'numero_correlativo_modificado', 'observaciones', 'cui', 'nombre_emisor', 
                           'nombre_razon', 'proyecto_operadores', 'tipo_nota', 'estado_comprobante', 'dam', 'clu']
        for col in varchar_columns:
            if col in df.columns: 
                df[col] = df[col].astype(str).replace('nan', np.nan)

        numeric_columns = ['valor', 'igv', 'icbp', 'isc', 'otros_cargos', 'exportacion', 'bi_gravada', 'descuento_bi', 
                           'base_igv', 'descuento_igv', 'exonerado', 'inafecto', 'bi_ivap', 'ivap', 'base_otros_cargos', 
                           'importe_total', 'tipo_cambio', 'fob_embarcado', 'gratuitas']
        for col in numeric_columns:
            if col in df.columns: 
                df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
