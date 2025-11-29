import os
import zipfile
import logging
import numpy as np
import pandas as pd
from io import StringIO
from typing import Optional, Dict, List

from .base_processor import BaseDocumentProcessor

class SireVentasProcessor(BaseDocumentProcessor):
    def __init__(self, logger: logging.Logger):
        super().__init__(logger)
        self.RENAME_MAP = {
            'Ruc': 'ruc', 'Periodo': 'periodo_tributario', 'CAR SUNAT': 'observaciones',
            'Fecha de emisión': 'fecha_emision', 'Fecha Vcto/Pago': 'fecha_vencimiento',
            'Tipo CP/Doc.': 'tipo_comprobante', 'Serie del CDP': 'numero_serie',
            'Nro CP o Doc. Nro Inicial (Rango)': 'numero_correlativo', 'Nro Final (Rango)': 'numero_final',
            'Tipo Doc Identidad': 'tipo_documento', 'Nro Doc Identidad': 'numero_documento',
            'Apellidos Nombres/ Razón Social': 'nombre_receptor',
            'Valor Facturado Exportación': 'valor_exportacion', 'BI Gravada': 'bi_gravada',
            'Dscto BI': 'dscto_bi', 'IGV / IPM': 'igv_base', 'Dscto IGV / IPM': 'dscto_igv',
            'Mto Exonerado': 'mto_exonerado', 'Mto Inafecto': 'mto_inafecto',
            'ISC': 'isc', 'BI Grav IVAP': 'bi_ivap', 'IVAP': 'ivap', 'ICBPER': 'icbp',
            'Otros Tributos': 'otros_tributos_base', 'Importe Total': 'importe_total',
            'Moneda': 'tipo_moneda', 'Tipo de Cambio': 'tipo_cambio',
            'Tipo CP Modificado': 'tipo_comprobante_modificado',
            'Serie CP Modificado': 'numero_serie_modificado',
            'Nro CP Modificado': 'numero_correlativo_modificado',
        }
        self.FINAL_COLUMNS = [
            'ruc', 'periodo_tributario', 'observaciones', 'fecha_emision', 'fecha_vencimiento',
            'tipo_comprobante', 'numero_serie', 'numero_correlativo', 'numero_final',
            'tipo_documento', 'numero_documento', 'nombre_receptor', 'valor_exportacion',
            'importe_total', 'isc', 'icbp', 'tipo_moneda', 'tipo_cambio',
            'tipo_comprobante_modificado', 'numero_serie_modificado', 'numero_correlativo_modificado',
            'destino', 'valor', 'igv', 'otros_cargos', 'tipo_operacion', 'cui'
        ]

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
        # --- REGLA DE DOCUMENTOS AÑADIDA ---
        if 'Tipo Doc Identidad' in df.columns and 'Nro Doc Identidad' in df.columns and 'Apellidos Nombres/ Razón Social' in df.columns:
            tipo_doc_mask = df['Tipo Doc Identidad'] == '-'
            df.loc[tipo_doc_mask, 'Tipo Doc Identidad'] = '0'
            nro_doc_mask = (df['Tipo Doc Identidad'] == '0') & (df['Nro Doc Identidad'] == '-')
            df.loc[nro_doc_mask, 'Nro Doc Identidad'] = df.loc[nro_doc_mask, 'Apellidos Nombres/ Razón Social']

        self._aplicar_filtro_complejo(df)
        self._convert_data_types(df)
        df.rename(columns=self.RENAME_MAP, inplace=True)
        df['cui'] = df.apply(
            lambda row: self._generate_cui(
                row.get('ruc'), row.get('tipo_comprobante'), row.get('numero_serie'),
                row.get('numero_correlativo')), axis=1)
        final_df = df[[col for col in self.FINAL_COLUMNS if col in df.columns]].copy()
        return final_df

    def _aplicar_filtro_complejo(self, df: pd.DataFrame) -> None:
        columnas_valor = ['BI Gravada', 'Dscto BI', 'IGV / IPM', 'Dscto IGV / IPM', 'Mto Exonerado', 'Mto Inafecto', 'BI Grav IVAP', 'IVAP', 'Otros Tributos', 'Valor Facturado Exportación', 'Tipo CP/Doc.']
        for col in columnas_valor:
            if col not in df.columns: df[col] = 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        suma_exo_inaf = df['Mto Exonerado'] + df['Mto Inafecto']
        condiciones = [
            (df['Tipo CP/Doc.'] == 7) & (df['Valor Facturado Exportación'] < 0),
            (df['Tipo CP/Doc.'] == 7) & (df['Valor Facturado Exportación'] == 0),
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] > 0) & (df['BI Gravada'] == 0) & (df['Dscto BI'] == 0) & (df['IGV / IPM'] == 0) & (df['Dscto IGV / IPM'] == 0) & (df['Mto Exonerado'] == 0) & (df['Mto Inafecto'] == 0) & (df['BI Grav IVAP'] == 0) & (df['IVAP'] == 0),
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] > 0) & (df['IGV / IPM'] > 0) & (suma_exo_inaf > 0) & (df['BI Grav IVAP'] == 0) & (df['IVAP'] == 0),
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] > 0) & (df['IGV / IPM'] > 0) & (suma_exo_inaf == 0) & (df['BI Grav IVAP'] == 0) & (df['IVAP'] == 0),
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] == 0) & (df['Dscto BI'] == 0) & (df['IGV / IPM'] == 0) & (df['Dscto IGV / IPM'] == 0) & (suma_exo_inaf > 0) & (df['BI Grav IVAP'] == 0) & (df['IVAP'] == 0),
            (df['Tipo CP/Doc.'] != 7) & (df['Valor Facturado Exportación'] == 0) & (df['BI Gravada'] == 0) & (df['Dscto BI'] == 0) & (df['IGV / IPM'] == 0) & (df['Dscto IGV / IPM'] == 0) & (suma_exo_inaf == 0) & (df['BI Grav IVAP'] > 0) & (df['IVAP'] > 0)
        ]
        resultados_tipo_op = [1, 1, 17, 1, 1, 1, 1]
        resultados_destino = [1, 1, 2, 3, 1, 2, 4]
        resultados_valor = [df['BI Gravada'] + df['Dscto BI'] + df['BI Grav IVAP'], df['Valor Facturado Exportación'], df['Valor Facturado Exportación'], df['BI Gravada'], df['BI Gravada'], suma_exo_inaf, df['BI Grav IVAP']]
        resultados_igv = [df['IGV / IPM'] + df['Dscto IGV / IPM'] + df['IVAP'], 0, 0, df['IGV / IPM'], df['IGV / IPM'], 0, df['IVAP']]
        resultados_otros = [df['Otros Tributos'], df['Otros Tributos'], df['Otros Tributos'], df['Otros Tributos'] + suma_exo_inaf, df['Otros Tributos'], df['Otros Tributos'], df['Otros Tributos'] + suma_exo_inaf]
        df['tipo_operacion'] = np.select(condiciones, resultados_tipo_op, default=99)
        df['destino'] = np.select(condiciones, resultados_destino, default=99)
        df['valor'] = np.select(condiciones, resultados_valor, default=0)
        df['igv'] = np.select(condiciones, resultados_igv, default=0)
        df['otros_cargos'] = np.select(condiciones, resultados_otros, default=df['Otros Tributos'])
        if 'CAR SUNAT' in df.columns:
            df.loc[df['destino'] == 99, 'CAR SUNAT'] = df['CAR SUNAT'].astype(str) + " | Revisar dinamica de destino"

    def _convert_data_types(self, df: pd.DataFrame) -> None:
        if 'Fecha de emisión' in df.columns:
            df['Fecha de emisión'] = pd.to_datetime(df['Fecha de emisión'], format='%d/%m/%Y', errors='coerce')
        if 'Fecha Vcto/Pago' in df.columns:
            df['Fecha Vcto/Pago'] = pd.to_datetime(df['Fecha Vcto/Pago'], format='%d/%m/%Y', errors='coerce')
        if 'Periodo' in df.columns:
            df['Periodo'] = pd.to_datetime(df['Periodo'], format='%Y%m', errors='coerce').dt.strftime('%Y%m')
        if 'Tipo Doc Identidad' in df.columns:
            df['Tipo Doc Identidad'] = pd.to_numeric(df['Tipo Doc Identidad'], errors='coerce')

    def _generate_cui(self, ruc, tipo_doc, serie, numero):
        if pd.isna(ruc) or pd.isna(tipo_doc) or pd.isna(serie) or pd.isna(numero): return None
        try:
            serie_fmt = str(serie).strip()
            numero_fmt = str(numero).strip()
            full_numero = f"{serie_fmt}-{numero_fmt}"
            return f"{hex(int(ruc))[2:].lower()}{int(tipo_doc):02d}{full_numero.replace('-', '')}"
        except (ValueError, TypeError): return None
