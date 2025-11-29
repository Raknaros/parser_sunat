import os
import zipfile
import logging
import numpy as np
import pandas as pd
from io import StringIO
from typing import Optional, Dict

from .base_processor import BaseDocumentProcessor

class SireVentasProcessor(BaseDocumentProcessor):
    """
    Procesa archivos de propuesta de SIRE Ventas, ya sean TXT o ZIP.
    Extrae, transforma y prepara los datos para ser cargados.
    """
    def __init__(self, logger: logging.Logger):
        super().__init__(logger)
        self.COLUMN_MAPPING = {
            'Ruc': 'ruc',
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
            'ISC': 'isc',
            'ICBPER': 'icbp',
            'Moneda': 'tipo_moneda',
            'Tipo CP Modificado': 'tipo_comprobante_modificado',
            'Serie CP Modificado': 'numero_serie_modificado',
            'Nro CP Modificado': 'numero_correlativo_modificado',
            'CUI': 'cui',
            'destino': 'destino',
            'valor': 'valor',
            'igv': 'igv',
            'otros_cargos': 'otros_cargos',
            'tipo_operacion': 'tipo_operacion'
        }

    def get_db_mapping(self) -> Dict[str, Dict]:
        final_mapping = {col: col for col in self.FINAL_COLUMNS}
        return {'sire_compras': {'table': '_5', 'schema': 'acc', 'columns': final_mapping}}

    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        file_name = os.path.basename(file_path)
        self.log_operation("Procesamiento SIRE Ventas", "Iniciado", f"Archivo: {file_name}")
        
        try:
            raw_df = self._extract_data(file_path)
            
            if raw_df is None:
                return None
            
            if raw_df.empty:
                self.logger.warning(f"El archivo SIRE '{file_name}' no contiene comprobantes de datos. Proceso exitoso (vacío).")
                return {'sire_ventas': pd.DataFrame()}

            transformed_df = self._transform_data(raw_df)
            
            self.log_operation("Procesamiento SIRE Ventas", "Éxito", f"Archivo: {file_name}, Filas procesadas: {len(transformed_df)}")
            
            return {'sire_ventas': transformed_df}

        except Exception as e:
            self.log_operation("Procesamiento SIRE Ventas", "Error", f"Error procesando {file_path}: {e}", level=logging.ERROR)
            self.logger.error(f"Detalle del error en SireVentasProcessor: {e}", exc_info=True)
            return None

    def _extract_data(self, file_path: str) -> Optional[pd.DataFrame]:
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
                                try:
                                    content = file.read().decode('utf-8')
                                except UnicodeDecodeError:
                                    file.seek(0)
                                    content = file.read().decode('latin-1', errors='replace')
                                
                                if len(content.splitlines()) < 2:
                                    continue

                                df = pd.read_csv(StringIO(content), sep='|', header=0, dtype=str)
                                lista_dataframes.append(df)
            elif file_path.lower().endswith('.txt'):
                with open(file_path, 'r', encoding='latin-1') as f:
                    if len(f.readlines()) < 2:
                        return pd.DataFrame()
                df = pd.read_csv(file_path, sep='|', header=0, dtype=str, encoding='latin-1')
                lista_dataframes.append(df)
            else:
                self.logger.warning(f"Formato no soportado por SireVentasProcessor: {file_path}")
                return None

            if not lista_dataframes:
                return pd.DataFrame()

            return pd.concat(lista_dataframes, ignore_index=True)
        except Exception as e:
            self.logger.error(f"Error en la fase de extracción para {file_path}: {e}", exc_info=True)
            return None

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df_transformado = df.copy()

        if 'CAR SUNAT' in df_transformado.columns:
            df_transformado = df_transformado[df_transformado['CAR SUNAT'].str.len() == 27].copy()

        if 'Tipo Doc Identidad' in df_transformado.columns and 'Nro Doc Identidad' in df_transformado.columns and 'Apellidos Nombres/ Razón Social' in df_transformado.columns:
            tipo_doc_mask = df_transformado['Tipo Doc Identidad'] == '-'
            df_transformado.loc[tipo_doc_mask, 'Tipo Doc Identidad'] = '0'
            nro_doc_mask = (df_transformado['Tipo Doc Identidad'] == '0') & (df_transformado['Nro Doc Identidad'] == '-')
            df_transformado.loc[nro_doc_mask, 'Nro Doc Identidad'] = df_transformado.loc[nro_doc_mask, 'Apellidos Nombres/ Razón Social']

        if 'Fecha de emisión' in df_transformado.columns:
            df_transformado['Fecha de emisión'] = pd.to_datetime(df_transformado['Fecha de emisión'], format='%d/%m/%Y', errors='coerce')
        if 'Fecha Vcto/Pago' in df_transformado.columns:
            df_transformado['Fecha Vcto/Pago'] = pd.to_datetime(df_transformado['Fecha Vcto/Pago'], format='%d/%m/%Y', errors='coerce')
        if 'Periodo' in df_transformado.columns:
            df_transformado['Periodo'] = pd.to_datetime(df_transformado['Periodo'], format='%Y%m', errors='coerce')

        columnas_monto = ['BI Gravada', 'Dscto BI', 'IGV / IPM', 'Dscto IGV / IPM', 'Mto Exonerado', 'Mto Inafecto', 'BI Grav IVAP', 'IVAP', 'ISC', 'ICBPER', 'Otros Tributos']
        for col in columnas_monto:
            if col in df_transformado.columns:
                df_transformado[col] = pd.to_numeric(df_transformado[col], errors='coerce').fillna(0)

        if 'Tipo Doc Identidad' in df_transformado.columns:
            df_transformado['Tipo Doc Identidad'] = df_transformado['Tipo Doc Identidad'].replace('-', '0')
            df_transformado['Tipo Doc Identidad'] = pd.to_numeric(df_transformado['Tipo Doc Identidad'], errors='coerce')

        df_transformado['CUI'] = df_transformado.apply(
            lambda row: self._generate_cui(
                row.get('Ruc'), row.get('Tipo CP/Doc.'), row.get('Serie del CDP'),
                row.get('Nro CP o Doc. Nro Inicial (Rango)')), axis=1)

        self._aplicar_filtro_complejo(df_transformado)
        df_renamed = self._rename_columns(df_transformado)
        return self._filter_final_columns(df_renamed)

    def _generate_cui(self, ruc, tipo_doc, serie, numero):
        if pd.isna(ruc) or pd.isna(tipo_doc) or pd.isna(serie) or pd.isna(numero): return None
        try:
            tipo_doc_str = str(tipo_doc).strip()
            serie_fmt = str(serie).strip()
            numero_fmt = str(numero).strip()
            full_numero = f"{serie_fmt}-{numero_fmt}"
            return f"{hex(int(ruc))[2:].lower()}{int(float(tipo_doc_str)):02d}{full_numero.replace('-', '')}"
        except (ValueError, TypeError): return None

    def _aplicar_filtro_complejo(self, df: pd.DataFrame) -> None:
        columnas_valor = ['BI Gravada', 'Dscto BI', 'IGV / IPM', 'Dscto IGV / IPM', 'Mto Exonerado', 'Mto Inafecto', 'BI Grav IVAP', 'IVAP', 'Otros Tributos', 'Valor Facturado Exportación', 'Tipo CP/Doc.']
        for col in columnas_valor:
            if col not in df.columns:
                df[col] = 0
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

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df_renamed = df.rename(columns=self.COLUMN_MAPPING)
        if 'observaciones' in df_renamed.columns:
            df_renamed['observaciones'] = "SIRE:" + df_renamed['observaciones'].astype(str)
        return df_renamed

    def _filter_final_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columnas_finales = list(self.COLUMN_MAPPING.values())
        columnas_existentes = [col for col in columnas_finales if col in df.columns]
        df_filtrado = df[columnas_existentes].copy()
        df_filtrado = df_filtrado.replace('', np.nan).replace(' ', np.nan)
        self._convert_data_types(df_filtrado)
        return df_filtrado

    def _convert_data_types(self, df: pd.DataFrame) -> None:
        if 'ruc' in df.columns: df['ruc'] = pd.to_numeric(df['ruc'], errors='coerce').astype('Int64')
        int_columns = ['periodo_tributario', 'tipo_comprobante', 'destino', 'tasa_detraccion', 'tipo_comprobante_modificado', 'numero_final']
        for col in int_columns:
            if col in df.columns:
                if col == 'periodo_tributario':
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y%m').astype('Int64')
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        date_columns = ['fecha_emision', 'fecha_vencimiento']
        for col in date_columns:
            if col in df.columns: df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        varchar_columns = ['numero_serie', 'numero_correlativo', 'tipo_documento', 'numero_documento', 'tipo_moneda', 'numero_serie_modificado', 'numero_correlativo_modificado', 'observaciones', 'cui']
        for col in varchar_columns:
            if col in df.columns: df[col] = df[col].astype(str).replace('nan', np.nan)
        numeric_columns = ['valor', 'igv', 'icbp', 'isc', 'otros_cargos']
        for col in numeric_columns:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
