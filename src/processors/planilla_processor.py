import os
import zipfile
import pandas as pd
import io
import logging
from typing import List, Dict, Optional

from .base_processor import BaseDocumentProcessor

class PlanillaProcessor(BaseDocumentProcessor):
    """
    Procesa archivos ZIP de reportes de planilla (T-Registro), extrayendo
    múltiples tipos de reportes (TRA, IDE, SSA) en DataFrames separados.
    """
    def __init__(self, logger: logging.Logger):
        super().__init__(logger)
        # Mapeos de columnas para cada tipo de reporte
        self.DB_MAPPINGS = {
            "TRA": {
                "table": "tra",
                "schema": "payroll",
                "columns": {
                    "Tipo Doc": "tipo_documento_id", "Nro Doc": "numero_documento",
                    "ApePat": "apellido_paterno", "ApeMat": "apellido_materno",
                    "Nombres": "nombres", "FecNac": "fecha_nacimiento",
                    "ruc": "ruc_empresa", "timestamp": "fecha_reporte"
                }
            },
            "IDE": {
                "table": "ide",
                "schema": "payroll",
                "columns": {
                    "Tipo Doc": "tipo_documento_id", "Nro Doc": "numero_documento",
                    "Fec Ini Lab": "fecha_inicio_laboral",
                    "ruc": "ruc_empresa", "timestamp": "fecha_reporte"
                }
            },
            "SSA": {
                "table": "ssa",
                "schema": "payroll",
                "columns": {
                    "Tipo Doc": "tipo_documento_id", "Nro Doc": "numero_documento",
                    "ruc": "ruc_empresa", "timestamp": "fecha_reporte"
                }
            }
        }

    def get_db_mapping(self) -> Dict[str, Dict]:
        return self.DB_MAPPINGS

    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        file_name = os.path.basename(file_path)
        self.log_operation("Procesamiento Planilla", "Iniciado", f"Archivo: {file_name}")

        try:
            if not os.path.exists(file_path):
                self.logger.error(f"Archivo no encontrado: {file_path}")
                return None

            datos_por_reporte = {"TRA": [], "IDE": [], "SSA": []}
            cabeceras_por_reporte = {}

            with zipfile.ZipFile(file_path, 'r') as z:
                for internal_file_name in z.namelist():
                    try:
                        reporte_tipo = internal_file_name[12:15]
                        if reporte_tipo not in datos_por_reporte:
                            continue
                    except IndexError:
                        continue

                    with io.TextIOWrapper(z.open(internal_file_name, 'r'), encoding='latin-1') as txt_file:
                        filas_de_datos, ruc, timestamp = [], None, None
                        for index, line in enumerate(txt_file):
                            if index == 2:
                                ruc = line.split(':', 1)[-1].strip()
                            elif index == 4:
                                ts_str = line.split(':', 1)[-1].strip()
                                timestamp = pd.to_datetime(ts_str, format='%d/%m/%Y %H:%M:%S', errors='coerce')
                            elif index == 9:
                                if reporte_tipo not in cabeceras_por_reporte:
                                    cabeceras_por_reporte[reporte_tipo] = [h.strip() for h in line.split('|')]
                            elif index >= 11 and line.strip():
                                filas_de_datos.append([e.strip() for e in line.split('|')])
                        
                        for fila in filas_de_datos:
                            fila.extend([ruc, timestamp])
                        datos_por_reporte[reporte_tipo].extend(filas_de_datos)

            dataframes_finales = {}
            for reporte_tipo, filas in datos_por_reporte.items():
                if not filas: continue
                
                columnas_base = cabeceras_por_reporte.get(reporte_tipo)
                if not columnas_base:
                    self.logger.error(f"No se encontró cabecera para el reporte '{reporte_tipo}' en {file_name}.")
                    continue
                
                columnas_totales = columnas_base + ['ruc', 'timestamp']
                df = pd.DataFrame(filas, columns=columnas_totales)
                dataframes_finales[reporte_tipo] = df
            
            self.log_operation("Procesamiento Planilla", "Éxito", f"Archivo: {file_name}, Reportes encontrados: {list(dataframes_finales.keys())}")
            return dataframes_finales

        except zipfile.BadZipFile:
            self.log_operation("Procesamiento Planilla", "Error", f"El archivo {file_name} está corrupto.", level=logging.ERROR)
            return None
        except Exception as e:
            self.log_operation("Procesamiento Planilla", "Error", f"Error inesperado en {file_name}: {e}", level=logging.ERROR)
            return None
