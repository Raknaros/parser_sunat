"""
Formulario0621Processor - Procesador para declaraciones de pago PDT 621 (Archivos ZIP).

Extrae dinámicamente casillas del archivo pdt621_casillas.csv dentro del ZIP,
filtra valores nulos/cero y audita CSVs secundarios.
"""
import io
import json
import logging
import os
import zipfile
from typing import Dict, Optional

import pandas as pd

from .base_processor import BaseDocumentProcessor


class Formulario0621(BaseDocumentProcessor):
    """
    Procesador para las declaraciones de pago PDT 621 (Archivos ZIP).
    Extrae dinámicamente casillas, filtra valores nulos/cero y audita CSVs secundarios.
    """

    # Columnas esperadas en la base de datos como campos enteros (Casillas principales)
    EXPECTED_CASILLAS = {
        '_100', '_101', '_102', '_103', '_160', '_161', '_162', '_163', '_106', '_127',
        '_105', '_109', '_112', '_107', '_108', '_110', '_111', '_113', '_114', '_115',
        '_116', '_117', '_119', '_120', '_122', '_172', '_169', '_173', '_340', '_341',
        '_182', '_301', '_312', '_380', '_315', '_140', '_145', '_184', '_171', '_168',
        '_164', '_179', '_176', '_165', '_185', '_187', '_188', '_353', '_351', '_352',
        '_347', '_342', '_343', '_344', '_302', '_303', '_304', '_326', '_327', '_305',
        '_328', '_317', '_319', '_324', '_681', '_682', '_683', '_154', '_156',
    }

    # Casillas que son REAL (decimal) en la BD — no castear a int
    REAL_CASILLAS = {'_173', '_380', '_315'}

    # Casillas de metadata que ya se capturan en base_data (ruc, periodo, fecha, etc.)
    # No deben incluirse en notas ni en EXPECTED_CASILLAS.
    METADATA_CASILLAS = {'_2', '_7', '_13', '_46', '_56', '_58'}

    def __init__(self, logger: logging.Logger):
        super().__init__(logger)

    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Legacy: Lee el archivo ZIP desde disco y delega a process_content.
        """
        try:
            with open(file_path, 'rb') as f:
                file_bytes = f.read()
            filename = os.path.basename(file_path)
            return self.process_content(filename, file_bytes)
        except Exception as e:
            self.logger.error(
                f"Formulario0621: error leyendo archivo '{file_path}': {e}",
                exc_info=True,
            )
            return None

    def process_content(
        self, file_name: str, file_content: bytes
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Procesa un archivo ZIP del PDT 621 desde bytes en memoria.

        Args:
            file_name: Nombre del archivo (para logging)
            file_content: Contenido del ZIP en bytes

        Returns:
            Dict con key 'casillas' → DataFrame, o None si falla
        """
        try:
            target_df = None
            observaciones_list = []

            # 1. Iterar sobre los archivos del ZIP en memoria
            with zipfile.ZipFile(io.BytesIO(file_content), 'r') as zipf:
                for file_info in zipf.infolist():
                    filename = file_info.filename
                    basename = os.path.basename(filename)

                    if not basename.endswith('.csv'):
                        continue

                    # 1.1 Si es el archivo principal de casillas, lo extraemos completo
                    if basename.endswith('pdt621_casillas.csv'):
                        with zipf.open(filename) as csv_file:
                            # El CSV tiene una coma final en cada fila de datos,
                            # lo que pandas interpreta como una columna extra.
                            # usecols=range(8) ignora esa columna fantasma y evita
                            # que los índices de columna se desplacen.
                            target_df = pd.read_csv(csv_file, usecols=range(8))

                    # 1.2 Si es otro CSV, auditamos si tiene información
                    else:
                        with zipf.open(filename) as csv_file:
                            try:
                                temp_df = pd.read_csv(csv_file, nrows=1)
                                if not temp_df.empty:
                                    parts = basename.split('_', 1)
                                    suffix = parts[1] if len(parts) > 1 else basename
                                    observaciones_list.append(
                                        f"{suffix} TIENE INFORMACION"
                                    )
                            except pd.errors.EmptyDataError:
                                pass

            if target_df is None or target_df.empty:
                return None

            # 2. Extraer metadata base
            base_row = target_df.iloc[0]
            base_data = {
                'ruc': str(base_row.get('Nro Ruc', '')),
                'periodo_tributario': str(base_row.get('Periodo', '')),
                'numero_orden': str(base_row.get('Nro Orden', '')),
                'fecha_presentacion': str(base_row.get('Fecha Presentacion', '')),
            }

            # 3. Transponer casillas — solo filas con Nro Casilla numérico
            casillas_df = target_df[['Nro Casilla', 'Valor Casilla']].dropna(
                subset=['Nro Casilla']
            )
            casillas_dict = {}
            for _, row in casillas_df.iterrows():
                nro = str(row['Nro Casilla']).strip()
                try:
                    casilla_num = int(nro)
                    casillas_dict[f"_{casilla_num}"] = row['Valor Casilla']
                except (ValueError, TypeError):
                    # No es numérico → se ignora (metadata no relevante)
                    pass

            # 4. Agrupar columnas válidas y empaquetar desconocidas
            final_row = {**base_data}
            notas_dict = {}

            for key, value in casillas_dict.items():
                if key in self.EXPECTED_CASILLAS:
                    try:
                        if key in self.REAL_CASILLAS:
                            # Conservar como float (decimal)
                            final_row[key] = float(value) if pd.notna(value) else None
                        else:
                            # Entero
                            final_row[key] = int(float(value)) if pd.notna(value) else None
                    except (ValueError, TypeError):
                        final_row[key] = value
                elif key not in self.METADATA_CASILLAS:
                    # LÓGICA DE FILTRADO PARA NOTAS
                    # Metadata (RUC, período, fecha, etc.) ya capturada en base_data
                    if pd.notna(value):
                        try:
                            if float(value) != 0:
                                notas_dict[key] = value
                        except (ValueError, TypeError):
                            if str(value).strip() != '':
                                notas_dict[key] = value

            # 5. Formatear campos complejos (JSON y Observaciones)
            final_row['notas'] = json.dumps(notas_dict) if notas_dict else None
            final_row['observaciones'] = (
                "|".join(observaciones_list) + "|" if observaciones_list else None
            )

            # 6. Construir DataFrame final
            final_df = pd.DataFrame([final_row])

            for col in self.EXPECTED_CASILLAS:
                if col not in final_df.columns:
                    final_df[col] = None

            # 7. Retornar con key lógica para el engine
            return {"casillas": final_df}

        except Exception as e:
            raise Exception(
                f"Error procesando declaración de pago {file_name}: {str(e)}"
            )

    def get_db_mapping(self) -> Dict[str, Dict]:
        """
        Retorna el mapping de la key lógica 'casillas' a la tabla acc._9.

        Returns:
            Dict con schema, table y column mapping.
        """
        # Construir el mapping columna a columna
        columns = {
            'ruc': 'ruc',
            'periodo_tributario': 'periodo_tributario',
            'numero_orden': 'numero_orden',
            'fecha_presentacion': 'fecha_presentacion',
            'notas': 'notas',
            'observaciones': 'observaciones',
        }
        # Agregar todas las casillas esperadas
        for casilla in sorted(self.EXPECTED_CASILLAS):
            columns[casilla] = casilla

        return {
            "casillas": {
                "schema": "acc",
                "table": "_9",
                "columns": columns,
            }
        }