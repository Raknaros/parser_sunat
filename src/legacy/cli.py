"""
Legacy CLI interface for SUNAT document processing.
Preserved for backward compatibility during the migration to the API.

Usage:
    python -m src.legacy.cli "D:/path/to/sunat/files" --output_format csv
"""
import argparse
import sys
from pathlib import Path
from typing import Dict, Tuple, Optional, Pattern, List

import pandas as pd

# Legacy imports - adapted to new structure
from src.utils.logger import setup_logger
from src.utils.db_manager import DatabaseManager
from src.processors.factura_processor import FacturaProcessor
from src.processors.nota_credito_processor import NotaCreditoProcessor
from src.processors.nota_debito_processor import NotaDebitoProcessor
from src.processors.guia_remision_processor import GuiaRemisionProcessor
from src.processors.boleta_venta_processor import BoletaVentaProcessor
from src.processors.sire_compras_processor import SireComprasProcessor
from src.processors.sire_ventas_processor import SireVentasProcessor
from src.processors.planilla_processor import PlanillaProcessor
from src.processors.base_processor import BaseDocumentProcessor
from src.core.document_rules import DOCUMENT_RULES, identify_document_type
from src.config import get_settings


def process_directory(input_path: Path, output_path: Path, logger, output_format: str, db_manager: DatabaseManager = None):
    processors: Dict[str, BaseDocumentProcessor] = {
        'factura_xml': FacturaProcessor(logger),
        'sire_compras': SireComprasProcessor(logger),
        'sire_ventas': SireVentasProcessor(logger),
        'reporte_planilla_zip': PlanillaProcessor(logger),
    }

    all_results = []
    stats = {'total_files': 0, 'processed_files': 0, 'errors': 0, 'unknown_files': 0, 'by_type': {}}
    error_details = []

    all_files = [p for p in input_path.glob('**/*') if p.is_file()]
    stats['total_files'] = len(all_files)

    for file_path in all_files:
        doc_type = identify_document_type(file_path.name)
        stats['by_type'][doc_type] = stats['by_type'].get(doc_type, 0) + 1

        processor = processors.get(doc_type)
        if processor:
            try:
                result_dict = processor.process_file(str(file_path))
                if result_dict:
                    all_results.append({'processor': processor, 'data': result_dict})
                    stats['processed_files'] += 1
                else:
                    stats['errors'] += 1
                    error_details.append({
                        'archivo': file_path.name,
                        'tipo_documento': doc_type,
                        'error': 'Fallo en procesador (Ver log para detalle exacto)'
                    })
            except Exception as e:
                logger.error(f"Error crítico procesando {file_path.name}: {str(e)}", exc_info=True)
                stats['errors'] += 1
                error_details.append({
                    'archivo': file_path.name,
                    'tipo_documento': doc_type,
                    'error': str(e)
                })
        else:
            logger.warning(f"No hay procesador para '{doc_type}': {file_path.name}")
            stats['unknown_files'] += 1

    if output_format == 'csv':
        results_flat = {}
        for res in all_results:
            for key, df in res['data'].items():
                if df is not None:
                    if key not in results_flat:
                        results_flat[key] = []
                    results_flat[key].append(df)
        save_results_to_csv(results_flat, output_path, logger)

    elif output_format == 'database' and db_manager:
        save_results_to_db(all_results, db_manager, logger)

    if error_details:
        errors_df = pd.DataFrame(error_details)
        errors_file = output_path / f'errores_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv'
        errors_df.to_csv(errors_file, index=False, encoding='utf-8')
        logger.info(f"Reporte de errores generado: {errors_file}")

    stats_df = pd.DataFrame([{
        'Total_Archivos': stats['total_files'],
        'Archivos_Procesados': stats['processed_files'],
        'Errores': stats['errors'],
        'Desconocidos': stats['unknown_files'],
        **{f'Total_{k}': v for k, v in stats['by_type'].items()}
    }])
    stats_file = output_path / f'estadisticas_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv'
    stats_df.to_csv(stats_file, index=False, encoding='utf-8')
    logger.info(f"Reporte de estadísticas generado: {stats_file}")


def save_results_to_csv(results: Dict[str, List[pd.DataFrame]], output_path: Path, logger):
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    for key, df_list in results.items():
        if df_list:
            non_empty_dfs = [df for df in df_list if not df.empty]
            if non_empty_dfs:
                final_df = pd.concat(non_empty_dfs, ignore_index=True)
                output_file = output_path / f'resultados_{key}_{timestamp}.csv'
                final_df.to_csv(output_file, index=False, encoding='utf-8')
                logger.info(f"Reporte CSV '{key}' generado con {len(final_df)} filas.")


def save_results_to_db(all_results: List[Dict], db: DatabaseManager, logger):
    logger.info("Iniciando carga de datos a la base de datos...")

    grouped_by_table = {}

    for result in all_results:
        processor = result['processor']
        data_dict = result['data']

        try:
            mapping = processor.get_db_mapping()
        except (NotImplementedError, AttributeError):
            continue

        for key, df in data_dict.items():
            if df is None or df.empty or key not in mapping:
                continue

            mapping_info = mapping[key]
            table = mapping_info['table']
            schema = mapping_info['schema']
            columns = mapping_info['columns']
            destination = (schema, table)

            if destination not in grouped_by_table:
                grouped_by_table[destination] = {
                    'dfs': [],
                    'columns': columns,
                }

            grouped_by_table[destination]['dfs'].append(df)

    for (schema, table), group in grouped_by_table.items():
        if not group['dfs']:
            continue

        full_df = pd.concat(group['dfs'], ignore_index=True)
        columns = group['columns']
        db.insert_dataframe(full_df, schema, table, columns)


def main():
    parser = argparse.ArgumentParser(description='Procesador de documentos SUNAT en lote.')
    parser.add_argument('input_dir', type=str, help='Directorio con los archivos a procesar.')
    parser.add_argument('--output_dir', type=str, default='output', help='Directorio para los resultados.')
    parser.add_argument('--output_format', type=str, choices=['csv', 'database'], default='csv', help="Formato de salida.")
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).resolve().parent.parent.parent
    input_path = Path(args.input_dir)
    output_path = script_dir / args.output_dir
    log_path = script_dir / 'logs'
    
    if not input_path.is_dir():
        print(f"Error: El directorio de entrada '{input_path}' no existe.", file=sys.stderr)
        sys.exit(1)
    
    output_path.mkdir(parents=True, exist_ok=True)
    log_path.mkdir(parents=True, exist_ok=True)
    
    logger = setup_logger(log_path)
    logger.info(f"Iniciando procesamiento. Directorio: {input_path}, Salida: {args.output_format}")

    db_manager = None
    if args.output_format == 'database':
        try:
            settings = get_settings()
            db_manager = DatabaseManager(db_uri=settings.db_uri, logger=logger)
            db_manager.connect()
        except (ConnectionError, ValueError) as e:
            logger.error(f"No se pudo iniciar la conexión a la base de datos: {e}. Abortando.")
            sys.exit(1)

    process_directory(input_path, output_path, logger, args.output_format, db_manager)
    
    if db_manager:
        db_manager.disconnect()
        
    logger.info("Procesamiento completado.")


if __name__ == '__main__':
    main()