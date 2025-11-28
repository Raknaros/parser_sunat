import argparse
import re
from pathlib import Path
import pandas as pd
from utils.logger import setup_logger
from utils.db_manager import DatabaseManager
from processors.factura_processor import FacturaProcessor
from processors.nota_credito_processor import NotaCreditoProcessor
from processors.nota_debito_processor import NotaDebitoProcessor
from processors.guia_remision_processor import GuiaRemisionProcessor
from processors.boleta_venta_processor import BoletaVentaProcessor
from processors.sire_compras_processor import SireComprasProcessor
from processors.sire_ventas_processor import SireVentasProcessor
from processors.planilla_processor import PlanillaProcessor # <-- Importado
from processors.base_processor import BaseDocumentProcessor
import sys
from datetime import datetime
from typing import Dict, Tuple, Optional, Pattern, List

DOCUMENT_RULES: Dict[str, Tuple[Pattern, list]] = {
    "declaraciones_pagos": (re.compile(r"^DetalleDeclaraciones_(\d{11})_(\d{14})\.(xlsx)$", re.IGNORECASE), ["ruc", "timestamp", "ext"]),
    "guia_remision_xml": (re.compile(r"^(\d{11})-09-([A-Z0-9]{4})-(\d{1,8})\.(xml)$", re.IGNORECASE), ["ruc", "serie", "correlativo", "ext"]),
    "sire_compras": (re.compile(r"^(\d{11})-\d{8}-\d{4,6}-propuesta\.(zip|txt)$", re.IGNORECASE), ["ruc"]),
    "sire_ventas": (re.compile(r"^LE(\d{11})\d{6}1?\d+EXP2\.(zip|txt)$", re.IGNORECASE), ["ruc"]),
    "factura_xml": (re.compile(r"^FACTURA([A-Z0-9]{4})-?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "boleta_xml": (re.compile(r"^BOLETA([A-Z0-9]{4})-(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "credito_xml": (re.compile(r"^NOTA_CREDITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "debito_xml": (re.compile(r"^NOTA_DEBITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE), ["serie", "correlativo", "ruc", "ext"]),
    "recibo_xml": (re.compile(r"^RHE(\d{11})(\d{1,8})\.(xml)$", re.IGNORECASE), ["ruc", "correlativo", "ext"]),
    "reporte_planilla_zip": (re.compile(r"^(\d{11})_([A-Z]{3})_(\d{8})\.(zip)$", re.IGNORECASE), ["ruc", "codigo", "fecha", "ext"]), # <-- Regla para Planilla
}

def identify_document_type(file_path: Path) -> Optional[str]:
    filename = file_path.name
    for doc_type, (pattern, _) in DOCUMENT_RULES.items():
        if pattern.match(filename):
            return doc_type
    return 'desconocido'

def process_directory(input_path: Path, output_path: Path, logger, output_format: str, db_manager: Optional[DatabaseManager] = None):
    processors: Dict[str, BaseDocumentProcessor] = {
        'factura_xml': FacturaProcessor(logger),
        'credito_xml': NotaCreditoProcessor(logger),
        'debito_xml': NotaDebitoProcessor(logger),
        'guia_remision_xml': GuiaRemisionProcessor(logger),
        'boleta_xml': BoletaVentaProcessor(logger),
        'sire_compras': SireComprasProcessor(logger),
        'sire_ventas': SireVentasProcessor(logger),
        'reporte_planilla_zip': PlanillaProcessor(logger), # <-- Registrado
    }
    
    results: Dict[str, List[pd.DataFrame]] = {}
    stats = {'total_files': 0, 'processed_files': 0, 'errors': 0, 'unknown_files': 0, 'by_type': {}}
    
    all_files = [p for p in input_path.glob('**/*') if p.is_file()]
    stats['total_files'] = len(all_files)
    
    for file_path in all_files:
        doc_type = identify_document_type(file_path)
        stats['by_type'][doc_type] = stats['by_type'].get(doc_type, 0) + 1
        
        if doc_type in processors:
            try:
                result_dict = processors[doc_type].process_file(str(file_path))
                if result_dict:
                    for key, df in result_dict.items():
                        if df is not None:
                            if key not in results: results[key] = []
                            results[key].append(df)
                    stats['processed_files'] += 1
                else:
                    stats['errors'] += 1
            except Exception as e:
                logger.error(f"Error crítico procesando {file_path.name}: {str(e)}", exc_info=True)
                stats['errors'] += 1
        else:
            logger.warning(f"No hay procesador para '{doc_type}': {file_path.name}")
            stats['unknown_files'] += 1

    if output_format == 'csv':
        save_results_to_csv(results, output_path, logger)
    elif output_format == 'database' and db_manager:
        save_results_to_db(results, db_manager, processors, logger)

    stats_df = pd.DataFrame([{'Total_Archivos': stats['total_files'], 'Archivos_Procesados': stats['processed_files'], 'Errores': stats['errors'], 'Desconocidos': stats['unknown_files'], **{f'Total_{k}': v for k, v in stats['by_type'].items()}}])
    stats_file = output_path / f'estadisticas_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    stats_df.to_csv(stats_file, index=False, encoding='utf-8')
    logger.info(f"Reporte de estadísticas generado: {stats_file}")

def save_results_to_csv(results: Dict[str, List[pd.DataFrame]], output_path: Path, logger):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for key, df_list in results.items():
        if df_list:
            non_empty_dfs = [df for df in df_list if not df.empty]
            if non_empty_dfs:
                final_df = pd.concat(non_empty_dfs, ignore_index=True)
                output_file = output_path / f'resultados_{key}_{timestamp}.csv'
                final_df.to_csv(output_file, index=False, encoding='utf-8')
                logger.info(f"Reporte CSV '{key}' generado con {len(final_df)} filas.")

def save_results_to_db(results: Dict[str, List[pd.DataFrame]], db: DatabaseManager, processors: Dict[str, BaseDocumentProcessor], logger):
    logger.info("Iniciando carga de datos a la base de datos...")
    
    all_mappings = {}
    for proc in processors.values():
        # Algunos procesadores simples pueden no tener mapeo de BD
        try:
            all_mappings.update(proc.get_db_mapping())
        except (NotImplementedError, AttributeError):
            continue

    for key, df_list in results.items():
        if not df_list or key not in all_mappings:
            continue
        
        mapping_info = all_mappings[key]
        table = mapping_info['table']
        schema = mapping_info['schema']
        columns = mapping_info['columns']
        
        full_df = pd.concat([df for df in df_list if not df.empty], ignore_index=True)
        if full_df.empty:
            continue

        if table == 'cabeceras' and 'cui' in columns.values():
            cui_keys = full_df['CUI'].dropna().tolist()
            existing_cuis = db.check_records_exist(schema, table, 'cui', cui_keys)
            
            new_records_df = full_df[~full_df['CUI'].isin(existing_cuis)]
            
            if not new_records_df.empty:
                logger.info(f"Tabla '{table}': {len(existing_cuis)} registros ya existían. Insertando {len(new_records_df)} nuevos.")
                db.insert_dataframe(new_records_df, schema, table, columns)
            else:
                logger.info(f"Tabla '{table}': No hay registros nuevos que insertar.")
        else:
            db.insert_dataframe(full_df, schema, table, columns)

def main():
    parser = argparse.ArgumentParser(description='Procesador de documentos SUNAT en lote.')
    parser.add_argument('input_dir', type=str, help='Directorio con los archivos a procesar.')
    parser.add_argument('--output_dir', type=str, default='output', help='Directorio para los resultados.')
    parser.add_argument('--output_format', type=str, choices=['csv', 'database'], default='csv', help="Formato de salida.")
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent.parent
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
            db_manager = DatabaseManager(logger)
            db_manager.connect()
        except ConnectionError as e:
            logger.error(f"No se pudo iniciar la conexión a la base de datos: {e}. Abortando.")
            sys.exit(1)

    process_directory(input_path, output_path, logger, args.output_format, db_manager)
    
    if db_manager:
        db_manager.disconnect()
        
    logger.info("Procesamiento completado.")

if __name__ == '__main__':
    main()
