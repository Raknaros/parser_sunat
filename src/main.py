import argparse
from pathlib import Path
import pandas as pd
from utils.logger import setup_logger
from processors.factura_processor import FacturaProcessor
from processors.nota_credito_processor import NotaCreditoProcessor
from processors.nota_debito_processor import NotaDebitoProcessor
from processors.guia_remision_processor import GuiaRemisionProcessor
from processors.boleta_venta_processor import BoletaVentaProcessor
from utils.xml_utils import get_xml_encoding
import sys
from datetime import datetime

def identify_document_type(file_path: Path) -> str:
    """Identifica el tipo de documento basado en el nombre del archivo"""
    try:
        filename = file_path.name.upper()
        if filename.startswith('FACTURA'):
            return 'Factura'
        elif filename.startswith('NOTACREDITO'):
            return 'NotaCredito' 
        elif filename.startswith('NOTADEBITO'):
            return 'NotaDebito'
        elif filename.startswith('GUIAREMISION'):
            return 'GuiaRemision'
        elif filename.startswith('BOLETAVENTA'):
            return 'BoletaVenta'
        else:
            return 'Desconocido'
    except Exception:
        return 'Error'

def process_directory(input_path: Path, output_path: Path, logger):
    """Procesa todos los archivos XML en el directorio especificado"""
    
    # Crear procesadores
    processors = {
        'Factura': FacturaProcessor(logger),
        'NotaCredito': NotaCreditoProcessor(logger),
        'NotaDebito': NotaDebitoProcessor(logger),
        'GuiaRemision': GuiaRemisionProcessor(logger),
        'BoletaVenta': BoletaVentaProcessor(logger)
    }
    
    # Preparar DataFrame para resultados
    results = {
        'headers': [],
        'lines': [],
        'payments': [] # Y otros que puedan existir
    }
    # Estadísticas
    stats = {
        'total_files': 0,
        'processed_files': 0,
        'errors': 0,
        'unknown_files': 0,
        'by_type': {}
    }
    
    # Procesar archivos
    xml_files = list(input_path.glob('**/*.xml'))
    stats['total_files'] = len(xml_files)
    
    for file_path in xml_files:
        try:
            doc_type = identify_document_type(file_path)
            
            # Actualizar estadísticas por tipo
            stats['by_type'][doc_type] = stats['by_type'].get(doc_type, 0) + 1
            
            if doc_type in processors:
                # Decodificacion dinamica
                encoding = get_xml_encoding(str(file_path))
                with open(file_path, 'r', encoding=encoding) as f:
                    content = f.read()

                # Asumimos que process_file devuelve un diccionario de DataFrames
                result_dict = processors[doc_type].process_file(content, file_path.name)
                
                # Verificar si el resultado es un diccionario y si la cabecera no está vacía
                if result_dict and isinstance(result_dict, dict) and 'header' in result_dict and not result_dict['header'].empty:
                    results['headers'].append(result_dict['header'])
                    
                    # Añadir líneas y pagos si existen
                    if 'lines' in result_dict and not result_dict['lines'].empty:
                        results['lines'].append(result_dict['lines'])
                    if 'payment_terms' in result_dict and not result_dict['payment_terms'].empty:
                        results['payments'].append(result_dict['payment_terms'])
                    # Aquí se podrían añadir más tipos de datos si los procesadores los devuelven

                    stats['processed_files'] += 1
                else:
                    stats['errors'] += 1
            else:
                logger.warning(f"No hay procesador para el tipo: {doc_type} - Archivo: {file_path}")
                stats['unknown_files'] += 1
                
        except Exception as e:
            logger.error(f"Error procesando {file_path}: {str(e)}")
            stats['errors'] += 1
    
    # Generar reporte de datos
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if results['headers']:
        final_headers_df = pd.concat(results['headers'], ignore_index=True)
        output_file = output_path / f'resultados_cabeceras_{timestamp}.csv'
        final_headers_df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Reporte de cabeceras generado: {output_file}")

    if results['lines']:
        final_lines_df = pd.concat(results['lines'], ignore_index=True)
        output_file = output_path / f'resultados_lineas_{timestamp}.csv'
        final_lines_df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Reporte de líneas generado: {output_file}")

    if results['payments']:
        final_payments_df = pd.concat(results['payments'], ignore_index=True)
        output_file = output_path / f'resultados_pagos_{timestamp}.csv'
        final_payments_df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Reporte de pagos generado: {output_file}")
    
    # Generar reporte de estadísticas
    stats_df = pd.DataFrame([{
        'Total_Archivos': stats['total_files'],
        'Archivos_Procesados': stats['processed_files'],
        'Errores': stats['errors'],
        **{f'Total_{k}': v for k, v in stats['by_type'].items()}
    }])
    
    stats_file = output_path / f'estadisticas_{timestamp}.csv'
    stats_df.to_csv(stats_file, index=False, encoding='utf-8')
    logger.info(f"Reporte de estadísticas generado: {stats_file}")

def main():
    parser = argparse.ArgumentParser(description='Procesador de documentos XML en lote')
    parser.add_argument('input_dir', type=str, help='Directorio con los archivos XML')
    parser.add_argument('--output_dir', type=str, default='output', help='Directorio para los resultados')
    
    args = parser.parse_args()
    
    # Configurar directorios
    script_dir = Path(__file__).parent.parent
    input_path = Path(args.input_dir)
    output_path = script_dir / args.output_dir
    log_path = script_dir / 'logs'
    
    # Verificar directorio de entrada
    if not input_path.exists():
        print(f"Error: El directorio {input_path} no existe")
        sys.exit(1)
    
    # Crear directorios de salida y logs si no existen
    output_path.mkdir(parents=True, exist_ok=True)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Configurar logger
    logger = setup_logger(log_path)
    logger.info(f"Iniciando procesamiento - Directorio: {input_path}")
    
    # Procesar archivos
    process_directory(input_path, output_path, logger)
    
    logger.info("Procesamiento completado")

if __name__ == '__main__':
    main()
