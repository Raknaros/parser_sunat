import argparse
from pathlib import Path
import pandas as pd
from utils.logger import setup_logger
from processors.factura_processor import FacturaProcessor
from processors.nota_credito_processor import NotaCreditoProcessor
from processors.nota_debito_processor import NotaDebitoProcessor
from processors.guia_remision_processor import GuiaRemisionProcessor
from processors.boleta_venta_processor import BoletaVentaProcessor
import sys
from datetime import datetime

def identify_document_type(file_path: Path) -> str:
    """Identifica el tipo de documento basado en el contenido del XML"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().lower()
            if 'factura' in content:
                return 'Factura'
            elif 'notacredito' in content:
                return 'NotaCredito'
            elif 'notadebito' in content:
                return 'NotaDebito'
            elif 'guiaremision' in content:
                return 'GuiaRemision'
            elif 'boletaventa' in content:
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
    all_results = []
    
    # Estadísticas
    stats = {
        'total_files': 0,
        'processed_files': 0,
        'errors': 0,
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
                result_df = processors[doc_type].process_file(file_path)
                if not result_df.empty:
                    all_results.append(result_df)
                    stats['processed_files'] += 1
                else:
                    stats['errors'] += 1
            else:
                logger.warning(f"No hay procesador para el tipo: {doc_type} - Archivo: {file_path}")
                stats['errors'] += 1
                
        except Exception as e:
            logger.error(f"Error procesando {file_path}: {str(e)}")
            stats['errors'] += 1
    
    # Generar reporte de datos
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_path / f'resultados_{timestamp}.csv'
        final_df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Reporte de datos generado: {output_file}")
    
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
    input_path = Path(args.input_dir)
    output_path = Path(args.output_dir)
    log_path = Path('logs')
    
    # Verificar directorio de entrada
    if not input_path.exists():
        print(f"Error: El directorio {input_path} no existe")
        sys.exit(1)
    
    # Crear directorios de salida si no existen
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Configurar logger
    logger = setup_logger(log_path)
    logger.info(f"Iniciando procesamiento - Directorio: {input_path}")
    
    # Procesar archivos
    process_directory(input_path, output_path, logger)
    
    logger.info("Procesamiento completado")

if __name__ == '__main__':
    main() 