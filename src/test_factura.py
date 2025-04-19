import logging
from pathlib import Path
import pandas as pd
import os
from datetime import datetime # Importar datetime al principio
import zipfile # Importar zipfile
import tempfile # Importar tempfile
from src.processors.factura_processor import FacturaProcessor

# --- Añadir stub de BaseXMLProcessor si no existe (para pruebas aisladas) ---
# Si tienes una clase BaseXMLProcessor real, asegúrate que sea importable
try:
    from src.processors.base_processor import BaseXMLProcessor
except ImportError:
    print("Advertencia: No se encontró BaseXMLProcessor. Usando un stub.")
    class BaseXMLProcessor:
        def __init__(self, logger):
            self.logger = logger
        def validate_xml(self, file_path):
            self.logger.info(f"Validando (stub): {file_path}")
            return True
        def log_operation(self, operation, status, message):
            self.logger.info(f"{operation} [{status}]: {message}")
# --- Fin Stub ---

# --- CONFIGURACIÓN --- 
PROCESS_ZIPS = True # Cambiar a False para desactivar el procesamiento de ZIPs
# --- FIN CONFIGURACIÓN ---

# Configurar el logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_factura')

def save_dataframe(df, base_path, filename_prefix, timestamp):
    """Intenta guardar un DataFrame en un CSV, con manejo de errores de permisos."""
    if df.empty:
        print(f"No hay datos para guardar en {filename_prefix}.csv")
        return

    output_file = base_path / f"{filename_prefix}_{timestamp}.csv"
    alt_output_file = Path(os.getcwd()) / f"{filename_prefix}_{timestamp}.csv"

    try:
        df.to_csv(output_file, index=False)
        print(f"Resultados ({filename_prefix}) guardados en: {output_file}")
    except PermissionError:
        print(f"Error de permisos al guardar {output_file}. Intentando ubicación alternativa...")
        try:
            df.to_csv(alt_output_file, index=False)
            print(f"Resultados ({filename_prefix}) guardados en ubicación alternativa: {alt_output_file}")
        except Exception as e_alt:
            print(f"Error al guardar {alt_output_file}: {str(e_alt)}")
    except Exception as e:
        print(f"Error inesperado al guardar {output_file}: {str(e)}")

def process_xml_file(processor, xml_file_path, results_collector):
    """Procesa un único archivo XML y añade los resultados al colector."""
    print(f"Procesando archivo XML: {xml_file_path.name} ... ", end="")
    try:
        result_dict = processor.process_file(xml_file_path)
        if result_dict is not None:
            if not result_dict['header'].empty:
                results_collector['headers'].append(result_dict['header'])
            if not result_dict['lines'].empty:
                results_collector['lines'].append(result_dict['lines'])
            if not result_dict['payment_terms'].empty:
                results_collector['payments'].append(result_dict['payment_terms'])
            print("OK")
            return True # Indica éxito
        else:
            print("Fallo (process_file retornó None)")
            return False
    except Exception as e:
        print(f"Error ({e})")
        logger.error(f"Error procesando {xml_file_path.name}", exc_info=True)
        return False

def main():
    xml_directory = Path("C:/Users/Raknaros/Desktop/xmlprueba")
    
    if not xml_directory.is_dir():
        print(f"ERROR: El directorio especificado no existe: {xml_directory}")
        return
    
    processor = FacturaProcessor(logger)
    
    # Diccionario para almacenar los DataFrames de cada tipo
    results = {
        'headers': [],
        'lines': [],
        'payments': []
    }
    
    # Busca archivos .xml y .zip RECURSIVAMENTE en el directorio
    files_to_check = list(xml_directory.rglob("*")) 
    
    print(f"Encontrados {len(files_to_check)} elementos recursivamente en {xml_directory}. Procesando...")
    processed_files_count = 0
    processed_zips_count = 0
    processed_xml_in_zip_count = 0

    for item_path in files_to_check:
        if item_path.is_file():
            # Procesa archivos XML sueltos
            if item_path.suffix.lower() == '.xml':
                if process_xml_file(processor, item_path, results):
                    processed_files_count += 1
            
            # Procesa archivos ZIP si está activado
            elif item_path.suffix.lower() == '.zip' and PROCESS_ZIPS:
                print(f"Procesando archivo ZIP: {item_path.name} ...")
                processed_zips_count += 1
                try:
                    with zipfile.ZipFile(item_path, 'r') as zip_ref:
                        xml_files_in_zip = [f for f in zip_ref.namelist() if f.lower().endswith('.xml')]
                        print(f"  Encontrados {len(xml_files_in_zip)} archivos XML dentro.")
                        
                        # Crear directorio temporal para extraer los XML
                        with tempfile.TemporaryDirectory() as temp_dir:
                            temp_dir_path = Path(temp_dir)
                            for xml_in_zip_name in xml_files_in_zip:
                                try:
                                    # Extraer el archivo XML actual al directorio temporal
                                    zip_ref.extract(xml_in_zip_name, temp_dir_path)
                                    extracted_xml_path = temp_dir_path / xml_in_zip_name
                                    
                                    # Procesar el archivo XML extraído
                                    print(f"  -> Procesando XML interno: {xml_in_zip_name} ...", end="")
                                    # Reutilizamos la lógica de procesamiento, pasando el colector
                                    if process_xml_file(processor, extracted_xml_path, results):
                                         processed_xml_in_zip_count += 1
                                         processed_files_count += 1 # Contar como archivo procesado exitosamente
                                    # El archivo temporal se elimina automáticamente al salir del with tempfile
                                except Exception as e_extract:
                                    print(f"  Error procesando {xml_in_zip_name} desde {item_path.name}: {e_extract}")
                                    logger.error(f"Error procesando {xml_in_zip_name} desde {item_path.name}", exc_info=True)
                except zipfile.BadZipFile:
                    print(f"Error: El archivo {item_path.name} no es un ZIP válido o está corrupto.")
                except Exception as e_zip:
                     print(f"Error inesperado procesando {item_path.name}: {e_zip}")
                     logger.error(f"Error inesperado procesando {item_path.name}", exc_info=True)

    print(f"\nProcesamiento completado.")
    print(f"- Archivos ZIP encontrados y procesados: {processed_zips_count}")
    print(f"- Archivos XML encontrados dentro de ZIPs: {processed_xml_in_zip_count}")
    print(f"- Total archivos XML procesados con éxito (sueltos + en ZIP): {processed_files_count}")

    # Si se procesaron archivos, combinar y guardar resultados
    if processed_files_count > 0:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        final_headers_df = pd.concat(results['headers'], ignore_index=True) if results['headers'] else pd.DataFrame()
        final_lines_df = pd.concat(results['lines'], ignore_index=True) if results['lines'] else pd.DataFrame()
        final_payments_df = pd.concat(results['payments'], ignore_index=True) if results['payments'] else pd.DataFrame()
        
        print("\nResumen de datos combinados:")
        print(f"- Cabeceras: {len(final_headers_df)} filas")
        print(f"- Líneas: {len(final_lines_df)} filas")
        print(f"- Términos de pago: {len(final_payments_df)} filas")
        
        print("\nGuardando resultados...")
        save_dataframe(final_headers_df, xml_directory, "resultados_cabeceras", timestamp)
        save_dataframe(final_lines_df, xml_directory, "resultados_lineas", timestamp)
        save_dataframe(final_payments_df, xml_directory, "resultados_pagos", timestamp)
        
    else:
        print("No se procesaron archivos con éxito.")

if __name__ == "__main__":
    # Configurar pandas para mostrar más columnas si es necesario
    pd.set_option('display.max_columns', None) 
    pd.set_option('display.width', 1000)
    main() 