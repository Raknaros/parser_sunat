import logging
from pathlib import Path
import pandas as pd
import os
from src.processors.factura_processor import FacturaProcessor

# Configurar el logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_factura')

def main():
    # Configurar la ruta al directorio donde están los archivos XML
    # ¡MODIFICA ESTA LÍNEA CON LA RUTA CORRECTA A TU DIRECTORIO DE FACTURAS XML!
    xml_directory = Path("C:/Users/Raknaros/Desktop/xmlprueba")
    
    # Asegurarse de que el directorio existe, si no, crearlo
    xml_directory.mkdir(parents=True, exist_ok=True)
    
    # Crear una instancia del procesador
    processor = FacturaProcessor(logger)
    
    # Procesar todos los archivos XML en el directorio
    results = []
    xml_files = list(xml_directory.glob("*.xml"))
    
    if not xml_files:
        print(f"No se encontraron archivos XML en {xml_directory}")
        return
        
    for xml_file in xml_files:
        print(f"Procesando: {xml_file}")
        df = processor.process_file(xml_file)
        if not df.empty:
            results.append(df)
            print(f"Datos extraídos de {xml_file.name}:")
            print(df.head())
            print("-" * 80)
    
    # Si se procesaron archivos con éxito, combinar resultados
    if results:
        all_results = pd.concat(results, ignore_index=True)
        
        print("\nResumen de todas las facturas procesadas:")
        print(all_results)
        
        # Intentar guardar resultados en un archivo CSV
        try:
            # Usar un nombre de archivo con timestamp para evitar colisiones
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = xml_directory / f"resultados_facturas_{timestamp}.csv"
            
            # Intentar guardar el archivo
            all_results.to_csv(output_file, index=False)
            print(f"Resultados guardados en: {output_file}")
        except PermissionError:
            # Si hay un error de permisos, intentar guardar en el directorio del proyecto
            alt_output_file = Path(os.getcwd()) / f"resultados_facturas_{timestamp}.csv"
            try:
                all_results.to_csv(alt_output_file, index=False)
                print(f"No se pudo guardar en la carpeta original debido a permisos.")
                print(f"Resultados guardados en ubicación alternativa: {alt_output_file}")
            except Exception as e:
                print(f"Error al guardar el archivo CSV: {str(e)}")
        except Exception as e:
            print(f"Error al guardar el archivo CSV: {str(e)}")
    else:
        print("No se pudieron procesar facturas XML.")

if __name__ == "__main__":
    main() 