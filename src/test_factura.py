import logging
import pandas as pd
from src.processors.factura_processor import FacturaProcessor

# 1. Configurar un logger básico para ver los mensajes del procesador
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_individual')

# 2. --- MODIFICA AQUÍ ---
#    Reemplaza esta ruta con la ruta absoluta de tu archivo XML de prueba.
#    Ejemplo en Windows: "C:\\Users\\TuUsuario\\Desktop\\factura_prueba.xml"
#    Ejemplo en Linux/Mac: "/home/tu_usuario/documentos/factura_prueba.xml"
RUTA_DEL_ARCHIVO_XML = "D:/parser_sunat/ruta/a/tu/archivo.xml" 

def probar_un_archivo():
    """
    Script para procesar un único archivo XML y mostrar los resultados.
    """
    print(f"Iniciando prueba con el archivo: {RUTA_DEL_ARCHIVO_XML}")

    # 3. Inicializar el procesador
    procesador = FacturaProcessor(logger)

    # Configurar pandas para mostrar todas las columnas en la consola
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    try:
        # 4. Llamar al método de procesamiento
        resultados_dict = procesador.process_file(RUTA_DEL_ARCHIVO_XML)

        # 5. Imprimir los resultados
        if resultados_dict:
            print("\n--- PROCESAMIENTO EXITOSO ---")
            
            header_df = resultados_dict.get('header')
            if header_df is not None and not header_df.empty:
                print("\n[ CABECERA ]")
                print(header_df.to_string())
            
            lines_df = resultados_dict.get('lines')
            if lines_df is not None and not lines_df.empty:
                print("\n[ LÍNEAS DE DETALLE ]")
                print(lines_df.to_string())

            payment_terms_df = resultados_dict.get('payment_terms')
            if payment_terms_df is not None and not payment_terms_df.empty:
                print("\n[ TÉRMINOS DE PAGO ]")
                print(payment_terms_df.to_string())

            despatch_df = resultados_dict.get('despatch_references')
            if despatch_df is not None and not despatch_df.empty:
                print("\n[ GUÍAS DE REMISIÓN ASOCIADAS ]")
                print(despatch_df.to_string())

        else:
            print("\n--- PROCESAMIENTO FALLIDO ---")
            print("El método process_file devolvió None. Revisa los logs para más detalles.")

    except FileNotFoundError:
        print(f"\n--- ERROR ---")
        print(f"El archivo no se encontró en la ruta especificada: {RUTA_DEL_ARCHIVO_XML}")
        print("Por favor, verifica que la ruta y el nombre del archivo sean correctos.")
    except Exception as e:
        print(f"\n--- ERROR INESPERADO ---")
        logger.error(f"Ocurrió un error durante la prueba: {e}", exc_info=True)

if __name__ == "__main__":
    probar_un_archivo()
