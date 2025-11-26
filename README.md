# Parser de Documentos XML de SUNAT

Esta aplicación procesa archivos XML de documentos electrónicos (Facturas, Notas de Crédito, etc.) y genera reportes en formato CSV.

## Requisitos

- Python 3.8 o superior
- Entorno virtual (venv)
- Dependencias listadas en requirements.txt

## Uso

Para procesar documentos XML, ejecute:

`ash
python src/main.py [directorio_entrada] --output_dir [directorio_salida]
`

## Tipos de Documentos Soportados

- Factura
- Nota de Crédito
- Nota de Débito
- Guía de Remisión
- Boleta de Venta

## Estructura del Proyecto

`
parser_sunat/

 src/
    processors/         # Procesadores para cada tipo de documento
    utils/             # Utilidades (logging, etc.)
    main.py           # Punto de entrada principal

 logs/                  # Archivos de log
 output/               # Resultados generados
 venv/                 # Entorno virtual
 requirements.txt      # Dependencias
 README.md            # Este archivo
`

PROXIMOS PASOS
Verificar la muestra de error del log
Verificar la integridad de la informacion
Verificar la ejecucion mediante comando
Elaborar un release de Github, subirlo y completar la version 1.0