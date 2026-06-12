# ARCHITECTURE BLUEPRINT: ELT Parser API (SUNAT)

## 1. Resumen Arquitectónico
Este proyecto transforma un script local de parseo de archivos SUNAT en una **API REST (FastAPI) Cloud-Native**. Actúa como el motor de extracción (Extract) y carga (Load) en un pipeline ELT, delegando la orquestación a un sistema externo (Celery) y la organización/transformación final a la base de datos PostgreSQL.

### Principios de Diseño Clave (Instrucciones para el Agente IA)
1. **In-Memory Processing:** Cero dependencias del disco duro local. Los archivos se descargan desde S3 directamente a la memoria RAM (`bytes` o `io.BytesIO`) y se parsean allí.
2. **Asincronía (Patrón Request-Reply):** La API recibe la solicitud, encola el proceso en `BackgroundTasks`, y responde un HTTP 202 inmediatamente para evitar Timeouts. Al finalizar, envía un Webhook al orquestador.
3. **Paralelismo de I/O:** Las descargas de S3 y el parseo XML/ZIP se paralelizan usando `concurrent.futures.ThreadPoolExecutor`.
4. **Filtrado Regex en Memoria:** Dado que S3 usa prefijos planos, Python listará todas las llaves de la carpeta de trabajo (`unparsing/`) y filtrará dinámicamente usando expresiones regulares antes de descargar los archivos completos.
5. **Carga Masiva (Bulk Insert):** Las inserciones a base de datos deben usar métodos masivos (`execute_values` de `psycopg2` o `to_sql(method='multi')` de `pandas`) sin chequear existencia previa (la BD manejará duplicados en fases posteriores).

---

## 2. Estructura de Directorios (Fase 1)

El agente debe inicializar o reestructurar el proyecto bajo la siguiente jerarquía:

```text
parser_sunat/
├── requirements.txt         # fastapi, uvicorn, boto3, httpx, pydantic-settings, lxml, pandas, psycopg2-binary
├── .env                     # Entorno local (Ignorado en git)
├── src/
│   ├── main.py              # Punto de entrada ASGI (FastAPI)
│   ├── config.py            # Manejo de variables con Pydantic BaseSettings
│   ├── api/
│   │   ├── dependencies.py  # Autenticación (API Key)
│   │   ├── routers.py       # Endpoints
│   │   └── schemas.py       # Modelos Pydantic para Request/Response
│   ├── core/
│   │   ├── engine.py        # Pipeline ELT y paralelismo
│   │   └── notifier.py      # Envío de Webhooks
│   ├── storage/
│   │   └── s3_storage.py    # Abstracción de boto3
│   ├── utils/
│   │   ├── db_manager.py    # Refactorizado para inserciones masivas
│   │   ├── logger.py        # Logging estándar de Python
│   │   └── xml_utils.py     # Utilidades de parsing
│   └── processors/
│       ├── base_processor.py # Interfaz abstracta actualizada para recibir bytes
│       └── ...              # Clases específicas de negocio (Factura, Sire, Planilla, etc.)

Configuración del Entorno (src/config.py)
Utilizar pydantic_settings. Variables requeridas:

S3_BUCKET_NAME

S3_ACCESS_KEY

S3_SECRET_KEY

S3_ENDPOINT_URL (Opcional, para MinIO on-premise)

API_SECRET_KEY (Para validación del header X-API-Key)

DB_URI

MAX_WORKERS (Default: 10)

3. Contratos de la API (Fase 2)
Autenticación: Header obligatorio X-API-Key: <API_SECRET_KEY>.

Ruta: POST /api/v1/jobs/parse

Request Schema (src/api/schemas.py):
class ParseFilters(BaseModel):
    ruc: Optional[str] = None
    tipo_archivo: Optional[str] = None

class ParseJobRequest(BaseModel):
    prefix: str = "unparsing/"  # Prefijo base en S3 a listar
    webhook_url: AnyHttpUrl
    filters: Optional[ParseFilters] = None
    job_metadata: Optional[Dict[str, Any]] = None

Response Schema (Inmediato - Status 202):
class JobAcceptedResponse(BaseModel):
    status: str = "processing"
    message: str

Comportamiento del Router (src/api/routers.py):
Debe inyectar engine.run_pipeline(request.dict()) en el objeto BackgroundTasks de FastAPI antes de retornar la respuesta.

4. Capa de Infraestructura: S3 Storage (Fase 3)
Archivo: src/storage/s3_storage.py
Crear una clase S3Storage usando boto3. Métodos requeridos:

list_keys(self, prefix: str) -> List[str]: Retorna una lista de strings con las llaves completas bajo ese prefijo. Debe manejar paginación si hay más de 1000 objetos (ContinuationToken).

get_bytes(self, key: str) -> bytes: Ejecuta get_object y retorna .read(). Asegurarse de manejar excepciones de red.

move_object(self, src_key: str, dest_key: str):

Ejecuta self.client.copy_object(Bucket=..., CopySource={'Bucket': ..., 'Key': src_key}, Key=dest_key)

Ejecuta self.client.delete_object(Bucket=..., Key=src_key)

5. Refactorización de Procesadores & Base de Datos (Fase 4)
Base de datos (src/utils/db_manager.py):

Eliminar o deprecar la función check_records_exist.

Modificar insert_dataframe para que dependa exclusivamente de Bulk Inserts sin control de colisión en código Python.

Procesadores (src/processors/base_processor.py):

Cambiar la firma de process_file a:
def process_content(self, file_name: str, file_content: bytes) -> Optional[Dict[str, pd.DataFrame]]:

Crítico: Las clases hijas deben actualizarse. Si procesan XML, usar lxml.etree.fromstring(file_content). Si procesan ZIP, usar zipfile.ZipFile(io.BytesIO(file_content)). No instanciar pathlib.Path.

6. Motor Core y Webhooks (Fase 5 - El Corazón del Sistema)
A. Filtrado y Ejecución (src/core/engine.py)
La función principal run_pipeline(request_data: dict) debe seguir este flujo imperativo:

Listado: Llamar a S3Storage.list_keys(prefix=request_data["prefix"]).

Filtrado en Memoria (Regex):

Iterar sobre las llaves. Evaluar cada nombre de archivo usando el diccionario DOCUMENT_RULES (Importarlo desde donde esté definido).

Extraer grupos regex (ej. ruc, tipo_archivo).

Aplicar los filters del request. Si un archivo no cumple los filtros de RUC o tipo solicitados, se ignora y no se añade a la cola.

Resultado: Una lista de tuplas (s3_key, tipo_archivo_detectado).

Mapeo Concurrente:

Usar concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_WORKERS).

Para cada tupla, descargar bytes con get_bytes, pasar a process_content del procesador correspondiente, y devolver el DataFrame resultante (o atrapar y guardar la Excepción para métricas).

Carga de Datos:

Agrupar todos los DataFrames resultantes por nombre de tabla destino y enviarlos a db_manager.bulk_insert().

Organización Post-Análisis (Limpieza):

Por cada archivo procesado con éxito: mover de unparsing/archivo.ext a parsed/archivo.ext mediante S3Storage.move_object.

Por cada archivo con error: mover a failed/archivo.ext.

B. Notificación de Webhook (src/core/notifier.py)
Crear función send_webhook(...) utilizando la librería httpx de forma síncrona o asíncrona.
Al finalizar el pipeline, armar este payload y hacer un POST a request_data["webhook_url"]:
{
  "status": "completed",
  "job_metadata": { "insertar_aqui_el_objeto_recibido": "intacto" },
  "metrics": {
    "total_files_scanned": 5000,
    "total_files_matched_filters": 100,
    "successfully_parsed": 98,
    "failed": 2,
    "duration_seconds": 15.2
  },
  "details": {
    "successful_keys": ["parsed/factura1.xml", "..."],
    "failed_keys": ["failed/corrupt.zip", "..."]
  }
}