import os
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List

class DatabaseManager:
    """
    Gestiona la conexión y las operaciones con la base de datos.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, logger: Optional[logging.Logger] = None):
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        self.logger = logger or logging.getLogger(__name__)
        self.engine: Optional[Engine] = None
        self._initialized = False

    def connect(self):
        """
        Crea la conexión con la base de datos usando credenciales de .env.
        """
        if self.engine is not None:
            return

        load_dotenv()
        
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        if not all([db_user, db_password, db_host, db_port, db_name]):
            self.logger.error("Faltan variables de entorno para la conexión a la base de datos. Asegúrate de que .env está configurado.")
            raise ConnectionError("Faltan credenciales de base de datos en el archivo .env")

        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        try:
            self.engine = create_engine(db_url)
            # Probar la conexión
            with self.engine.connect() as connection:
                self.logger.info("Conexión a la base de datos establecida exitosamente.")
            self._initialized = True
        except SQLAlchemyError as e:
            self.logger.error(f"Error al conectar con la base de datos: {e}", exc_info=True)
            self.engine = None
            raise

    def check_records_exist(self, schema: str, table: str, unique_key_column: str, keys: List[str]) -> List[str]:
        """
        Verifica qué claves de una lista ya existen en una tabla.

        Args:
            schema: El esquema de la tabla.
            table: El nombre de la tabla.
            unique_key_column: La columna que contiene la clave única (ej. 'cui').
            keys: Una lista de claves a verificar.

        Returns:
            Una lista de las claves que SÍ existen en la tabla.
        """
        if not self.engine or not keys:
            return []
        
        try:
            with self.engine.connect() as connection:
                query = text(f'SELECT "{unique_key_column}" FROM "{schema}"."{table}" WHERE "{unique_key_column}" IN :keys')
                result = connection.execute(query, {'keys': tuple(keys)})
                existing_keys = [row[0] for row in result]
                return existing_keys
        except SQLAlchemyError as e:
            self.logger.error(f"Error al verificar registros en {schema}.{table}: {e}", exc_info=True)
            return []

    def insert_dataframe(self, df: pd.DataFrame, schema: str, table: str, column_mapping: dict):
        """
        Inserta un DataFrame en la base de datos, renombrando las columnas según el mapeo.
        """
        if not self.engine or df.empty:
            return

        df_to_load = df.rename(columns=column_mapping)
        
        # Asegurarse de que solo se insertan las columnas que existen en el mapeo
        final_columns = list(column_mapping.values())
        df_to_load = df_to_load[[col for col in final_columns if col in df_to_load.columns]]

        try:
            df_to_load.to_sql(
                name=table,
                con=self.engine,
                schema=schema,
                if_exists='append',
                index=False
            )
            self.logger.info(f"Se insertaron {len(df_to_load)} filas en la tabla '{schema}.{table}'.")
        except SQLAlchemyError as e:
            self.logger.error(f"Error al insertar datos en {schema}.{table}: {e}", exc_info=True)

    def disconnect(self):
        """Cierra la conexión del motor de la base de datos."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.logger.info("Conexión a la base de datos cerrada.")
