"""
Database manager with thread-safe connection pool for PostgreSQL.

This module replaces the legacy Singleton pattern with a pool-based approach
that is safe for concurrent access via ThreadPoolExecutor.

The legacy check_records_exist() method has been removed per ARCHITECTURE_BLUEPRINT:
the database handles duplicate detection in later stages.
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List, Dict


class DatabaseManager:
    """
    Thread-safe database manager using SQLAlchemy connection pooling.

    Uses QueuePool internally which is thread-safe by default.
    To be used with concurrent.futures.ThreadPoolExecutor.
    """

    def __init__(self, db_uri: str, logger: Optional[logging.Logger] = None):
        """
        Initialize the database manager with a connection URI.

        Args:
            db_uri: PostgreSQL connection URI (e.g. postgresql://user:pass@host:5432/db)
            logger: Optional logger instance. Creates one if not provided.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.db_uri = db_uri
        self.engine: Optional[Engine] = None

    def connect(self):
        """
        Create the SQLAlchemy engine with thread-safe connection pooling.

        Uses QueuePool with:
        - pool_size=5 (base connections)
        - max_overflow=10 (additional connections when pool is exhausted)
        - pool_pre_ping=True (verify connections before use)
        """
        if self.engine is not None:
            return

        try:
            self.engine = create_engine(
                self.db_uri,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600,  # Recycle connections after 1 hour
            )
            # Verify the connection
            with self.engine.connect() as connection:
                self.logger.info(
                    "Conexión a la base de datos establecida exitosamente "
                    "(pool_size=5, max_overflow=10)."
                )
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error al conectar con la base de datos: {e}", exc_info=True
            )
            self.engine = None
            raise ConnectionError(f"No se pudo conectar a la base de datos: {e}") from e

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        column_mapping: Dict[str, str],
    ):
        """
        Insert a DataFrame into a database table using bulk insert (method='multi').

        This method does NOT check for existing records before inserting.
        Duplicate handling is delegated to the database layer (constraints, ON CONFLICT, etc.).

        Args:
            df: DataFrame with data to insert
            schema: Database schema name (e.g. 'meta', 'public')
            table: Table name (e.g. 'stg_xml_headers')
            column_mapping: Dict mapping DataFrame columns -> database columns
                           e.g. {'CUI': 'cui', 'numero': 'serie_numero'}
        """
        if not self.engine:
            self.logger.error("Database engine not initialized. Call connect() first.")
            return

        if df.empty:
            self.logger.warning(
                f"DataFrame vacío para '{schema}.{table}'. No se insertarán filas."
            )
            return

        # Rename DataFrame columns to match database column names
        df_to_load = df.rename(columns=column_mapping)

        # Only keep columns that exist in the mapping
        final_columns = list(column_mapping.values())
        existing_columns = [col for col in final_columns if col in df_to_load.columns]
        df_to_load = df_to_load[existing_columns]

        try:
            df_to_load.to_sql(
                name=table,
                con=self.engine,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",  # Bulk insert for performance
                chunksize=500,   # Insert 500 rows at a time
            )
            self.logger.info(
                f"Se insertaron {len(df_to_load)} filas en la tabla "
                f"'{schema}.{table}'."
            )
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error al insertar datos en '{schema}.{table}': {e}", exc_info=True
            )

    def execute_raw(self, query: str, params: Optional[dict] = None) -> Optional[List]:
        """
        Execute a raw SQL query (for administrative operations).

        Args:
            query: Raw SQL string
            params: Optional dictionary of parameters

        Returns:
            List of result rows, or None if the query fails.
        """
        if not self.engine:
            return None

        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                connection.commit()
                if result.returns_rows:
                    return result.fetchall()
                return []
        except SQLAlchemyError as e:
            self.logger.error(f"Error ejecutando query: {e}", exc_info=True)
            return None

    def disconnect(self):
        """Dispose the connection pool and release all resources."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.logger.info("Pool de conexiones a la base de datos cerrado.")