import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logger(log_path: Path) -> logging.Logger:
    """Configura y retorna un logger personalizado"""
    
    # Crear el directorio de logs si no existe
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Crear logger
    logger = logging.getLogger('parser_sunat')
    logger.setLevel(logging.INFO)
    
    # Evitar duplicación de handlers
    if not logger.handlers:
        # Handler para archivo
        log_file = log_path / f'process_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setLevel(logging.INFO)
        
        # Handler para consola (stderr para visibilidad en Docker)
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(logging.INFO)
        
        # Formato
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
    
    return logger


def configure_root_logger():
    """
    Configure the root logger to ensure all loggers in the project
    (including those created via logging.getLogger(__name__)) are
    visible in Docker logs via stderr.
    
    Call this once at application startup.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Only add handler if none exist (avoid duplicates on reload)
    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
