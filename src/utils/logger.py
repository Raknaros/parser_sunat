import logging
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
        
        # Handler para consola
        ch = logging.StreamHandler()
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