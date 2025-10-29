# config/logging_config.py
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from . import settings

def setup_logging():
    """
    Configura o sistema de logging para a aplicação.
    - Loga para a consola em modo de desenvolvimento.
    - Loga para um ficheiro com rotação diária.
    """
    # Cria a pasta de logs se ela não existir
    settings.LOGS_DIR.mkdir(exist_ok=True)
    log_file_path = settings.LOGS_DIR / "pharma_boost.log"

    # Define o formato do log
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Obtém o logger raiz
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO) # Define o nível mínimo de log

    # Limpa handlers existentes para evitar duplicação
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # --- Handler para a Consola ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    # --- Handler para o Ficheiro ---
    # Rotação diária, mantém os últimos 7 dias de logs
    file_handler = TimedRotatingFileHandler(
        log_file_path, 
        when="midnight", 
        interval=1, 
        backupCount=7, 
        encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    logging.info("="*50)
    logging.info("Sistema de Logging Inicializado")
    logging.info("="*50)