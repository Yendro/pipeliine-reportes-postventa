"""
Configuración centralizada de logging
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
import config

class CustomLogger:
    """Logger personalizado con configuración centralizada"""
    
    def __init__(self, name=None):
        """
        Inicializa el logger
        
        Args:
            name: Nombre del módulo (opcional)
        """
        self.name = name or __name__
        self.logger = logging.getLogger(self.name)
        self._setup_logger()
    
    def _setup_logger(self):
        """Configura el logger con handlers de archivo y consola"""
        # Resetear handlers si ya existen
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        
        # Nivel de logging desde configuración
        log_level = getattr(logging, config.LOG_CONFIG['level'].upper())
        self.logger.setLevel(log_level)
        
        # Formato del log
        formatter = logging.Formatter(
            fmt=config.LOG_CONFIG['format'],
            datefmt=config.LOG_CONFIG['date_format']
        )
        
        # Handler para archivo (con rotación)
        log_file = Path(config.LOG_CONFIG['file'])
        log_file.parent.mkdir(exist_ok=True)
        
        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=10*1024*1024,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        
        # Handler para consola
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        
        # Agregar handlers al logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_logger(self):
        """Retorna el objeto logger configurado"""
        return self.logger
    
    # Métodos de conveniencia
    def debug(self, message, exc_info=False):
        self.logger.debug(message, exc_info=exc_info)
    
    def info(self, message, exc_info=False):
        self.logger.info(message, exc_info=exc_info)
    
    def warning(self, message, exc_info=False):
        self.logger.warning(message, exc_info=exc_info)
    
    def error(self, message, exc_info=True):
        self.logger.error(message, exc_info=exc_info)
    
    def critical(self, message, exc_info=True):
        self.logger.critical(message, exc_info=exc_info)
    
    def log_execution_start(self):
        """Registra el inicio de una ejecución"""
        self.info("=" * 60)
        self.info("INICIO DE EJECUCION")
        self.info("=" * 60)
    
    def log_execution_end(self, success=True, error_msg=None):
        """Registra el fin de una ejecución"""
        self.info("=" * 60)
        if success:
            self.info("EJECUCIÓN COMPLETADA EXITOSAMENTE")
        else:
            self.error("EJECUCIÓN FINALIZADA CON ERRORES")
            if error_msg:
                self.error(f"Error: {error_msg}")
        self.info("=" * 60)
    
    def log_query_start(self, query_name, sql_file):
        """Registra el inicio de procesamiento de una consulta"""
        self.info(f"[QUERY] Iniciando procesamiento: {query_name}")
        self.debug(f"[QUERY] Archivo SQL: {sql_file}")
    
    def log_query_result(self, query_name, row_count):
        """Registra el resultado de una consulta"""
        if row_count > 0:
            self.info(f"[QUERY] {query_name}: {row_count} filas obtenidas")
        else:
            self.warning(f"[QUERY] {query_name}: Consulta sin resultados")

# Instancia global del logger
app_logger = CustomLogger("bigquery_pipeline")

# Función conveniente para obtener logger en otros módulos
def get_logger(name=None):
    """Obtiene un logger configurado"""
    if name:
        return CustomLogger(name).get_logger()
    return app_logger.get_logger()