"""
Gestor de pipelines de datos
"""

import importlib
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import traceback
from src.modules.logger import get_logger

# Obtener logger para este módulo
logger = get_logger(__name__)

class PipelineManager:
    def __init__(self, pipelines_dir="pipelines"):
        """
        Inicializa el gestor de pipelines
        
        Args:
            pipelines_dir: Directorio donde se encuentran los módulos de transformación
        """
        try:
            self.pipelines_dir = Path(pipelines_dir)
            sys.path.insert(0, str(self.pipelines_dir.parent))

            self.pipelines_dir = Path(pipelines_dir)
            
            # Verificar si el directorio existe
            if not self.pipelines_dir.exists():
                logger.warning(f"Directorio de pipelines no encontrado: {self.pipelines_dir}")
                self.pipelines_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Directorio de pipelines creado: {self.pipelines_dir}")
            
            sys.path.insert(0, str(self.pipelines_dir.parent))
            logger.info(f"Pipelines Manager inicializado. Directorio: {self.pipelines_dir}")
        except Exception as e:
            logger.error(f"Error inicializando PipelineManager: {str(e)}")
            raise
    
    def list_available_pipelines(self):
        """
        Lista todas las transformaciones disponibles
        
        Returns:
            list: Lista de nombres de módulos de transformación
        """
        try:
            pipelines = []
            if self.pipelines_dir.exists():
                for file in self.pipelines_dir.glob("*_pipeline.py"):
                    pipelines.append(file.stem)
            
            logger.debug(f"Pipelines disponibles: {pipelines}")
            return pipelines
            
        except Exception as e:
            logger.error(f"Error listando pipelines: {str(e)}")
            return []
    
    def apply_pipeline(self, query_name, df):
        """
        Aplica la transformación específica para una consulta
        
        Args:
            query_name: Nombre de la consulta (coincide con el nombre del módulo)
            df: DataFrame a transformar
            
        Returns:
            pandas.DataFrame: DataFrame transformado
        """
        logger.info(f"Aplicando transformación para consulta: {query_name}")
        logger.debug(f"DataFrame inicial - Filas: {len(df)}, Columnas: {list(df.columns)}")
        
        try:
            # Verificar si el DataFrame está vacío
            if df.empty:
                logger.warning(f"DataFrame vacío recibido para transformación {query_name}")
                return df
            
            # Importar el módulo de transformación
            module_name = f"{query_name}_transform"
            
            try:
                logger.debug(f"Intentando importar módulo: transformations.{module_name}")
                transformation_module = importlib.import_module(f"transformations.{module_name}")
                logger.info(f"Módulo de transformación cargado: {module_name}")
            except ModuleNotFoundError as e:
                logger.warning(f"No se encontró módulo de transformación para {query_name}: {str(e)}")
                logger.info(f"Devolviendo datos sin transformación para {query_name}")
                return df
            except ImportError as e:
                logger.error(f"Error importando módulo {module_name}: {str(e)}")
                return df
            
            # Buscar la función de transformación
            if hasattr(transformation_module, 'transform_data'):
                transform_func = transformation_module.transform_data
                logger.debug(f"Función de transformación encontrada para {query_name}")
                
                # Aplicar transformación
                logger.info(f"Ejecutando transformación para {query_name}...")
                start_time = datetime.now()
                
                try:
                    df_transformed = transform_func(df)
                    
                    # Verificar que se devolvió un DataFrame
                    if not isinstance(df_transformed, pd.DataFrame):
                        logger.error(f"La función transform_data de {module_name} no devolvió un DataFrame")
                        return df
                    
                    execution_time = (datetime.now() - start_time).total_seconds()
                    logger.info(f"Transformación completada en {execution_time:.2f} segundos")
                    logger.debug(f"DataFrame transformado - Filas: {len(df_transformed)}, Columnas: {list(df_transformed.columns)}")
                    
                    return df_transformed
                    
                except Exception as transform_error:
                    logger.error(f"Error ejecutando transformación para {query_name}: {str(transform_error)}")
                    logger.debug(f"Traceback de transformación: {traceback.format_exc()}")
                    return df
                    
            else:
                logger.warning(f"No se encontró función 'transform_data' en {module_name}")
                logger.info(f"Devolviendo datos sin transformación para {query_name}")
                return df
            
        except Exception as e:
            logger.error(f"Error general aplicando transformación para {query_name}: {str(e)}")
            logger.debug(f"Traceback completo: {traceback.format_exc()}")
            return df
    
    
    def apply_pipeline(self, query_name, df):
        """
        Aplica la transformación específica para una consulta
        
        Args:
            query_name: Nombre de la consulta (coincide con el nombre del módulo)
            df: DataFrame a transformar
            
        Returns:
            pandas.DataFrame: DataFrame transformado
        """
        try:
            # Importar el módulo de transformación
            module_name = f"{query_name}_transform"
            
            try:
                pipeline_module = importlib.import_module(f"pipelines.{module_name}")
            except ModuleNotFoundError:
                print(f"No se encontró módulo de transformación para {query_name}")
                return df
            
            # Buscar la función de transformación
            if hasattr(pipeline_module, 'transform_data'):
                transform_func = pipeline_module.transform_data
                df = transform_func(df)
                print(f"Transformación aplicada para {query_name}")
            else:
                print(f"No se encontró función transform_data en {module_name}")
            
            return df
            
        except Exception as e:
            print(f"Error aplicando transformación para {query_name}: {str(e)}")
            return df