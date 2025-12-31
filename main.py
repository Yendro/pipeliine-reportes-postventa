"""
Script principal para ejecutar la pipeline completa con logging
"""

import sys
import traceback
from pathlib import Path
import pandas as pd
from datetime import datetime
import schedule
import time
import config
from src.modules.bigquery_client import BigQueryClient
from src.modules.pipeline_manager import PipelineManager
from src.modules.email_sender import EmailSender
from src.modules.logger import get_logger

# Obtener logger
logger = get_logger()

def ensure_directories():
    """Asegura que existan los directorios necesarios"""
    directories = ['reports', 'data', 'src/pipelines']
    
    for dir_name in directories:
        try:
            directory = Path(dir_name)
            directory.mkdir(exist_ok=True)
            logger.debug(f"Directorio verificado: {directory}")
        except Exception as e:
            logger.error(f"Error creando directorio {dir_name}: {str(e)}")
            raise

def process_query(query_name, query_config, bq_client, pipeline):
    """
    Procesa una consulta individual con logging
    
    Args:
        query_name: Nombre de la consulta
        query_config: Configuración de la consulta
        bq_client: Cliente de BigQuery
        pipeline: Gestor de transformaciones
        
    Returns:
        str: Ruta del archivo generado o None si hubo error
    """
    try:
        logger.log_query_start(query_name, query_config['sql_file'])
        
        # Cargar consulta SQL
        sql_file = query_config['sql_file']
        sql_query = bq_client.load_query_from_file(sql_file)
        logger.debug(f"Consulta SQL cargada ({len(sql_query)} caracteres)")
        
        # Ejecutar consulta con filtros
        filters = query_config.get('filters', {})
        logger.debug(f"Filtros a aplicar: {filters}")
        
        df = bq_client.execute_query(sql_query, filters)
        logger.log_query_result(query_name, len(df))
        
        if df.empty:
            logger.warning(f"Consulta '{query_name}' devolvió 0 filas")
            return None
        
        # Aplicar transformaciones
        df = pipeline.apply_pipeline(query_name, df)
        logger.debug(f"Transformación aplicada a {query_name}")
        
        # Guardar en Excel
        output_file = query_config['output_file']
        output_path = Path(output_file)
        
        try:
            output_path.parent.mkdir(exist_ok=True)
            df.to_excel(output_file, index=False, engine='openpyxl')
            logger.info(f"Archivo guardado exitosamente: {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Error guardando archivo {output_file}: {str(e)}")
            return None
        
    except FileNotFoundError as e:
        logger.error(f"Archivo no encontrado para {query_name}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error procesando consulta {query_name}: {str(e)}")
        logger.debug(f"Traceback completo: {traceback.format_exc()}")
        return None

def run_pipeline():
    """Ejecuta la pipeline completa con logging detallado"""
    logger.log_execution_start()
    
    try:
        # Validar configuración
        config.validate_config()
        logger.info("Configuración validada exitosamente")
        
        # Asegurar directorios
        ensure_directories()
        
        # Inicializar clientes
        logger.info("Inicializando clientes...")
        bq_client = BigQueryClient(config.BIGQUERY_CREDENTIALS)
        pipeline = PipelineManager()
        email_sender = EmailSender()
        logger.info("Clientes inicializados exitosamente")
        
        # Procesar cada consulta
        generated_files = []
        successful_queries = []
        failed_queries = []
        
        logger.info(f"Procesando {len(config.QUERIES_CONFIG)} consultas")
        
        for query_name, query_config in config.QUERIES_CONFIG.items():
            try:
                output_file = process_query(query_name, query_config, bq_client, pipeline)
                
                if output_file:
                    generated_files.append(output_file)
                    successful_queries.append(query_name)
                    logger.info(f"✓ {query_name}: ÉXITO")
                else:
                    failed_queries.append(query_name)
                    logger.warning(f"✗ {query_name}: FALLÓ")
                    
            except Exception as e:
                failed_queries.append(query_name)
                logger.error(f"✗ {query_name}: ERROR - {str(e)}")
        
        # Resumen de procesamiento
        logger.info(f"Resumen: {len(successful_queries)} exitosas, {len(failed_queries)} fallidas")
        
        # Enviar correo si hay archivos generados
        if generated_files:
            try:
                # Crear cuerpo del mensaje
                body = f"""
                Hola,
                
                Se han generado los siguientes reportes:
                
                {chr(10).join([f"• {Path(f).name}" for f in generated_files])}
                
                Resumen:
                - Consultas exitosas: {len(successful_queries)}
                - Consultas fallidas: {len(failed_queries)}
                - Total de archivos: {len(generated_files)}
                
                Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                
                Saludos,
                Sistema Automatizado de Reportes
                """
                
                # Usar el asunto de la primera consulta exitosa o uno general
                if successful_queries:
                    subject = config.QUERIES_CONFIG[successful_queries[0]].get('subject', 'Reportes Automatizados')
                else:
                    subject = 'Reportes Automatizados'
                
                # Personalizar asunto
                if '{mes_actual}' in subject:
                    subject = subject.replace('{mes_actual}', datetime.now().strftime('%B %Y'))
                if '{fecha}' in subject:
                    subject = subject.replace('{fecha}', datetime.now().strftime('%Y-%m-%d'))
                
                # Enviar correo
                logger.info(f"Enviando correo con {len(generated_files)} adjuntos")
                email_sender.send_email(
                    subject=subject,
                    body=body,
                    attachments=generated_files
                )
                
                logger.info(f"Correo enviado exitosamente a {len(config.RECIPIENTS['to'])} destinatarios")
                
            except Exception as e:
                logger.error(f"Error enviando correo: {str(e)}")
                logger.log_execution_end(success=False, error_msg=str(e))
                return False
        else:
            logger.warning("No se generaron archivos. No se enviará correo.")
        
        logger.log_execution_end(success=(len(failed_queries) == 0))
        return len(failed_queries) == 0
        
    except EnvironmentError as e:
        logger.critical(f"Error de configuración: {str(e)}")
        logger.log_execution_end(success=False, error_msg=str(e))
        return False
    except Exception as e:
        logger.critical(f"Error inesperado en la pipeline: {str(e)}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        logger.log_execution_end(success=False, error_msg=str(e))
        return False

def schedule_pipeline():
    """
    Programa la ejecución automática de la pipeline
    """
    try:
        # Ejecutar todos los días a las 8:00 AM
        schedule.every().day.at("08:00").do(run_pipeline)
        
        # También ejecutar inmediatamente la primera vez
        logger.info("Iniciando ejecución programada...")
        logger.info("Ejecución inicial en progreso...")
        initial_success = run_pipeline()
        
        if initial_success:
            logger.info("Programación iniciada. Próxima ejecución: 08:00 AM")
        else:
            logger.error("Ejecución inicial falló. Revisar logs para detalles.")
        
        # Mantener el programa en ejecución
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Verificar cada minuto
            except KeyboardInterrupt:
                logger.info("Ejecución interrumpida por el usuario")
                break
            except Exception as e:
                logger.error(f"Error en el scheduler: {str(e)}")
                time.sleep(300)  # Esperar 5 minutos antes de reintentar
                
    except Exception as e:
        logger.critical(f"Error crítico en el scheduler: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Verificar argumentos de línea de comandos
        if len(sys.argv) > 1 and sys.argv[1] == "--schedule":
            # Modo programado
            logger.info("Modo: Programado")
            schedule_pipeline()
        elif len(sys.argv) > 1 and sys.argv[1] == "--test":
            # Modo prueba
            logger.info("Modo: Prueba")
            logger.info("Ejecutando validación de configuración...")
            config.validate_config()
            logger.info("Configuración validada exitosamente")
        else:
            # Ejecución única
            logger.info("Modo: Ejecución única")
            success = run_pipeline()
            sys.exit(0 if success else 1)
            
    except KeyboardInterrupt:
        logger.info("Ejecución interrumpida por el usuario")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Error en ejecución principal: {str(e)}")
        sys.exit(1)