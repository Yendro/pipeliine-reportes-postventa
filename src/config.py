"""
Configuración del proyecto con variables de entorno
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de BigQuery
BIGQUERY_CREDENTIALS = os.getenv("BIGQUERY_CREDENTIALS_PATH")

def get_email_config():
    """Obtiene la configuración de email"""
    email_config = {
        'smtp_server': os.getenv("SMTP_SERVER"),
        'smtp_port': int(os.getenv("SMTP_PORT")),
        'email': os.getenv("EMAIL_ADDRESS"),
        'password': os.getenv("EMAIL_PASSWORD"),
        'domain': os.getenv("EMAIL_DOMAIN"),
        'use_tls': os.getenv("USE_TLS").lower() == "true",
        'use_ssl': os.getenv("REQUIRE_SSL").lower() == "true",
        'auth_method': os.getenv("AUTH_METHOD").lower(),
        'timeout': 30  # Tiempo de espera en segundos
    }
    
    # Validar configuración mínima
    if not email_config['email']:
        raise ValueError("EMAIL_ADDRESS no está configurado en variables de entorno")
    
    if not email_config['password']:
        raise ValueError("EMAIL_PASSWORD no está configurado en variables de entorno")
    
    # Detectar si es correo corporativo por el dominio
    if '@' in email_config['email']:
        email_domain = email_config['email'].split('@')[1]
        if email_domain not in ['outlook.com', 'hotmail.com', 'live.com', 'gmail.com']:
            email_config['is_corporate'] = True
            if not email_config['domain']:
                email_config['domain'] = email_domain
        else:
            email_config['is_corporate'] = False
    
    return email_config

EMAIL_CONFIG = get_email_config()

# Destinatarios desde variables de entorno
def get_recipients_from_env():
    """Obtiene destinatarios desde variables de entorno"""
    to_emails = os.getenv("EMAIL_TO")
    cc_emails = os.getenv("EMAIL_CC")
    
    # Convertir strings separados por comas en listas
    to_list = [email.strip() for email in to_emails.split(",") if email.strip()]
    cc_list = [email.strip() for email in cc_emails.split(",") if email.strip()]
    
    # Si no hay destinatarios en variables de entorno, usar valores por defecto
    if not to_list:
        to_list = ["rmoreno@lopezrosa.com"]
    if not cc_list:
        cc_list = ["lrayas@lopezrosa.com"]
    
    return {
        'to': to_list,
        'cc': cc_list,
        'bcc': ["yhernandez0610@gmail.com"]  # Opcional
    }

RECIPIENTS = get_recipients_from_env()

# Configuración de Logging
LOG_CONFIG = {
    'level': os.getenv("LOG_LEVEL"),
    'file': os.getenv("LOG_FILE"),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'date_format': '%d-%m-%Y %H:%M' #'%Y-%m-%d %H:%M:%S'
}

# Verificar variables críticas
def validate_config():
    """Valida que las variables críticas estén configuradas"""
    errors = []
    
    if not os.path.exists(BIGQUERY_CREDENTIALS):
        errors.append(f"Archivo de credenciales no encontrado: {BIGQUERY_CREDENTIALS}")
    
    if not EMAIL_CONFIG['email']:
        errors.append("EMAIL_ADDRESS no está configurado en variables de entorno")
    
    if not EMAIL_CONFIG['password']:
        errors.append("EMAIL_PASSWORD no está configurado en variables de entorno")
    
    if errors:
        raise EnvironmentError("\n".join(errors))
    
    return True

# Configuración de consultas (mantener igual)
QUERIES_CONFIG = {
    'ingresos_gaia': {
        'sql_file': 'data/ingresos-gaia.sql',
        'transformation': 'ingresos_gaia_transform',
        'output_file': 'reports/Ingresos-GAIA.xlsx',
        'subject': 'Reporte Mensual - {mes_actual}/{año_acutal}',
        'filters': {
            'mes_actual': True,
            'año_actual': True
        }
    },
    'ingresos_condominios_bi': {
        'sql_file': 'data/ingresos-condominios-bi.sql',
        'transformation': 'ingresoos_condominios_bi_transform',
        'output_file': 'reports/Ingresos-Condominios-BI.xlsx',
        'subject': 'Ingresos Condominios - {mes_actual}/{año_acutal}',
        'filters': {
            'mes_actual': True,
            'año_actual': True
        }
    },
    'ingresos_condominios_bi': {
        'sql_file': 'data/ingresos-condominios-bi.sql',
        'transformation': 'ingresoos_condominios_bi_transform',
        'output_file': 'reports/Ingresos-Condominios-BI.xlsx',
        'subject': 'Ingresos Condominios - {mes_actual}/{año_acutal}',
        'filters': {
            'mes_actual': True,
            'año_actual': True
        }
    },
    'ingresos_condominios_bi': {
        'sql_file': 'data/ingresos-condominios-bi.sql',
        'transformation': 'ingresoos_condominios_bi_transform',
        'output_file': 'reports/Ingresos-Condominios-BI.xlsx',
        'subject': 'Ingresos Condominios - {mes_actual}/{año_acutal}',
        'filters': {
            'mes_actual': True,
            'año_actual': True
        }
    },
    'ingresos_condominios_bi': {
        'sql_file': 'data/ingresos-condominios-bi.sql',
        'transformation': 'ingresoos_condominios_bi_transform',
        'output_file': 'reports/Ingresos-Condominios-BI.xlsx',
        'subject': 'Ingresos Condominios - {mes_actual}/{año_acutal}',
        'filters': {
            'mes_actual': True,
            'año_actual': True
        }
    }
}

# Configuración de filtros dinámicos (mantener igual)
DYNAMIC_FILTERS = {
    'mes_actual': "EXTRACT(MONTH FROM fecha) = EXTRACT(MONTH FROM CURRENT_DATE())",
    'año_actual': "EXTRACT(YEAR FROM fecha) = EXTRACT(YEAR FROM CURRENT_DATE())",
    'trimestre_actual': """
        CASE 
            WHEN EXTRACT(MONTH FROM CURRENT_DATE()) BETWEEN 1 AND 3 THEN 1
            WHEN EXTRACT(MONTH FROM CURRENT_DATE()) BETWEEN 4 AND 6 THEN 2
            WHEN EXTRACT(MONTH FROM CURRENT_DATE()) BETWEEN 7 AND 9 THEN 3
            ELSE 4
        END = 
        CASE 
            WHEN EXTRACT(MONTH FROM fecha) BETWEEN 1 AND 3 THEN 1
            WHEN EXTRACT(MONTH FROM fecha) BETWEEN 4 AND 6 THEN 2
            WHEN EXTRACT(MONTH FROM fecha) BETWEEN 7 AND 9 THEN 3
            ELSE 4
        END
    """
}