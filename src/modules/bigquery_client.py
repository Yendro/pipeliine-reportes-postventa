"""
Cliente para conexión y consultas en BigQuery
"""

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import sqlparse
from pathlib import Path
from datetime import datetime
import config

class BigQueryClient:
    def __init__(self, credentials_path):
        """
        Inicializa el cliente de BigQuery
        
        Args:
            credentials_path: Ruta al archivo JSON de credenciales
        """
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id
        )
    
    def load_query_from_file(self, file_path):
        """
        Carga una consulta SQL desde un archivo
        
        Args:
            file_path: Ruta al archivo SQL
            
        Returns:
            str: Consulta SQL
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    
    def apply_dynamic_filters(self, query, filters):
        """
        Aplica filtros dinámicos a la consulta
        
        Args:
            query: Consulta SQL original
            filters: Diccionario de filtros a aplicar
            
        Returns:
            str: Consulta SQL con filtros aplicados
        """
        if not filters:
            return query
        
        # Parsear la consulta
        parsed = sqlparse.parse(query)
        if not parsed:
            return query
        
        statement = parsed[0]
        
        # Buscar la cláusula WHERE
        where_pos = -1
        tokens = statement.tokens
        for i, token in enumerate(tokens):
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'WHERE':
                where_pos = i
                break
        
        # Construir condiciones de filtro
        conditions = []
        for filter_name, should_apply in filters.items():
            if should_apply and filter_name in config.DYNAMIC_FILTERS:
                conditions.append(config.DYNAMIC_FILTERS[filter_name])
        
        if not conditions:
            return query
        
        filter_condition = " AND ".join(conditions)
        
        # Aplicar filtros
        if where_pos != -1:
            # Ya existe WHERE, agregar condiciones
            new_tokens = list(tokens[:where_pos + 1])
            new_tokens.append(f" ({filter_condition}) AND ")
            new_tokens.extend(tokens[where_pos + 1:])
            return ''.join(str(t) for t in new_tokens)
        else:
            # Buscar ORDER BY o GROUP BY para insertar WHERE antes
            for i, token in enumerate(tokens):
                if token.ttype is sqlparse.tokens.Keyword and token.value.upper() in ('ORDER BY', 'GROUP BY', 'LIMIT'):
                    new_tokens = list(tokens[:i])
                    new_tokens.append(f" WHERE {filter_condition} ")
                    new_tokens.extend(tokens[i:])
                    return ''.join(str(t) for t in new_tokens)
            
            # Si no encuentra, agregar al final
            return f"{query} WHERE {filter_condition}"
    
    def execute_query(self, query, filters=None):
        """
        Ejecuta una consulta en BigQuery
        
        Args:
            query: Consulta SQL
            filters: Filtros dinámicos a aplicar
            
        Returns:
            pandas.DataFrame: Resultados de la consulta
        """
        try:
            # Aplicar filtros dinámicos si existen
            if filters:
                query = self.apply_dynamic_filters(query, filters)
            
            # Ejecutar consulta
            query_job = self.client.query(query)
            
            # Convertir a DataFrame
            df = query_job.to_dataframe()
            
            print(f"Consulta ejecutada exitosamente. Filas obtenidas: {len(df)}")
            return df
            
        except Exception as e:
            print(f"Error ejecutando consulta: {str(e)}")
            raise