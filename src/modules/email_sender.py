"""
Módulo para enviar correos electrónicos con archivos adjuntos
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from pathlib import Path
from datetime import datetime
import config

class EmailSender:
    def __init__(self):
        """Inicializa el cliente de correo"""
        self.config = config.EMAIL_CONFIG
    
    def create_message(self, subject, body, attachments=None):
        """
        Crea un mensaje de correo
        
        Args:
            subject: Asunto del correo
            body: Cuerpo del correo
            attachments: Lista de rutas de archivos a adjuntar
            
        Returns:
            MIMEMultipart: Mensaje de correo
        """
        msg = MIMEMultipart()
        msg['From'] = self.config['email']
        msg['To'] = ', '.join(config.RECIPIENTS['to'])
        msg['Cc'] = ', '.join(config.RECIPIENTS['cc'])
        
        # Personalizar asunto con fecha actual si tiene marcadores
        if '{mes_actual}' in subject:
            subject = subject.replace('{mes_actual}', datetime.now().strftime('%B %Y'))
        if '{fecha}' in subject:
            subject = subject.replace('{fecha}', datetime.now().strftime('%Y-%m-%d'))
        
        msg['Subject'] = subject
        
        # Agregar cuerpo del mensaje
        msg.attach(MIMEText(body, 'plain'))
        
        # Adjuntar archivos
        if attachments:
            for attachment_path in attachments:
                if os.path.exists(attachment_path):
                    self._attach_file(msg, attachment_path)
                else:
                    print(f"Archivo no encontrado: {attachment_path}")
        
        return msg
    
    def _attach_file(self, msg, file_path):
        """
        Adjunta un archivo al mensaje
        
        Args:
            msg: Mensaje MIMEMultipart
            file_path: Ruta del archivo a adjuntar
        """
        filename = os.path.basename(file_path)
        
        with open(file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename={filename}",
        )
        
        msg.attach(part)
    
    def send_email(self, subject, body, attachments=None):
        """
        Envía un correo electrónico
        
        Args:
            subject: Asunto del correo
            body: Cuerpo del correo
            attachments: Lista de rutas de archivos a adjuntar
        """
        try:
            # Crear mensaje
            msg = self.create_message(subject, body, attachments)
            
            # Conectar al servidor SMTP
            server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
            
            if self.config['use_tls']:
                server.starttls()
            
            # Iniciar sesión
            server.login(self.config['email'], self.config['password'])
            
            # Enviar correo
            all_recipients = config.RECIPIENTS['to'] + config.RECIPIENTS['cc'] + config.RECIPIENTS['bcc']
            server.send_message(msg, from_addr=self.config['email'], to_addrs=all_recipients)
            
            # Cerrar conexión
            server.quit()
            
            print(f"Correo enviado exitosamente a {len(all_recipients)} destinatarios")
            
        except Exception as e:
            print(f"Error enviando correo: {str(e)}")
            raise