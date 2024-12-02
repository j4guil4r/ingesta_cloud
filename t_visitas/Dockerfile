# Usar una imagen base de Python
FROM python:3.9

# Instalar las dependencias necesarias
RUN pip install pandas boto3

# Copiar el script de ingesta al contenedor
COPY app.py /app/app.py

# Configurar el punto de entrada
CMD ["python", "/app/app.py"]
