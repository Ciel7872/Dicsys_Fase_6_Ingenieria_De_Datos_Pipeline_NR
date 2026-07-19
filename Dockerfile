# Usamos una imagen oficial, estable y con una versión de Python compatible (3.11)
FROM apache/airflow:2.9.1-python3.11

# Copiamos el archivo de requerimientos
COPY requirements.txt /requirements.txt

# Instalamos las librerías de GCP y utilidades
RUN pip install --no-cache-dir -r /requirements.txt