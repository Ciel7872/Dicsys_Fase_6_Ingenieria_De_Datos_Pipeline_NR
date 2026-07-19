# Usamos la imagen oficial de Airflow 2.7.1 con Python 3.11
FROM apache/airflow:2.7.1-python3.11

# Copiamos el archivo de requerimientos
COPY requirements.txt /requirements.txt

# Instalamos las librerías de GCP, dbt y utilidades
RUN pip install --no-cache-dir -r /requirements.txt
