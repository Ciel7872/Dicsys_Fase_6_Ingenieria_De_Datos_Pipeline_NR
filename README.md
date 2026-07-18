# Fase 6 – Data Engineering: Pipeline Automatizado con Dataflow y Airflow

## 📌 Objetivo
Automatizar la ingesta, procesamiento y transformación de eventos en tiempo real usando servicios nativos de Google Cloud Platform (Pub/Sub, Dataflow, BigQuery) y orquestación con Cloud Composer (Airflow).

## ✅ Estado actual del proyecto
El repositorio ya incluye:
- Pipeline de Apache Beam para leer mensajes de Pub/Sub y escribir a BigQuery.
- Script de publicación a Pub/Sub desde archivos JSON.
- Script de creación de infraestructura en GCP.
- Configuración de entorno y credenciales centralizadas.
- Plantilla Docker y metadata para Flex Template.

Faltaba completar la parte de orquestación y los artefactos SQL para cerrar el flujo end-to-end.

## 🏗️ Arquitectura Propuesta
[Simulador JSON] → [Pub/Sub] → [Dataflow Streaming] → [BigQuery (bronze)]
↓
[Cloud Composer (Airflow)] → [Transformación SQL] → [BigQuery (curated)]

text

## 📁 Estructura de Carpetas Esperada
novaretail_pipeline/
├── dags/
│ ├── dag_eventos_realtime.py # DAG principal de orquestación
│ └── dependencies/
│ └── task_factory.py # Funciones auxiliares para tareas
├── dataflow/
│ ├── streaming_pipeline.py # Pipeline de Apache Beam
│ ├── metadata.json # Metadatos para Flex Template
│ ├── Dockerfile # Imagen personalizada para Dataflow
│ └── cloudbuild.yaml # (Opcional) Build con Cloud Build
├── ingestion/
│ ├── pubsub_publisher.py # Script para publicar eventos JSON
│ ├── setup_infrastructure.py # Creación de tópico, suscripción y tablas
│ └── consumer_test.py # Consumidor de prueba (para validación)
├── sql/
│ ├── transforms/
│ │ └── transform_bronze_to_curated.sql # SQL para limpiar y cargar FACT_EVENTS
│ └── schemas/
│ └── create_bronze_table.sql # DDL para tabla bronze_events
├── config/
│ ├── settings.py # Configuración centralizada (variables de entorno)
│ └── gcp_credentials.json # (Ignorado en Git) Credenciales de servicio
├── data/
│ ├── sample/
│ │ └── eventos.json # Muestra de 10 eventos
│ └── full/
│ └── eventos_full.json # Archivo completo para simulación
├── requirements.txt # Dependencias para el entorno local
└── README.md

text

## 📦 Archivos Clave (contenido esperado)

### 1. `config/settings.py`
Carga variables de entorno y define constantes globales (proyecto, región, nombres de tópicos, datasets, etc.). Usa `python-dotenv` para cargar desde `.env`.

### 2. `dataflow/streaming_pipeline.py`
Pipeline de Apache Beam que:
- Lee mensajes de Pub/Sub.
- Parsea JSON, valida y tipifica campos.
- Escribe en `bronze_events` (éxitos) y `deadletter_events` (fallos).
- Usa ventanas de 1 minuto y escritura con Storage Write API.

### 3. `dataflow/metadata.json`
Define los parámetros esperados por el Flex Template (project, topic, output-table, deadletter-table).

### 4. `dataflow/Dockerfile`
Construye una imagen personalizada para el Flex Template basada en `python3-template-launcher-base`, copiando `streaming_pipeline.py` y dependencias necesarias.

### 5. `dags/dag_eventos_realtime.py`
DAG de Airflow que:
- Lanza el job de Dataflow (usando el Flex Template).
- Espera a que finalice (opcional).
- Ejecuta una consulta SQL de transformación para mover datos de `bronze_events` a `FACT_EVENTS`.
- Se ejecuta cada 10 minutos (schedule_interval).

### 6. `sql/transforms/transform_bronze_to_curated.sql`
Script SQL que:
- Elimina registros del período a procesar en `FACT_EVENTS` (idempotencia).
- Inserta datos limpios desde `bronze_events`, aplicando LEFT JOIN con dimensiones y manejo de nulos.

### 7. `ingestion/pubsub_publisher.py`
Script que lee un archivo JSON y publica los eventos en Pub/Sub con reintentos y manejo de errores.

### 8. `ingestion/setup_infrastructure.py`
Crea/verifica:
- Tópico Pub/Sub
- Suscripción Pub/Sub
- Dataset BigQuery (bronze, silver, gold)
- Tablas: `bronze_events`, `deadletter_events`

## 🐍 Dependencias (requirements.txt)
google-cloud-pubsub==2.19.0
google-cloud-bigquery==3.14.1
google-cloud-storage==2.10.0
google-auth==2.23.4
google-auth-oauthlib==1.0.0
python-dotenv==1.0.0
pathlib2==2.3.7
protobuf==3.20.3

Nota: Apache Beam debe instalarse por separado en un entorno Python 3.10/3.11 para ejecutar Dataflow localmente o construir el template.


text

## 🛠️ Pasos para Desplegar y Ejecutar

### 1. Configuración Inicial
```bash
# Clonar/clonar repositorio
cd novaretail_pipeline

# Crear y activar entorno virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate      # Windows

# Instalar dependencias
pip install -r requirements.txt
2. Configurar Credenciales
Coloca tu archivo gcp-credentials.json en config/ y exporta la variable:

bash
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/config/gcp_credentials.json"
3. Crear Infraestructura
bash
python ingestion/setup_infrastructure.py
4. Probar Publicador
bash
python ingestion/pubsub_publisher.py --input data/sample/eventos.json --limit 10
5. Construir Flex Template de Dataflow
bash
gcloud dataflow flex-template build gs://novaretail-dataflow-templates/pubsub-to-bq-streaming.json \
    --image-gcr-path="gcr.io/dataleaguenovaretail/dataflow/pubsub-to-bq:latest" \
    --sdk-language="PYTHON" \
    --flex-template-base-image="PYTHON3" \
    --metadata-file="dataflow/metadata.json" \
    --py-path="." \
    --env="FLEX_TEMPLATE_PYTHON_PY_MODULE=dataflow.streaming_pipeline" \
    --project=dataleaguenovaretail \
    --region=us-south1
6. Lanzar Job de Dataflow (opcional manual)
bash
gcloud dataflow flex-template run "streaming-events-$(date +%Y%m%d-%H%M%S)" \
    --template-file-gcs-location="gs://novaretail-dataflow-templates/pubsub-to-bq-streaming.json" \
    --parameters="project=dataleaguenovaretail,topic=eventos-realtime,output-table=dataleaguenovaretail:nR_core_datasets.bronze_events,deadletter-table=dataleaguenovaretail:nR_core_datasets.deadletter_events" \
    --region=us-south1
7. Desplegar DAG en Cloud Composer
Sube los archivos de la carpeta dags/ al bucket de DAGs de tu entorno de Composer.

Activa el DAG desde la interfaz de Airflow.

Verifica los logs y el monitoreo.

📊 Verificación en BigQuery
sql
SELECT COUNT(*) FROM `nR_core_datasets.bronze_events`;
SELECT COUNT(*) FROM `nR_core_datasets.FACT_EVENTS`;
🔐 Seguridad y Permisos Necesarios
Cuenta de servicio para Dataflow: roles/dataflow.admin, roles/storage.objectAdmin, roles/bigquery.dataEditor, roles/pubsub.subscriber.

Cuenta de usuario para despliegue: roles/dataflow.admin, roles/storage.objectAdmin, roles/iam.serviceAccountUser.

Cloud Composer (Airflow): la cuenta de servicio del entorno debe tener permisos para ejecutar jobs de Dataflow y consultas en BigQuery.

📝 Notas Finales
El pipeline de Dataflow está configurado en modo streaming continuo.

El DAG de Airflow ejecuta la transformación batch cada 10 minutos para mantener actualizada la tabla de hechos.

Se implementa idempotencia en las cargas para evitar duplicados.

📎 Enlaces Útiles
Documentación de Flex Templates

Apache Beam en Python

Cloud Composer