"""crear si no existen
Tópico Pub/Sub 
Suscripción Pub/Sub 
Tablas BigQuery """
#!/usr/bin/env python3

import sys
import os
from pathlib import Path

# Agregar la raíz del proyecto al path para importar config
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import (
    PROJECT_ID,
    REGION,
    TOPIC_ID,
    SUBSCRIPTION_ID,
    BQ_DATASET,
    BRONZE_TABLE,
    DEADLETTER_TABLE,
    get_credentials,
)

from google.cloud import pubsub_v1, bigquery
from google.api_core.exceptions import AlreadyExists, NotFound
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# FUNCIONES DE CREACIÓN
# ============================================================

def create_pubsub_topic(project_id: str, topic_id: str) -> None:
    """Crea el tópico Pub/Sub si no existe."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    try:
        publisher.get_topic(request={"topic": topic_path})
        logger.info(f"✅ Tópico '{topic_path}' ya existe.")
    except NotFound:
        publisher.create_topic(request={"name": topic_path})
        logger.info(f"✅ Tópico '{topic_path}' creado exitosamente.")

def create_pubsub_subscription(project_id: str, topic_id: str, subscription_id: str) -> None:
    """Crea la suscripción al tópico si no existe."""
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    try:
        subscriber.get_subscription(request={"subscription": subscription_path})
        logger.info(f"✅ Suscripción '{subscription_path}' ya existe.")
    except NotFound:
        subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "ack_deadline_seconds": 30,  # tiempo para confirmar recepción
                # (opcional) retención de mensajes no confirmados
                # "message_retention_duration": {"seconds": 600},
            }
        )
        logger.info(f"✅ Suscripción '{subscription_path}' creada.")

def create_bigquery_dataset(project_id: str, dataset_id: str, region: str) -> None:
    """Crea el dataset de BigQuery si no existe."""
    client = bigquery.Client(project=project_id, credentials=get_credentials())
    dataset_ref = client.dataset(dataset_id)

    try:
        client.get_dataset(dataset_ref)
        logger.info(f"✅ Dataset '{dataset_id}' ya existe.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = region
        client.create_dataset(dataset)
        logger.info(f"✅ Dataset '{dataset_id}' creado en {region}.")

def create_bigquery_table(project_id: str, dataset_id: str, table_id: str, schema: list,
                          partition_field: str = None, clustering_fields: list = None) -> None:
    """Crea una tabla en BigQuery con esquema, particionamiento y clustering opcionales."""
    client = bigquery.Client(project=project_id, credentials=get_credentials())
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        client.get_table(table_ref)
        logger.info(f"✅ Tabla '{table_id}' ya existe.")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
                expiration_ms=30 * 24 * 60 * 60 * 1000,  # 30 días
            )
        if clustering_fields:
            table.clustering_fields = clustering_fields
        client.create_table(table)
        logger.info(f"✅ Tabla '{table_id}' creada.")

# ============================================================
# ESQUEMAS DE BIGQUERY
# ============================================================
BRONZE_SCHEMA = [
    bigquery.SchemaField("event_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("date_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("customer_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("product_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("session_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("event_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="REQUIRED"),
]

DEADLETTER_SCHEMA = [
    bigquery.SchemaField("raw_message", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("error", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="REQUIRED"),
]

# ============================================================
# EJECUCIÓN PRINCIPAL
# ============================================================
if __name__ == "__main__":
    logger.info("🚀 Iniciando configuración de infraestructura...")

    # 1. Pub/Sub
    create_pubsub_topic(PROJECT_ID, TOPIC_ID)
    create_pubsub_subscription(PROJECT_ID, TOPIC_ID, SUBSCRIPTION_ID)

    # 2. BigQuery
    create_bigquery_dataset(PROJECT_ID, BQ_DATASET, REGION)
    #create_bigquery_table(PROJECT_ID, BQ_DATASET, BRONZE_TABLE, BRONZE_SCHEMA)
    #create_bigquery_table(PROJECT_ID, BQ_DATASET, DEADLETTER_TABLE, DEADLETTER_SCHEMA)
    # Para bronze_events: con clustering y particionamiento
    create_bigquery_table(
        PROJECT_ID, BQ_DATASET, BRONZE_TABLE, BRONZE_SCHEMA,
        partition_field="ingestion_time",
        clustering_fields=["event_type", "customer_id"]
    )

    # Para deadletter_events: SIN clustering (solo particionamiento opcional)
    create_bigquery_table(
        PROJECT_ID, BQ_DATASET, DEADLETTER_TABLE, DEADLETTER_SCHEMA,
        partition_field="ingestion_time",
        clustering_fields=None   # o simplemente omite este parámetro
    )

    logger.info("✅ Infraestructura lista. Tópico, suscripción y tablas creados/verificados.")