"""crear si no existen
Tópico Pub/Sub 
Suscripción Pub/Sub 
Tablas BigQuery """
#!/usr/bin/env python3

import sys
import os
from pathlib import Path

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

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_SCHEMA = [
    {"name": "event_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "date_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "customer_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "product_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "session_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

DEADLETTER_SCHEMA = [
    {"name": "raw_message", "type": "STRING", "mode": "REQUIRED"},
    {"name": "error", "type": "STRING", "mode": "REQUIRED"},
    {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

def create_pubsub_topic(project_id: str, topic_id: str) -> None:
    from google.cloud import pubsub_v1
    from google.api_core.exceptions import AlreadyExists, NotFound, GoogleAPIError

    publisher = pubsub_v1.PublisherClient(credentials=get_credentials())
    topic_path = publisher.topic_path(project_id, topic_id)
    try:
        publisher.get_topic(request={"topic": topic_path})
        logger.info(f"Topico '{topic_path}' ya existe.")
    except NotFound:
        publisher.create_topic(request={"name": topic_path})
        logger.info(f"Topico '{topic_path}' creado exitosamente.")
    except GoogleAPIError as exc:
        logger.error(f"No se pudo verificar/crear el topico: {exc}")

def create_pubsub_subscription(project_id: str, topic_id: str, subscription_id: str) -> None:
    from google.cloud import pubsub_v1
    from google.api_core.exceptions import NotFound, GoogleAPIError

    subscriber = pubsub_v1.SubscriberClient(credentials=get_credentials())
    topic_path = subscriber.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    try:
        subscriber.get_subscription(request={"subscription": subscription_path})
        logger.info(f"Suscripcion '{subscription_path}' ya existe.")
    except NotFound:
        subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "ack_deadline_seconds": 30,
            }
        )
        logger.info(f"Suscripcion '{subscription_path}' creada.")
    except GoogleAPIError as exc:
        logger.error(f"No se pudo verificar/crear la suscripcion: {exc}")

def create_bigquery_dataset(project_id: str, dataset_id: str, region: str) -> None:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound, GoogleAPIError

    client = bigquery.Client(project=project_id, credentials=get_credentials())
    dataset_ref = client.dataset(dataset_id)

    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset '{dataset_id}' ya existe.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = region
        client.create_dataset(dataset)
        logger.info(f"Dataset '{dataset_id}' creado en {region}.")
    except GoogleAPIError as exc:
        logger.error(f"No se pudo verificar/crear el dataset: {exc}")

def create_bigquery_table(project_id: str, dataset_id: str, table_id: str, schema: list,
                          partition_field: str = None, clustering_fields: list = None) -> None:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound, GoogleAPIError

    client = bigquery.Client(project=project_id, credentials=get_credentials())
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        client.get_table(table_ref)
        logger.info(f"Tabla '{table_id}' ya existe.")
    except NotFound:
        bq_schema = [bigquery.SchemaField(**field) for field in schema]
        table = bigquery.Table(table_ref, schema=bq_schema)
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
                expiration_ms=30 * 24 * 60 * 60 * 1000,
            )
        if clustering_fields:
            table.clustering_fields = clustering_fields
        client.create_table(table)
        logger.info(f"Tabla '{table_id}' creada.")

if __name__ == "__main__":
    logger.info("Iniciando configuracion de infraestructura...")

    create_pubsub_topic(PROJECT_ID, TOPIC_ID)
    create_pubsub_subscription(PROJECT_ID, TOPIC_ID, SUBSCRIPTION_ID)
    create_bigquery_dataset(PROJECT_ID, BQ_DATASET, REGION)
    create_bigquery_table(
        PROJECT_ID, BQ_DATASET, BRONZE_TABLE, BRONZE_SCHEMA,
        partition_field="ingestion_time",
        clustering_fields=["event_type", "customer_id"]
    )
    create_bigquery_table(
        PROJECT_ID, BQ_DATASET, DEADLETTER_TABLE, DEADLETTER_SCHEMA,
        partition_field="ingestion_time",
    )

    logger.info("Infraestructura lista.")
