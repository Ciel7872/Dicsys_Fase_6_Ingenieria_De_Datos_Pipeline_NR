#!/usr/bin/env python3
"""
Consumidor de prueba: lee mensajes de Pub/Sub y los escribe en BigQuery.
Útil para validar el flujo end-to-end antes de implementar Dataflow.
"""
import json
import sys
from pathlib import Path
from datetime import datetime

# Agregar raíz al path para importar config
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import (
    PROJECT_ID,
    SUBSCRIPTION_ID,
    BQ_DATASET,
    BRONZE_TABLE,
    get_credentials,
)

from google.cloud import pubsub_v1, bigquery
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consume_and_write(max_messages: int = 10):
    """Consume hasta 'max_messages' mensajes de la suscripción y los escribe en BigQuery."""
    credentials = get_credentials()
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    table_ref = bq_client.dataset(BQ_DATASET).table(BRONZE_TABLE)

    # Solicitar mensajes (pull)
    response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": max_messages,
        }
    )

    if not response.received_messages:
        logger.info("No hay mensajes pendientes en la suscripción.")
        return

    rows_to_insert = []
    ack_ids = []

    for received_msg in response.received_messages:
        ack_ids.append(received_msg.ack_id)
        try:
            data = json.loads(received_msg.message.data.decode("utf-8"))
            # Convertir timestamp a date_id (como en Dataflow)
            ts_str = data.get("event_timestamp")
            if ts_str:
                # Asumimos formato ISO
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                date_id = int(ts.strftime("%Y%m%d"))
            else:
                date_id = int(datetime.utcnow().strftime("%Y%m%d"))

            row = {
                "event_id": data.get("event_id", 0),
                "date_id": date_id,
                "customer_id": data.get("customer_id", 0),
                "product_id": data.get("product_id", 0),
                "session_id": data.get("session_id", ""),
                "event_type": data.get("event_type", "unknown"),
                "ingestion_time": datetime.utcnow().isoformat() + "Z",
            }
            rows_to_insert.append(row)
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")

    if rows_to_insert:
        errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            logger.error(f"Errores al insertar en BigQuery: {errors}")
        else:
            logger.info(f"✅ Insertados {len(rows_to_insert)} registros en {BQ_DATASET}.{BRONZE_TABLE}")

    # Confirmar recepción (ack) para eliminar mensajes de la suscripción
    if ack_ids:
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
        logger.info(f"✅ Confirmados {len(ack_ids)} mensajes.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=10, help="Máximo de mensajes a consumir")
    args = parser.parse_args()
    consume_and_write(args.max)