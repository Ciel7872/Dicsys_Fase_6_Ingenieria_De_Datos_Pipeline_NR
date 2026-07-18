#!/usr/bin/env python3
# scripts/pubsub_publisher.py
"""
 Importa la configuración centralizada y no tiene credenciales hardcodeadas.
 Publica eventos en un tópico de Pub/Sub desde un archivo JSON."""
import json
import logging
import time
import sys
from pathlib import Path

# Agregar raíz al path para importar config
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import (
    PROJECT_ID,
    TOPIC_ID,
    BATCH_SIZE,
    SLEEP_BETWEEN_BATCHES,
    get_credentials,
)

from google.cloud import pubsub_v1
from google.api_core import retry, exceptions
from google.api_core.exceptions import GoogleAPICallError

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def publish_messages(project_id: str, topic_id: str, events_file: str, limit: int = None):
    """Publica eventos desde un archivo JSON."""

    # Cargar credenciales desde settings
    credentials = get_credentials()
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_path = publisher.topic_path(project_id, topic_id)

    # Leer archivo JSON
    with open(events_file, "r") as f:
        data = json.load(f)

    if isinstance(data, dict):
        data = [data]

    if limit:
        data = data[:limit]

    logger.info(f"📦 Publicando {len(data)} eventos en {topic_path}")

    for i, event in enumerate(data):
        try:
            # Codificar el mensaje
            message = json.dumps(event).encode("utf-8")

            # Publicar con reintento automático
            future = publisher.publish(topic_path, message)
            future.result(timeout=10)  # Espera confirmación

            logger.debug(f"✅ Evento {i+1}/{len(data)} publicado: {event.get('event_id', 'sin_id')}")

            # Pausa para simular tiempo real
            time.sleep(SLEEP_BETWEEN_BATCHES)

        except GoogleAPICallError as e:
            logger.error(f"❌ Error publicando evento {i}: {e}")

    logger.info("✅ Publicación completada.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/full/eventos_full.json", help="Ruta al archivo JSON")
    parser.add_argument("--limit", type=int, default=None, help="Límite de eventos a publicar")
    args = parser.parse_args()

    publish_messages(PROJECT_ID, TOPIC_ID, args.input, args.limit)