"""
Pipeline de streaming que lee de Pub/Sub y escribe en BigQuery.
Utiliza config.settings para obtener parámetros.
"""
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
import sys

# Agregar raíz al path para importar config
sys.path.append(str(Path(__file__).parent.parent))
from config.settings import (
    PROJECT_ID,
    TOPIC_ID,
    BQ_DATASET,
    BRONZE_TABLE,
    DEADLETTER_TABLE,
    REGION,
    get_credentials,
)

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    StandardOptions,
    WorkerOptions,
)
from apache_beam.transforms import window
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Esquemas para BigQuery (igual que en setup_infrastructure.py)
BRONZE_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "date_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "product_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "session_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

DEADLETTER_SCHEMA = {
    "fields": [
        {"name": "raw_message", "type": "STRING", "mode": "REQUIRED"},
        {"name": "error", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

class ParseEvent(beam.DoFn):
    """Parsea el mensaje JSON y prepara el registro para BigQuery."""
    def process(self, element: bytes):
        try:
            data = json.loads(element.decode("utf-8"))
            # Extraer timestamp
            raw_ts = data.get("event_timestamp")
            if raw_ts:
                if isinstance(raw_ts, (int, float)):
                    ts = datetime.fromtimestamp(raw_ts)
                else:
                    ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
            else:
                ts = datetime.utcnow()
            date_id = int(ts.strftime("%Y%m%d"))

            # Campos
            event_id = int(data.get("event_id", 0))
            customer_id = int(data.get("customer_id", 0))
            product_id = int(data.get("product_id", 0))
            session_id = str(data.get("session_id", ""))[:255]
            event_type = str(data.get("event_type", "unknown"))[:50]

            yield {
                "event_id": event_id,
                "date_id": date_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "session_id": session_id,
                "event_type": event_type,
                "ingestion_time": datetime.utcnow().isoformat() + "Z",
            }
        except Exception as e:
            # Emitir a deadletter
            yield beam.pvalue.TaggedOutput(
                "deadletter",
                {
                    "raw_message": element.decode("utf-8", errors="ignore"),
                    "error": str(e)[:500],
                    "ingestion_time": datetime.utcnow().isoformat() + "Z",
                }
            )

def run(
    project_id: str,
    topic_id: str,
    output_table: str,
    deadletter_table: str,
    region: str,
    staging_bucket: str,
    temp_bucket: str,
    runner: str = "DataflowRunner",
    machine_type: str = "n1-standard-2",
    num_workers: int = 2,
    max_num_workers: int = 5,
):
    # Opciones del pipeline
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.region = region
    google_cloud_options.staging_location = f"{staging_bucket}/staging"
    google_cloud_options.temp_location = f"{temp_bucket}/temp"
    google_cloud_options.job_name = f"streaming-events-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    options.view_as(StandardOptions).runner = runner
    options.view_as(StandardOptions).streaming = True

    worker_opts = options.view_as(WorkerOptions)
    worker_opts.machine_type = machine_type
    worker_opts.num_workers = num_workers
    worker_opts.max_num_workers = max_num_workers
    worker_opts.autoscaling_algorithm = "THROUGHPUT_BASED"

    topic_path = f"projects/{project_id}/topics/{topic_id}"

    with beam.Pipeline(options=options) as p:
        messages = p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=topic_path)

        # Ventana de 1 minuto para optimizar escritura
        windowed = messages | "Windowing" >> beam.WindowInto(window.FixedWindows(60))

        parsed = windowed | "Parse Events" >> beam.ParDo(ParseEvent()).with_outputs("deadletter", main="success")

        success = parsed["success"]
        deadletter = parsed["deadletter"]

        # Escribir a BigQuery (con Storage Write API)
        success | "Write to BQ" >> WriteToBigQuery(
            table=output_table,
            schema=BRONZE_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method="STORAGE_WRITE_API",
            triggering_frequency=30,
        )

        deadletter | "Write Deadletter" >> WriteToBigQuery(
            table=deadletter_table,
            schema=DEADLETTER_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method="STORAGE_WRITE_API",
            triggering_frequency=30,
        )

        logger.info("Pipeline iniciado. Escuchando mensajes...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", default="DataflowRunner")
    parser.add_argument("--staging-bucket", default="novaretail-dataflow-staging")
    parser.add_argument("--temp-bucket", default="novaretail-temp")
    parser.add_argument("--machine-type", default="n1-standard-2")
    parser.add_argument("--num-workers", type=int, default=2)
    parser.add_argument("--max-num-workers", type=int, default=5)
    args = parser.parse_args()

    run(
        project_id=PROJECT_ID,
        topic_id=TOPIC_ID,
        output_table=f"{PROJECT_ID}:{BQ_DATASET}.{BRONZE_TABLE}",
        deadletter_table=f"{PROJECT_ID}:{BQ_DATASET}.{DEADLETTER_TABLE}",
        region=REGION,
        staging_bucket=args.staging_bucket,
        temp_bucket=args.temp_bucket,
        runner=args.runner,
        machine_type=args.machine_type,
        num_workers=args.num_workers,
        max_num_workers=args.max_num_workers,
    )