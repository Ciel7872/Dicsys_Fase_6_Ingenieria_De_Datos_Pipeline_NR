"""
Pipeline de streaming que lee de Pub/Sub y escribe en BigQuery.
Soporta DirectRunner (local) y DataflowRunner (GCP).

Uso local (DirectRunner):
    python dataflow/streaming_pipeline.py --runner DirectRunner --input-file data/eventos_ing.json

Uso en GCP (DataflowRunner):
    python dataflow/streaming_pipeline.py --runner DataflowRunner
"""
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))
from config.settings import (
    PROJECT_ID,
    TOPIC_ID,
    BQ_BRONZE_DATASET,
    BRONZE_TABLE,
    DEADLETTER_TABLE,
    REGION,
    STAGING_BUCKET,
    TEMP_BUCKET,
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
    def process(self, element):
        try:
            data = json.loads(element if isinstance(element, str) else element.decode("utf-8"))
            raw_ts = data.get("event_timestamp")
            if raw_ts:
                if isinstance(raw_ts, (int, float)):
                    ts = datetime.fromtimestamp(raw_ts)
                else:
                    ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
            else:
                ts = datetime.utcnow()
            date_id = int(ts.strftime("%Y%m%d"))
            yield {
                "event_id": int(data.get("event_id", 0)),
                "date_id": date_id,
                "customer_id": int(data.get("customer_id", 0)),
                "product_id": int(data.get("product_id", 0)),
                "session_id": str(data.get("session_id", ""))[:255],
                "event_type": str(data.get("event_type", "unknown"))[:50],
                "ingestion_time": datetime.utcnow().isoformat() + "Z",
            }
        except Exception as e:
            yield beam.pvalue.TaggedOutput(
                "deadletter",
                {
                    "raw_message": str(element)[:500],
                    "error": str(e)[:500],
                    "ingestion_time": datetime.utcnow().isoformat() + "Z",
                },
            )


def run(
    project_id: str = PROJECT_ID,
    topic_id: str = TOPIC_ID,
    output_table: str = None,
    deadletter_table: str = None,
    region: str = REGION,
    staging_bucket: str = None,
    temp_bucket: str = None,
    runner: str = "DataflowRunner",
    input_file: str = None,
    machine_type: str = "n1-standard-2",
    num_workers: int = 2,
    max_num_workers: int = 5,
):
    output_table = output_table or f"{project_id}:{BQ_BRONZE_DATASET}.{BRONZE_TABLE}"
    deadletter_table = deadletter_table or f"{project_id}:{BQ_BRONZE_DATASET}.{DEADLETTER_TABLE}"
    staging_bucket = staging_bucket or STAGING_BUCKET
    temp_bucket = temp_bucket or TEMP_BUCKET

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.region = region
    google_cloud_options.staging_location = f"gs://{staging_bucket}/staging"
    google_cloud_options.temp_location = f"gs://{temp_bucket}/temp"
    google_cloud_options.job_name = f"streaming-events-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    options.view_as(StandardOptions).runner = runner
    options.view_as(StandardOptions).streaming = runner == "DataflowRunner"

    if runner == "DataflowRunner":
        worker_opts = options.view_as(WorkerOptions)
        worker_opts.machine_type = machine_type
        worker_opts.num_workers = num_workers
        worker_opts.max_num_workers = max_num_workers
        worker_opts.autoscaling_algorithm = "THROUGHPUT_BASED"

    with beam.Pipeline(options=options) as p:
        if runner == "DirectRunner" and input_file:
            messages = p | "Read JSON" >> beam.io.ReadFromText(input_file)
        else:
            topic_path = f"projects/{project_id}/topics/{topic_id}"
            messages = p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=topic_path)

        parsed = messages | "Parse Events" >> beam.ParDo(ParseEvent()).with_outputs("deadletter", main="success")
        success = parsed["success"]
        deadletter = parsed["deadletter"]

        success | "Write to BQ" >> WriteToBigQuery(
            table=output_table,
            schema=BRONZE_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method="STORAGE_WRITE_API" if runner == "DataflowRunner" else "DEFAULT",
            triggering_frequency=30 if runner == "DataflowRunner" else None,
        )

        deadletter | "Write Deadletter" >> WriteToBigQuery(
            table=deadletter_table,
            schema=DEADLETTER_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method="STORAGE_WRITE_API" if runner == "DataflowRunner" else "DEFAULT",
            triggering_frequency=30 if runner == "DataflowRunner" else None,
        )

        logger.info(f"Pipeline iniciado con runner: {runner}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=PROJECT_ID)
    parser.add_argument("--topic", default=TOPIC_ID)
    parser.add_argument("--output-table", default=None)
    parser.add_argument("--deadletter-table", default=None)
    parser.add_argument("--region", default=REGION)
    parser.add_argument("--runner", default="DataflowRunner", choices=["DataflowRunner", "DirectRunner"])
    parser.add_argument("--input-file", default=None, help="Archivo JSON para DirectRunner")
    parser.add_argument("--staging-bucket", default=None)
    parser.add_argument("--temp-bucket", default=None)
    parser.add_argument("--machine-type", default="n1-standard-2")
    parser.add_argument("--num-workers", type=int, default=2)
    parser.add_argument("--max-num-workers", type=int, default=5)
    args = parser.parse_args()

    run(
        project_id=args.project,
        topic_id=args.topic,
        output_table=args.output_table,
        deadletter_table=args.deadletter_table,
        region=args.region,
        staging_bucket=args.staging_bucket,
        temp_bucket=args.temp_bucket,
        runner=args.runner,
        input_file=args.input_file,
        machine_type=args.machine_type,
        num_workers=args.num_workers,
        max_num_workers=args.max_num_workers,
    )
