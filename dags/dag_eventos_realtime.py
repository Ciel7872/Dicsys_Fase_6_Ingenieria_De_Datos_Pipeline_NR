"""DAG de Airflow para orquestar el pipeline de eventos en tiempo real.

Flujo:
  setup_infra >> launch_dataflow >> dbt_run >> dbt_test >> validate_data
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dependencies.task_factory import (
    get_airflow_env_var,
    resolve_project_root,
)

PROJECT_ID = get_airflow_env_var("GCP_PROJECT_ID", "dataleaguenovaretail")
REGION = get_airflow_env_var("GCP_REGION", "us-south1")
TOPIC_ID = get_airflow_env_var("PUBSUB_TOPIC", "eventos-realtime")
BQ_BRONZE_DATASET = get_airflow_env_var("BQ_BRONZE_DATASET", "nR_bronze")
BQ_SILVER_DATASET = get_airflow_env_var("BQ_SILVER_DATASET", "nR_silver")
BQ_GOLD_DATASET = get_airflow_env_var("BQ_GOLD_DATASET", "nR_gold")
BRONZE_TABLE = get_airflow_env_var("BQ_BRONZE_TABLE", "bronze_events")
DEADLETTER_TABLE = get_airflow_env_var("BQ_DEADLETTER_TABLE", "deadletter_events")
GOLD_TABLE = get_airflow_env_var("BQ_GOLD_TABLE", "fact_events")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 18),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def run_setup_infrastructure() -> None:
    """Verifica y crea la infraestructura GCP necesaria."""
    import sys as _sys
    project_root = resolve_project_root()
    if str(project_root) not in _sys.path:
        _sys.path.insert(0, str(project_root))

    from ingestion.setup_infrastructure import (
        create_pubsub_topic,
        create_pubsub_subscription,
        create_bigquery_dataset,
        create_bigquery_table,
        BRONZE_SCHEMA,
        DEADLETTER_SCHEMA,
    )

    create_pubsub_topic(PROJECT_ID, TOPIC_ID)
    create_pubsub_subscription(PROJECT_ID, TOPIC_ID, f"{TOPIC_ID}-sub")

    create_bigquery_dataset(PROJECT_ID, BQ_BRONZE_DATASET, REGION)
    create_bigquery_dataset(PROJECT_ID, BQ_SILVER_DATASET, REGION)
    create_bigquery_dataset(PROJECT_ID, BQ_GOLD_DATASET, REGION)

    create_bigquery_table(
        PROJECT_ID, BQ_BRONZE_DATASET, BRONZE_TABLE, BRONZE_SCHEMA,
        partition_field="ingestion_time",
        clustering_fields=["event_type", "customer_id"],
    )
    create_bigquery_table(
        PROJECT_ID, BQ_BRONZE_DATASET, DEADLETTER_TABLE, DEADLETTER_SCHEMA,
        partition_field="ingestion_time",
    )


def run_launch_dataflow():
    """Lee un batch rotativo de eventos y lo escribe a BigQuery."""
    from google.cloud import bigquery
    sys.path.insert(0, str(ROOT_DIR))
    from config.settings import get_credentials
    from datetime import datetime
    import json

    batches_dir = ROOT_DIR / "data" / "batches"
    if not batches_dir.exists():
        raise FileNotFoundError(f"No se encontro {batches_dir}. Ejecute generate_events.py primero.")

    batch_files = sorted(batches_dir.glob("batch_*.json"))
    if not batch_files:
        raise FileNotFoundError("No se encontraron archivos batch en data/batches/")

    batch_num = (datetime.utcnow().minute // 10) % len(batch_files) + 1
    input_file = batches_dir / f"batch_{batch_num:02d}.json"
    print(f"Processing batch {batch_num}/{len(batch_files)}: {input_file.name}")

    with open(input_file, "r", encoding="utf-8") as f:
        raw_events = json.load(f)

    rows = []
    for ev in raw_events:
        raw_ts = ev.get("event_timestamp")
        if raw_ts:
            if isinstance(raw_ts, (int, float)):
                ts = datetime.fromtimestamp(raw_ts)
            else:
                ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
        else:
            ts = datetime.utcnow()
        rows.append({
            "event_id": int(ev.get("event_id", 0)),
            "date_id": int(ts.strftime("%Y%m%d")),
            "customer_id": int(ev.get("customer_id", 0)),
            "product_id": int(ev.get("product_id", 0)),
            "session_id": str(ev.get("session_id", ""))[:255],
            "event_type": str(ev.get("event_type", "unknown"))[:50],
            "ingestion_time": datetime.utcnow().isoformat() + "Z",
        })

    table_id = f"{PROJECT_ID}.{BQ_BRONZE_DATASET}.{BRONZE_TABLE}"
    client = bigquery.Client(project=PROJECT_ID, credentials=get_credentials())
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    print(f"Inserted {len(rows)} rows from {input_file.name} into {table_id}")


def run_validate_data() -> None:
    """Valida la cantidad de registros en bronze y curated."""
    from google.cloud import bigquery
    sys.path.insert(0, str(ROOT_DIR))
    from config.settings import get_credentials

    client = bigquery.Client(project=PROJECT_ID, credentials=get_credentials())
    results = {}

    for table, dataset in [
        (BRONZE_TABLE, BQ_BRONZE_DATASET),
        ("stg_bronze_events", BQ_SILVER_DATASET),
        (GOLD_TABLE, BQ_GOLD_DATASET),
    ]:
        query = f"SELECT COUNT(*) AS total FROM `{PROJECT_ID}.{dataset}.{table}`"
        rows = list(client.query(query))
        count = rows[0].total
        results[f"{dataset}.{table}"] = count
        print(f"{dataset}.{table}: {count} registros")

    bronze_count = results.get(f"{BQ_BRONZE_DATASET}.{BRONZE_TABLE}", 0)
    if bronze_count == 0:
        raise ValueError("No hay datos en bronze_events - verifique la ingesta")
    print(f"Validacion OK: {json.dumps(results)}")


with DAG(
    dag_id="dag_eventos_realtime",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["gcp", "pubsub", "dataflow", "dbt"],
) as dag:

    setup_infra = PythonOperator(
        task_id="setup_infrastructure",
        python_callable=run_setup_infrastructure,
    )

    launch_dataflow = PythonOperator(
        task_id="launch_dataflow_job",
        python_callable=run_launch_dataflow,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt deps --profiles-dir . && "
            "dbt run --profiles-dir ."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --profiles-dir ."
        ),
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=run_validate_data,
    )

    setup_infra >> launch_dataflow >> dbt_run >> dbt_test >> validate_data
