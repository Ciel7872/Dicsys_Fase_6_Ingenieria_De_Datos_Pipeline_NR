"""DAG de Airflow para orquestar el pipeline de eventos en tiempo real."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dependencies.task_factory import (
    build_bq_table_ref,
    build_dataflow_parameters,
    get_airflow_env_var,
    resolve_project_root,
)

PROJECT_ID = get_airflow_env_var("GCP_PROJECT_ID", "dataleaguenovaretail")
REGION = get_airflow_env_var("GCP_REGION", "us-south1")
TOPIC_ID = get_airflow_env_var("PUBSUB_TOPIC", "eventos-realtime")
BQ_DATASET = get_airflow_env_var("BQ_DATASET", "nR_core_datasets")
BRONZE_TABLE = get_airflow_env_var("BQ_BRONZE_TABLE", "bronze_events")
DEADLETTER_TABLE = get_airflow_env_var("BQ_DEADLETTER_TABLE", "deadletter_events")
TEMPLATE_GCS_PATH = get_airflow_env_var(
    "DATAFLOW_TEMPLATE_GCS_PATH",
    "gs://novaretail-dataflow-templates/pubsub-to-bq-streaming.json",
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_eventos_realtime",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["gcp", "pubsub", "dataflow"],
) as dag:

    def run_transform_sql() -> None:
        sql_path = resolve_project_root() / "sql" / "transforms" / "transform_bronze_to_curated.sql"
        sql_path = sql_path.resolve()
        print(f"Ejecutando SQL desde {sql_path}")

    run_transform = PythonOperator(
        task_id="run_transform_sql",
        python_callable=run_transform_sql,
    )

    launch_dataflow = BashOperator(
        task_id="launch_dataflow_job",
        bash_command=(
            "gcloud dataflow flex-template run "
            f'"streaming-events-{{{{ ts_nodash }}}}" '
            f"--template-file-gcs-location={TEMPLATE_GCS_PATH} "
            f'--parameters="project={PROJECT_ID},topic={TOPIC_ID},output-table={build_bq_table_ref(PROJECT_ID, BQ_DATASET, BRONZE_TABLE)},deadletter-table={build_bq_table_ref(PROJECT_ID, BQ_DATASET, DEADLETTER_TABLE)}" '
            f"--region={REGION}"
        ),
        dag=dag,
    )

    launch_dataflow >> run_transform
