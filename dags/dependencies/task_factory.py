"""Funciones auxiliares para los DAGs del pipeline de eventos."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def resolve_project_root() -> Path:
    """Devuelve la ruta raíz del proyecto."""
    return Path(__file__).resolve().parents[1]


def get_airflow_env_var(name: str, default: str | None = None) -> str | None:
    """Obtiene una variable de entorno o devuelve un valor por defecto."""
    return os.getenv(name, default)


def build_bq_table_ref(project_id: str, dataset_id: str, table_id: str) -> str:
    """Construye una referencia completa a una tabla de BigQuery."""
    return f"{project_id}:{dataset_id}.{table_id}"


def build_dataflow_parameters(project_id: str, topic_id: str, output_table: str, deadletter_table: str) -> dict[str, Any]:
    """Construye los parámetros del job de Dataflow."""
    return {
        "project": project_id,
        "topic": topic_id,
        "output-table": output_table,
        "deadletter-table": deadletter_table,
    }
