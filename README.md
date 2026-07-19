# Fase 6 - Pipeline automatizado con BigQuery, Airflow y dbt

## Objetivo
Automatizar la ingesta, el procesamiento y la transformacion de eventos en tiempo real mediante un pipeline medallion (bronze -> silver -> gold) con orquestacion en Airflow, transformaciones en dbt y almacenamiento en BigQuery.

## Arquitectura

```
                         CAPAS MEDALLION

  ┌─────────────────────────────────────────────────────────┐
  │  BRONZE (nR_bronze)                                     │
  │  ├── bronze_events     ← datos crudos, duplicados OK    │
  │  └── deadletter_events ← mensajes con error             │
  ├─────────────────────────────────────────────────────────┤
  │  SILVER (nR_silver)                                     │
  │  └── stg_bronze_events ← view, limpieza basica          │
  ├─────────────────────────────────────────────────────────┤
  │  GOLD (nR_gold)                                         │
  │  └── fact_events       ← table, deduplicada + enriched  │
  └─────────────────────────────────────────────────────────┘

  Flujo: JSON batches → Bronze → Silver (dbt) → Gold (dbt)
```

### Flujo detallado
```
data/batches/batch_XX.json (10 archivos, 50 eventos c/u)
    │
    ▼
BigQuery nR_bronze.bronze_events (ingesta raw)
    │
    ▼
dbt: nR_silver.stg_bronze_events (view - limpieza + filtros)
    │
    ▼
dbt: nR_gold.fact_events (table - deduplicada con ROW_NUMBER)
    │
    ▼
dbt test (11 quality checks)
    │
    ▼
Validacion de registros (bronze, silver, gold)
```

## Stack tecnologico
| Componente | Tecnologia | Funcion |
|------------|------------|---------|
| Orquestacion | Apache Airflow 2.7.1 | DAG scheduler (Docker) |
| Transformacion | dbt 1.12.0 | SQL modular y testable |
| Almacenamiento | Google BigQuery | Data warehouse analitico |
| Ingesta | Google Cloud Python Client | Carga directa a BigQuery |
| Testing | pytest + dbt tests | Validacion end-to-end |

### Stack local (testing)
| Componente | Tecnologia | Funcion |
|------------|------------|---------|
| Pipeline | Apache Beam 2.75.0 | Procesamiento DirectRunner |
| Generador | Python CLI | Generacion de eventos |

## Estructura del proyecto
```
dags/
  dag_eventos_realtime.py          # DAG principal (5 tasks)
  dependencies/
    __init__.py
    task_factory.py                # Helpers para Airflow

dataflow/
  streaming_pipeline.py            # Pipeline Beam (Dataflow + DirectRunner)

dbt/
  dbt_project.yml                  # Configuracion dbt
  profiles.yml                     # Conexion BigQuery (dataset: nR)
  models/
    staging/
      stg_bronze_events.sql        # Vista de limpieza
      schema.yml                   # Tests de calidad (11 tests)
    marts/
      fact_events.sql              # Tabla deduplicada
      schema.yml                   # Tests de la tabla final

ingestion/
  pubsub_publisher.py              # Simulador de ingesta Pub/Sub
  setup_infrastructure.py          # Creacion de datasets y tablas GCP

data/
  batches/                         # 10 archivos batch (50 eventos c/u)
    batch_01.json ... batch_10.json
  generate/
    generate_events.py             # Generador CLI con seed

config/
  settings.py                      # Configuracion centralizada

tests/
  test_streaming_pipeline.py       # Tests del pipeline Beam
  test_dag_import.py               # Tests de estructura del DAG
  test_generate_events.py          # Tests del generador
  test_settings.py                 # Tests de configuracion
  test_task_factory.py             # Tests de helpers
```

## DAG de Airflow
El DAG `dag_eventos_realtime` ejecuta 5 tasks en secuencia:

```
setup_infra >> launch_dataflow >> dbt_run >> dbt_test >> validate_data
```

| # | Task | Tipo | Descripcion |
|---|------|------|-------------|
| 1 | `setup_infrastructure` | PythonOperator | Crea 3 datasets (bronze, silver, gold) y tablas en BigQuery |
| 2 | `launch_dataflow_job` | PythonOperator | Lee batch rotativo de JSON y lo inserta en bronze_events |
| 3 | `dbt_run` | BashOperator | Ejecuta dbt run: crea view en silver, table en gold |
| 4 | `dbt_test` | BashOperator | Ejecuta 11 dbt tests de calidad de datos |
| 5 | `validate_data` | PythonOperator | Cuenta registros en las 3 capas y valida |

### Schedule
- Frecuencia: cada 10 minutos (`*/10 * * * *`)
- catchup: desactivado (no ejecuta rangos atrasados)
- Retries: 2 por task, delay de 2 minutos
- Batch rotation: rota entre batch_01 a batch_10 cada 10 minutos

### Tests de dbt (11 tests)
Los tests verifican automaticamente en cada ejecucion:

**Silver (staging):**
- `event_id`: not_null
- `date_id`: not_null
- `event_type`: accepted_values (login, view, add_to_cart, checkout, purchase, cart_abandoned)
- `ingestion_time`: not_null

**Gold (fact_events):**
- `event_id`: unique + not_null
- `date_id`: not_null
- `event_type`: not_null + accepted_values
- `ingestion_time`: not_null
- `transformed_at`: not_null

## Ejecucion local

### 1. Generar eventos
```bash
python data/generate/generate_events.py --num-events 500 --seed 42
```

### 2. Ejecutar pipeline con DirectRunner
```bash
python dataflow/streaming_pipeline.py --runner DirectRunner --input-file data/eventos_ing.json
```

### 3. Ejecutar tests
```bash
pytest tests/ -v
```

### 4. Levantar Airflow
```bash
docker-compose up --build -d
```
Acceso: http://localhost:8080 (admin/admin)

### 5. Parar Airflow
```bash
docker-compose down
```

### Credenciales GCP
El archivo `config/gcp_credentials.json` contiene la service account de GCP.
**Nunca subir este archivo a git.**

## Justificacion de arquitectura

### Patron Medallion (Bronze -> Silver -> Gold)
- **Bronze** (`nR_bronze`): Datos crudos tal como llegan del generador. Duplicados permitidos (eventos reinsertados en cada ciclo).
- **Silver** (`nR_silver`): Vista de limpieza. Filtra nulos, tipa campos. Sin deduplicacion (refleja la realidad del stream).
- **Gold** (`nR_gold`): Tabla deduplicada con `ROW_NUMBER()` sobre `event_id`. Lista para consumo analitico.

### Por que dbt?
- **SQL modular**: Transformaciones mantenibles y versionables
- **Testing automatico**: Validacion de calidad de datos en cada ejecucion
- **Documentacion**: Descripciones auto-generadas de modelos y columnas
- **Lineage**: Dependencias claras entre modelos (staging -> marts)

### Por que Airflow?
- **Orquestacion central**: Coordina ingesta, transformacion y validacion
- **Monitoreo**: UI para visualizar estado de pipelines y logs
- **Reintentos**: Manejo de fallos automatico con backoff
- **Programacion**: Cron-like para ejecuciones periodicas

### Por que BigQuery?
- **Serverless**: Sin gestion de infraestructura
- **Free tier**: 1 TB de queries y 10 GB de almacenamiento gratis al mes
- **Integracion nativa**: Conectores oficiales de Python y dbt
