"""obj: carga las credenciales desde variables de entorno y define constantes globales."""

import os
from pathlib import Path
import logging
from google.oauth2 import service_account

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- VARIABLES DE ENTORNO ----------
# Cargar desde .env (opcional, instalar python-dotenv)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
except ImportError:
    logger.info("python-dotenv no está instalado; se usarán las variables del entorno actual.")

# Proyecto
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dataleaguenovaretail")
REGION = os.getenv("GCP_REGION", "us-south1")

# Pub/Sub
TOPIC_ID = os.getenv("PUBSUB_TOPIC", "eventos-realtime")
SUBSCRIPTION_ID = os.getenv("PUBSUB_SUBSCRIPTION", "eventos-realtime-sub")

# BigQuery - Capas Medallion
BQ_BRONZE_DATASET = os.getenv("BQ_BRONZE_DATASET", "nR_bronze")
BQ_SILVER_DATASET = os.getenv("BQ_SILVER_DATASET", "nR_silver")
BQ_GOLD_DATASET = os.getenv("BQ_GOLD_DATASET", "nR_gold")
BRONZE_TABLE = os.getenv("BQ_BRONZE_TABLE", "bronze_events")
DEADLETTER_TABLE = os.getenv("BQ_DEADLETTER_TABLE", "deadletter_events")
SILVER_TABLE = os.getenv("BQ_SILVER_TABLE", "stg_bronze_events")
GOLD_TABLE = os.getenv("BQ_GOLD_TABLE", "fact_events")

# Rutas de credenciales
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "config/gcp_credentials.json")


def resolve_credentials_path() -> Path:
    """Resuelve la ruta de las credenciales de forma robusta."""
    raw_value = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", str(CREDENTIALS_PATH))
    path = Path(raw_value).expanduser()
    if not path.is_absolute():
        path = (Path(__file__).resolve().parent.parent / path).resolve()
    return path


CREDENTIALS_PATH = resolve_credentials_path()

# Verificar que el archivo de credenciales existe
if not CREDENTIALS_PATH.exists():
    logger.warning(f"No se encontró el archivo de credenciales en {CREDENTIALS_PATH}. "
                   "Asegúrate de tenerlo en config/ o de establecer la variable de entorno.")

# ---------- FUNCIÓN PARA OBTENER CLIENTES AUTENTICADOS ----------
def get_credentials():
    """Devuelve las credenciales de Google Cloud usando el archivo de servicio."""
    path = resolve_credentials_path()
    if path.exists():
        credentials = service_account.Credentials.from_service_account_file(
            str(path),
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        logger.info(f"Cargadas credenciales desde {path}")
        return credentials
    else:
        # Si no existe, intenta con las credenciales por defecto de GCP
        logger.warning("Usando credenciales por defecto (Application Default Credentials).")
        return None

# ---------- CONSTANTES ADICIONALES ----------
# Buckets de GCS (necesarios para Dataflow)
STAGING_BUCKET = os.getenv("GCS_STAGING_BUCKET", "novaretail-dataflow-staging")
TEMP_BUCKET = os.getenv("GCS_TEMP_BUCKET", "novaretail-temp")

# Otras configuraciones
BATCH_SIZE = int(os.getenv("PUBSUB_BATCH_SIZE", 50))
SLEEP_BETWEEN_BATCHES = float(os.getenv("PUBSUB_SLEEP_SECONDS", 0.5))