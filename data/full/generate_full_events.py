import json
import random
from datetime import datetime, timedelta

# Configuración
NUM_EVENTS = 500
OUTPUT_FILE = "data/full/eventos_full.json"

# Listas de valores posibles
EVENT_TYPES = ["login", "view", "add_to_cart", "checkout", "purchase", "cart_abandoned"]
CUSTOMER_IDS = list(range(1, 251))  # 250 clientes
PRODUCT_IDS = list(range(1, 151))   # 150 productos
SESSION_IDS = [f"sess_{i:06d}" for i in range(1, 201)]

# Fecha de inicio: últimos 7 días
start_date = datetime(2026, 7, 11, 0, 0, 0)

events = []
for i in range(1, NUM_EVENTS + 1):
    # Generar timestamp aleatorio en los últimos 7 días
    random_days = random.randint(0, 7)
    random_seconds = random.randint(0, 86400)
    timestamp = start_date + timedelta(days=random_days, seconds=random_seconds)
    timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

    event = {
        "event_id": 2000 + i,  # IDs únicos a partir de 2001
        "event_type": random.choice(EVENT_TYPES),
        "customer_id": random.choice(CUSTOMER_IDS),
        "product_id": random.choice(PRODUCT_IDS) if random.random() > 0.2 else 0,  # 20% productos nulos
        "session_id": random.choice(SESSION_IDS),
        "event_timestamp": timestamp_str,
    }
    events.append(event)

# Guardar archivo
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(events, f, indent=2, ensure_ascii=False)

print(f"✅ Archivo generado: {OUTPUT_FILE} con {len(events)} eventos")