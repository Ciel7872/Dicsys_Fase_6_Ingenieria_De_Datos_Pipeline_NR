#!/usr/bin/env python3
"""
Generador de eventos de prueba para el pipeline de ingesta en tiempo real.
Genera eventos consistentes con el esquema bronze de BigQuery.

Uso:
    python data/generate/generate_events.py
    python data/generate/generate_events.py --num-events 1000
    python data/generate/generate_events.py --num-events 500 --output data/eventos_ing.json
"""
import json
import random
import argparse
from datetime import datetime, timedelta
from pathlib import Path

EVENT_TYPES = ["login", "view", "add_to_cart", "checkout", "purchase", "cart_abandoned"]
CUSTOMER_IDS = list(range(1, 251))
PRODUCT_IDS = list(range(1, 151))
SESSION_IDS = [f"sess_{i:06d}" for i in range(1, 201)]


def generate_events(num_events: int, start_date: datetime, days_range: int = 7) -> list[dict]:
    events = []
    for i in range(1, num_events + 1):
        random_days = random.randint(0, days_range)
        random_seconds = random.randint(0, 86400)
        timestamp = start_date + timedelta(days=random_days, seconds=random_seconds)
        product_id = random.choice(PRODUCT_IDS) if random.random() > 0.2 else 0
        events.append({
            "event_id": 2000 + i,
            "event_type": random.choice(EVENT_TYPES),
            "customer_id": random.choice(CUSTOMER_IDS),
            "product_id": product_id,
            "session_id": random.choice(SESSION_IDS),
            "event_timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
    return events


def main():
    parser = argparse.ArgumentParser(description="Generador de eventos de prueba")
    parser.add_argument("--num-events", type=int, default=500, help="Cantidad de eventos a generar (default: 500)")
    parser.add_argument("--output", type=str, default=None, help="Ruta de salida (default: data/eventos_ing.json)")
    parser.add_argument("--start-date", type=str, default=None, help="Fecha de inicio YYYY-MM-DD (default: hace 7 dias)")
    parser.add_argument("--days-range", type=int, default=7, help="Rango de dias para timestamps (default: 7)")
    parser.add_argument("--seed", type=int, default=None, help="Seed para reproducibilidad")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else datetime.now() - timedelta(days=args.days_range)

    project_root = Path(__file__).resolve().parents[2]
    output_path = Path(args.output) if args.output else project_root / "data" / "eventos_ing.json"

    events = generate_events(args.num_events, start_date, args.days_range)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, ensure_ascii=False)

    print(f"Generados {len(events)} eventos en {output_path}")


if __name__ == "__main__":
    main()
