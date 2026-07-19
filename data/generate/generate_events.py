#!/usr/bin/env python3
"""
Generador de eventos de prueba para el pipeline de ingesta en tiempo real.
Genera 10 archivos batch con eventos unicos para simular ingesta rotativa.

Uso:
    python data/generate/generate_events.py
    python data/generate/generate_events.py --num-batches 10 --events-per-batch 50
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


def generate_batch(batch_num: int, events_per_batch: int, start_event_id: int, start_date: datetime, days_range: int = 7) -> list[dict]:
    events = []
    for i in range(events_per_batch):
        event_id = start_event_id + (batch_num * events_per_batch) + i
        random_days = random.randint(0, days_range)
        random_seconds = random.randint(0, 86400)
        timestamp = start_date + timedelta(days=random_days, seconds=random_seconds)
        product_id = random.choice(PRODUCT_IDS) if random.random() > 0.2 else 0
        events.append({
            "event_id": event_id,
            "event_type": random.choice(EVENT_TYPES),
            "customer_id": random.choice(CUSTOMER_IDS),
            "product_id": product_id,
            "session_id": random.choice(SESSION_IDS),
            "event_timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
    return events


def main():
    parser = argparse.ArgumentParser(description="Generador de eventos de prueba")
    parser.add_argument("--num-batches", type=int, default=10, help="Cantidad de archivos batch (default: 10)")
    parser.add_argument("--events-per-batch", type=int, default=50, help="Eventos por batch (default: 50)")
    parser.add_argument("--start-event-id", type=int, default=1000, help="ID inicial (default: 1000)")
    parser.add_argument("--output-dir", type=str, default=None, help="Directorio de salida")
    parser.add_argument("--start-date", type=str, default=None, help="Fecha de inicio YYYY-MM-DD")
    parser.add_argument("--days-range", type=int, default=7, help="Rango de dias para timestamps")
    parser.add_argument("--seed", type=int, default=None, help="Seed para reproducibilidad")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else datetime.now() - timedelta(days=args.days_range)

    project_root = Path(__file__).resolve().parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "data" / "batches"
    output_dir.mkdir(parents=True, exist_ok=True)

    total_events = 0
    for batch_num in range(args.num_batches):
        events = generate_batch(batch_num, args.events_per_batch, args.start_event_id, start_date, args.days_range)
        output_path = output_dir / f"batch_{batch_num + 1:02d}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(events, f, indent=2, ensure_ascii=False)
        total_events += len(events)
        print(f"Batch {batch_num + 1}: {len(events)} eventos -> {output_path}")

    print(f"\nTotal: {total_events} eventos en {args.num_batches} archivos en {output_dir}")


if __name__ == "__main__":
    main()
