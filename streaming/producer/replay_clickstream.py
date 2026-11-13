"""
Producteur Kafka : rejoue le fichier clickstream comme un flux d'événements.

C'est une SIMULATION de temps réel : les 500 000 événements du CSV Kaggle sont
publiés un par un dans le topic Kafka `clickstream`, au débit choisi
(--rate événements/seconde), comme s'ils arrivaient du site e-commerce.

Les valeurs sont envoyées telles quelles, sales comprises (dates vides ou
impossibles, URL manquantes...) : le nettoyage est le travail de la couche Silver,
pas du producteur.

Exemples :
    python replay_clickstream.py                      # tout le fichier, 2 000 év./s
    python replay_clickstream.py --rate 200 --limit 5000
    python replay_clickstream.py --rate 0             # le plus vite possible
"""

import argparse
import csv
import json
import os
import time
from pathlib import Path

DEFAULT_FILE = Path(__file__).resolve().parents[2] / "data" / "sample" / "clickstream_500k_events.csv"
FIELDS = ["event_id", "session_id", "customer_id", "event_type",
          "page_url", "device_id", "timestamp", "ingest_run_id"]


def to_message(row: dict) -> tuple[bytes, bytes]:
    """(clé, valeur) Kafka pour une ligne du CSV.
    Clé = event_id : répartit les événements sur les partitions et sert
    d'identifiant pour dédoublonner en aval. Valeur = JSON, tout en texte."""
    event = {field: (row.get(field) or "") for field in FIELDS}
    return event["event_id"].encode("utf-8"), json.dumps(event, ensure_ascii=False).encode("utf-8")


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    futures[topic].result()
    print(f"Topic '{topic}' créé ({partitions} partitions)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", default=os.environ.get("CLICKSTREAM_FILE", str(DEFAULT_FILE)))
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default="clickstream")
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--rate", type=float, default=2000, help="événements par seconde (0 = sans limite)")
    parser.add_argument("--limit", type=int, default=0, help="nombre max d'événements (0 = tout le fichier)")
    args = parser.parse_args()

    from confluent_kafka import Producer

    ensure_topic(args.bootstrap, args.topic, args.partitions)
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,   # pas de doublon côté Kafka en cas de nouvelle tentative
        "linger.ms": 20,
        "compression.type": "lz4",
    })

    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(err)

    sent, start = 0, time.monotonic()
    with open(args.file, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key, value = to_message(row)
            while True:
                try:
                    producer.produce(args.topic, key=key, value=value, on_delivery=on_delivery)
                    break
                except BufferError:          # file d'attente locale pleine : on laisse partir
                    producer.poll(0.1)
            producer.poll(0)
            sent += 1
            if args.rate > 0:                # régulation du débit
                ahead = sent / args.rate - (time.monotonic() - start)
                if ahead > 0:
                    time.sleep(ahead)
            if sent % 50_000 == 0:
                print(f"{sent} événements envoyés ({sent / (time.monotonic() - start):.0f}/s)")
            if args.limit and sent >= args.limit:
                break

    producer.flush(60)
    if errors:
        raise SystemExit(f"ECHEC : {len(errors)} événements non livrés (ex. : {errors[0]})")
    print(f"Terminé : {sent} événements publiés dans '{args.topic}' en {time.monotonic() - start:.1f} s")


if __name__ == "__main__":
    main()
