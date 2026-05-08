import os
import time
import csv
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "events")
DATA_PATH = "/app/data/sample_dataset.csv"
SLEEP_SECONDS = 0.2  # controls “real-time” speed

def main():
  producer = KafkaProducer(
      bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
      value_serializer=lambda v: str(v).encode("utf-8"),
  )

  with open(DATA_PATH, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
      row["ingest_ts"] = datetime.utcnow().isoformat()
      producer.send(TOPIC_NAME, row)
      time.sleep(SLEEP_SECONDS)

  producer.flush()

if __name__ == "__main__":
  main()
