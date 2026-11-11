"""Publish the Sentiment140 sample to Kafka (posts.raw) as source 'sentiment140'.
Event time = ingestion time (replaying a historical dataset; the stream's 7-day watermark would
otherwise treat 2009 timestamps as late data). Gold labels are NOT sent; they are used only by evaluate.py."""
import csv
import json
import os
from datetime import datetime, timezone

from kafka import KafkaProducer

p = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"), acks="all",
                  key_serializer=lambda k: k.encode(), value_serializer=lambda v: json.dumps(v).encode())
n = 0
with open("/work/sentiment140_sample.csv", newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        p.send("posts.raw", key="sentiment140", value={
            "id": r["id"], "source": "sentiment140", "author": r["user"], "text": r["text"],
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), "url": ""})
        n += 1
p.flush()
print("published", n, "records to posts.raw")
