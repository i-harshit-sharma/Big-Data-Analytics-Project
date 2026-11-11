"""Publish collected Bluesky posts (bluesky_posts.jsonl) to Kafka topic posts.raw.
Run inside the Docker network (see dataset/BLUESKY.md). Real post timestamps are kept, so collect
within the last 7 days (the stream's watermark) or late posts may be dropped from deduplication state."""
import json
import os

from kafka import KafkaProducer

p = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"), acks="all",
                  key_serializer=lambda k: k.encode(), value_serializer=lambda v: json.dumps(v).encode())
n = 0
with open("/work/bluesky_posts.jsonl", encoding="utf-8") as f:
    for line in f:
        r = json.loads(line)
        p.send("posts.raw", key="bluesky", value={
            "id": r["id"], "source": "bluesky", "author": r.get("author", ""), "text": r["text"],
            "created_at": r["created_at"], "url": ""})
        n += 1
p.flush()
print("published", n, "bluesky posts to posts.raw")
