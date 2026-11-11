"""Kafka producer: live REAL posts/comments from the public Hacker News API (no credentials).
Other real sources are loaded separately (see dataset/). Messages are JSON, keyed by source
(so one source stays ordered within a partition)."""
import html
import json
import os
import re
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "posts.raw"


def wait_for_kafka():
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
            for name, parts in ((TOPIC, 3), ("posts.dlq", 1)):
                try:
                    admin.create_topics([NewTopic(name, parts, 1)])  # replication 1: single broker
                except TopicAlreadyExistsError:
                    pass
            admin.close()
            return
        except Exception as e:  # noqa: BLE001
            print("waiting for Kafka:", e, flush=True)
            time.sleep(5)


def hn_posts(seen):
    out = []
    try:
        ids = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json", timeout=10).json()[:25]
        for sid in ids:
            story = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{sid}.json", timeout=10).json() or {}
            items = [story] + [requests.get(f"https://hacker-news.firebaseio.com/v0/item/{k}.json", timeout=10).json() or {}
                               for k in (story.get("kids") or [])[:2]]
            for it in items:
                txt = it.get("title") or it.get("text")
                if not txt or it.get("id") in seen:
                    continue
                seen.add(it["id"])
                txt = re.sub(r"<[^>]+>", " ", html.unescape(txt))
                out.append({"id": f"hn-{it['id']}", "source": "hackernews", "author": it.get("by", ""),
                            "text": txt, "url": it.get("url", ""),
                            "created_at": datetime.fromtimestamp(it.get("time", time.time()), timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")})
    except Exception as e:  # noqa: BLE001
        print("hackernews fetch failed (continuing):", e, flush=True)
    return out


def main():
    wait_for_kafka()
    p = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", retries=5, linger_ms=20,
                      key_serializer=lambda k: k.encode(), value_serializer=lambda v: json.dumps(v).encode())
    # two deliberately malformed messages to demonstrate the dead-letter topic
    p.send(TOPIC, key="bad", value={"id": None, "source": "hackernews", "text": ""})
    p.send(TOPIC, key="bad", value={"oops": "no required fields"})
    p.flush()

    seen = set()
    while True:
        for m in hn_posts(seen):
            p.send(TOPIC, key=m["source"], value=m)
        p.flush()
        print("hackernews batch sent; total seen", len(seen), flush=True)
        time.sleep(60)


if __name__ == "__main__":
    main()
