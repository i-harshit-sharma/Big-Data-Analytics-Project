"""Bulk-load recent Hacker News stories and their comments into Kafka (posts.raw) as source 'hackernews'.
Public API, no credentials. Only items from the last 6 days are sent (the stream's 7-day watermark).
Ids are 'hn-<item id>', identical to the live producer, so re-running never creates duplicates in HBase."""
import html
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

API = "https://hacker-news.firebaseio.com/v0"
MAX_ITEMS = int(os.getenv("HN_MAX_ITEMS", "3000"))
COMMENTS_PER_STORY = int(os.getenv("HN_COMMENTS_PER_STORY", "12"))
sess = requests.Session()


def item(i):
    for _ in range(3):
        try:
            return sess.get(f"{API}/item/{i}.json", timeout=10).json() or {}
        except Exception:  # noqa: BLE001
            time.sleep(0.5)
    return {}


def story_ids():
    ids = []
    for lst in ("topstories", "newstories", "beststories", "askstories", "showstories"):
        ids += sess.get(f"{API}/{lst}.json", timeout=15).json()[:500]
    return list(dict.fromkeys(ids))


def comments_of(story):
    """Breadth-first over kids, capped per story."""
    out, queue = [], list(story.get("kids") or [])
    while queue and len(out) < COMMENTS_PER_STORY:
        batch, queue = queue[:COMMENTS_PER_STORY], queue[COMMENTS_PER_STORY:]
        for c in map(item, batch):
            if c and not c.get("deleted") and not c.get("dead"):
                out.append(c)
                queue += c.get("kids") or []
    return out


def to_msg(it):
    txt = it.get("title") or it.get("text")
    if not txt or it.get("deleted") or it.get("dead"):
        return None
    txt = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html.unescape(txt))).strip()
    if len(txt) < 15 or time.time() - it.get("time", 0) > 6 * 86400:
        return None
    return {"id": f"hn-{it['id']}", "source": "hackernews", "author": it.get("by", ""), "text": txt,
            "url": it.get("url", ""), "created_at": datetime.fromtimestamp(it["time"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}


def main():
    p = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"), acks="all",
                      key_serializer=lambda k: k.encode(), value_serializer=lambda v: json.dumps(v).encode())
    ids = story_ids()
    print("story ids:", len(ids), flush=True)
    def fetch_story(i):  # story + its comments, fetched inside a worker thread
        s = item(i)
        return ([s] + comments_of(s)) if s else []

    sent, seen = 0, set()
    with ThreadPoolExecutor(24) as pool:
        for items in pool.map(fetch_story, ids):
            if sent >= MAX_ITEMS:
                break
            for it in items:
                m = to_msg(it)
                if m and m["id"] not in seen:
                    seen.add(m["id"])
                    p.send("posts.raw", key="hackernews", value=m)
                    sent += 1
            if sent % 500 < 40:
                print("sent", sent, flush=True)
    p.flush()
    print("published", sent, "Hacker News items to posts.raw")


if __name__ == "__main__":
    main()
