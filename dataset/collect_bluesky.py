"""Collect public Bluesky posts from the Jetstream firehose and save them as JSON Lines.
RUN THIS ON A MACHINE WHOSE NETWORK ALLOWS BLUESKY (it is blocked on the project laptop's network).

  pip install websockets
  python collect_bluesky.py --max 3000 --minutes 10 --lang en --out bluesky_posts.jsonl

Notes
- Jetstream is Bluesky's public stream of repo events; no login or API key is needed (not verified from the
  project laptop, which cannot reach Bluesky). Check Bluesky's current terms before publishing collected data.
- Only public post text is stored. The author DID is replaced by a short one-way hash to avoid keeping an
  identifier, and the text is kept as posted.
- Output line: {"id","source","author","text","created_at","lang"}; created_at is UTC 'YYYY-MM-DDTHH:MM:SS'.
"""
import argparse
import asyncio
import hashlib
import json
import sys
import time
from datetime import datetime, timezone

import websockets

URL = "wss://{host}/subscribe?wantedCollections=app.bsky.feed.post"
HOSTS = ["jetstream2.us-east.bsky.network", "jetstream1.us-east.bsky.network",
         "jetstream1.us-west.bsky.network", "jetstream2.us-west.bsky.network"]


def parse_time(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:  # noqa: BLE001
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


async def collect(args):
    deadline = time.time() + args.minutes * 60
    n = 0
    seen = set()
    with open(args.out, "a", encoding="utf-8") as out:
        for host in HOSTS:
            if n >= args.max or time.time() >= deadline:
                break
            try:
                async with websockets.connect(URL.format(host=host), max_size=2**20) as ws:
                    print("connected to", host, flush=True)
                    while n < args.max and time.time() < deadline:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            continue
                        ev = json.loads(raw)
                        c = ev.get("commit") or {}
                        if ev.get("kind") != "commit" or c.get("operation") != "create":
                            continue
                        rec = c.get("record") or {}
                        text = (rec.get("text") or "").strip()
                        langs = rec.get("langs") or []
                        if len(text) < args.min_chars or (args.lang and args.lang not in langs):
                            continue
                        pid = f"{ev['did']}/{c['rkey']}"
                        if pid in seen:
                            continue
                        seen.add(pid)
                        out.write(json.dumps({
                            "id": "bsky-" + hashlib.sha256(pid.encode()).hexdigest()[:16],
                            "source": "bluesky",
                            "author": hashlib.sha256(ev["did"].encode()).hexdigest()[:10],
                            "text": text,
                            "created_at": parse_time(rec.get("createdAt", "")),
                            "lang": langs[0] if langs else "",
                        }, ensure_ascii=False) + "\n")
                        n += 1
                        if n % 100 == 0:
                            print(f"collected {n}", flush=True)
            except Exception as e:  # noqa: BLE001
                print(f"{host} failed: {e}", file=sys.stderr, flush=True)
    print(f"done: {n} posts written to {args.out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=3000, help="stop after this many posts")
    ap.add_argument("--minutes", type=float, default=10, help="stop after this many minutes")
    ap.add_argument("--lang", default="en", help="keep posts tagged with this language ('' = all languages)")
    ap.add_argument("--min-chars", type=int, default=20)
    ap.add_argument("--out", default="bluesky_posts.jsonl")
    asyncio.run(collect(ap.parse_args()))
