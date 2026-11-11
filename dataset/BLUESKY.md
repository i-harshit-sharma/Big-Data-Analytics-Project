# Bluesky data: collect elsewhere, load here

Bluesky is blocked on the project laptop's network, so collection runs on another machine.

## 1. On the other machine (any OS with Python 3.9+)
Copy only `collect_bluesky.py`, then:
```
pip install websockets
python collect_bluesky.py --max 3000 --minutes 10 --lang en --out bluesky_posts.jsonl
```
Expected: lines like `connected to jetstream2.us-east.bsky.network` and `collected 100`, ending with
`done: N posts written to bluesky_posts.jsonl`. If every host prints "failed", that network blocks Bluesky too.
Use `--lang ""` to keep all languages (the pipeline only scores English reliably).

## 2. On the project laptop
Copy `bluesky_posts.jsonl` into this `dataset/` folder (within 7 days of collecting), then, in PowerShell from
the `social-trends` folder, with the stack running:
```
docker run --rm --network social-trends_lab -v "${PWD}/dataset:/work" social-trends-producer python /work/load_bluesky.py
```
Expected: `published N bluesky posts to posts.raw`. Within about a minute the dashboard's platform chart shows a
`bluesky` source (watch with `docker compose logs --tail 5 spark-stream`).

## Status
Written but NOT run: the project laptop cannot reach Bluesky, so the Jetstream message format
(`commit.record.text`, `createdAt`, `langs`) is from memory of the public docs and unverified.
If the collector writes 0 posts while connected, print one raw message and adjust the field names.
Bluesky has no labels, so there is no accuracy score for it; only the dashboard analytics apply.
