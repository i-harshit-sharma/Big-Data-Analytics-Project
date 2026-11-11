"""HBase helpers shared by the streaming and batch jobs (HBase Thrift via happybase)."""
import time
import zlib

import happybase

HBASE_HOST = "hbase"
THRIFT_PORT = 9090

# table -> column families
TABLES = {
    "raw_posts": ["r"],          # r:json, r:ingested
    "processed_posts": ["d", "s", "n"],  # d: data, s: sentiment, n: nlp
    "topics": ["t"],             # t:words, t:count   (written by batch LDA)
    "analytics": ["a"],          # a:pos, a:neg, a:neu (written by batch job)
}


def connect(retries=1):
    last = None
    for _ in range(retries):
        try:
            c = happybase.Connection(HBASE_HOST, port=THRIFT_PORT, timeout=120000)
            c.open()
            return c
        except Exception as e:  # noqa: BLE001
            last = e
            print("waiting for HBase thrift:", e, flush=True)
            time.sleep(5)
    raise RuntimeError(f"cannot reach HBase: {last}")


def ensure_tables():
    c = connect(retries=60)
    existing = {t.decode() for t in c.tables()}
    for name, fams in TABLES.items():
        if name not in existing:
            c.create_table(name, {f: dict() for f in fams})
            print("created table", name, flush=True)
    c.close()


def rowkey(source, post_id):
    """2-digit salt (8 buckets) avoids hotspotting on a single region."""
    salt = zlib.crc32(f"{source}|{post_id}".encode()) % 8
    return f"{salt:02d}|{source}|{post_id}"
