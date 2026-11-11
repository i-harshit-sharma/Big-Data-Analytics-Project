"""Delete all rows of the given sources from HBase (raw_posts, processed_posts, analytics).
usage (inside the Docker network): python remove_source.py sample_forum sample_reviews"""
import sys

import happybase

sources = {s.encode() for s in sys.argv[1:]}
c = happybase.Connection("hbase", port=9090, timeout=60000)

for table, col in (("raw_posts", None), ("processed_posts", b"d:source")):
    t = c.table(table)
    n = 0
    with t.batch(batch_size=500) as b:
        if col:
            for k, d in t.scan(columns=[col]):
                if d.get(col) in sources:
                    b.delete(k)
                    n += 1
        else:  # row key is salt|source|id
            for k, _ in t.scan(columns=[b"r:ingested"]):
                if k.split(b"|")[1] in sources:
                    b.delete(k)
                    n += 1
    print(f"{table}: deleted {n} rows")

t = c.table("analytics")
n = 0
with t.batch() as b:
    for k, _ in t.scan():
        if k.split(b"|")[-1] in sources:
            b.delete(k)
            n += 1
print(f"analytics: deleted {n} rows")
