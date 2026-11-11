"""Make a small balanced sample from the Sentiment140 training CSV (cp1252, no header).
usage: python make_sample.py <training.csv> <out.csv> [n_per_class]"""
import csv
import random
import sys

src, dst = sys.argv[1], sys.argv[2]
n = int(sys.argv[3]) if len(sys.argv) > 3 else 2000
random.seed(42)

pos, neg = [], []
with open(src, newline="", encoding="cp1252", errors="replace") as f:
    for polarity, tid, _date, _q, user, text in csv.reader(f):
        (pos if polarity == "4" else neg).append((tid, "positive" if polarity == "4" else "negative", user, text))

rows = random.sample(pos, n) + random.sample(neg, n)
random.shuffle(rows)
with open(dst, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["id", "gold", "user", "text"])
    w.writerows(rows)
print(f"wrote {len(rows)} rows ({n} positive, {n} negative) to {dst}")
