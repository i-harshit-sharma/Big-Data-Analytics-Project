"""Compare pipeline sentiment (read from HBase) with Sentiment140 gold labels. Real measurement, no assumptions.
Gold is binary (positive/negative); the pipeline can also answer 'neutral', which counts as a miss."""
import csv
from collections import Counter

import happybase

gold = {r["id"]: r["gold"] for r in csv.DictReader(open("/work/sentiment140_sample.csv", encoding="utf-8"))}
c = happybase.Connection("hbase", port=9090, timeout=30000)
pred = {}
for _, d in c.table("processed_posts").scan(columns=[b"d:id", b"d:source", b"s:label"]):
    if d.get(b"d:source") == b"sentiment140":
        pred[d[b"d:id"].decode()] = d[b"s:label"].decode()

ids = [i for i in gold if i in pred]
print(f"gold rows: {len(gold)} | found in HBase: {len(ids)}")
if not ids:
    raise SystemExit("nothing processed yet")

conf = Counter((gold[i], pred[i]) for i in ids)
print("\nconfusion (gold -> predicted):")
for g in ("positive", "negative"):
    print(f"  {g:9s}", {p: conf[(g, p)] for p in ("positive", "negative", "neutral")})

correct = sum(conf[(g, g)] for g in ("positive", "negative"))
print(f"\naccuracy (neutral counted wrong): {correct / len(ids):.3f}")
decided = [i for i in ids if pred[i] != "neutral"]
print(f"accuracy on decided (non-neutral) predictions: {sum(gold[i] == pred[i] for i in decided) / len(decided):.3f} ({len(decided)} rows)")
print(f"predicted neutral: {len(ids) - len(decided)} rows")
print()
f1s = []
for cls in ("positive", "negative"):
    tp = conf[(cls, cls)]
    fp = sum(conf[(o, cls)] for o in ("positive", "negative") if o != cls)
    fn = sum(conf[(cls, p)] for p in ("negative", "positive", "neutral") if p != cls)
    pr = tp / (tp + fp) if tp + fp else 0
    rc = tp / (tp + fn) if tp + fn else 0
    f1 = 2 * pr * rc / (pr + rc) if pr + rc else 0
    f1s.append(f1)
    print(f"{cls:9s} precision {pr:.3f}  recall {rc:.3f}  F1 {f1:.3f}")
print(f"macro-F1 (2 gold classes): {sum(f1s) / 2:.3f}")
