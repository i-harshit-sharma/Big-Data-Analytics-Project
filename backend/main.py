"""FastAPI backend. The React app talks only to this API; only this service talks to HBase (via Thrift)."""
import asyncio
import json
import os
import time
from collections import Counter, defaultdict
from contextvars import ContextVar
from datetime import datetime, timedelta

import happybase
from fastapi import FastAPI, Query, WebSocket

app = FastAPI(title="Social Trends API")
HOST = os.getenv("HBASE_HOST", "hbase")
_cache = {"t": 0, "rows": []}
# Optional ?source=<name> on any endpoint filters every aggregate to one source (set per request).
SRC: ContextVar[str] = ContextVar("SRC", default="")


@app.middleware("http")
async def source_filter(request, call_next):
    SRC.set(request.query_params.get("source", ""))
    return await call_next(request)


def conn():
    return happybase.Connection(HOST, port=9090, timeout=20000)


def load_posts():
    rows = _load_all()
    s = SRC.get()
    return [r for r in rows if r["source"] == s] if s else rows


def _load_all():
    if time.time() - _cache["t"] < 3:
        return _cache["rows"]
    c = conn()
    rows = []
    try:
        for _, d in c.table("processed_posts").scan(limit=50000):
            g = lambda k, default=b"": d.get(k, default).decode()
            rows.append({
                "id": g(b"d:id"), "source": g(b"d:source"), "text": g(b"d:text"),
                "created_at": g(b"d:created_at"), "lang": g(b"d:lang"),
                "label": g(b"s:label"), "score": float(g(b"s:score", b"0") or 0),
                "hashtags": json.loads(g(b"n:hashtags", b"[]")),
                "keywords": json.loads(g(b"n:keywords", b"[]")),
                "aspects": json.loads(g(b"n:aspects", b"{}")),
            })
    finally:
        c.close()
    _cache.update(t=time.time(), rows=rows)
    return rows


def summary_of(rows):
    cnt = Counter(r["label"] for r in rows)
    return {"total": len(rows), "positive": cnt["positive"], "negative": cnt["negative"], "neutral": cnt["neutral"]}


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/summary")
def summary():
    return summary_of(load_posts())


@app.get("/api/sentiment")
def sentiment():
    s = summary_of(load_posts())
    return [{"name": k, "value": s[k]} for k in ("positive", "negative", "neutral")]


@app.get("/api/platforms")
def platforms():
    agg = defaultdict(Counter)
    for r in load_posts():
        agg[r["source"]][r["label"]] += 1
    return [{"source": s, "total": sum(c.values()), "positive": c["positive"], "negative": c["negative"],
             "neutral": c["neutral"]} for s, c in sorted(agg.items())]


@app.get("/api/timeseries")
def timeseries():
    agg = defaultdict(Counter)
    for r in load_posts():
        agg[r["created_at"][:10]][r["label"]] += 1
    return [{"day": d, "positive": c["positive"], "negative": c["negative"], "neutral": c["neutral"]}
            for d, c in sorted(agg.items())]


@app.get("/api/hashtags")
def hashtags(limit: int = 10):
    """Trending = recent (last 24h of data) volume vs average daily baseline of the earlier period."""
    rows = load_posts()
    if not rows:
        return []
    latest = max(datetime.fromisoformat(r["created_at"]) for r in rows if r["created_at"])
    cut = latest - timedelta(days=1)
    recent, base = Counter(), Counter()
    for r in rows:
        if not r["created_at"]:
            continue
        bucket = recent if datetime.fromisoformat(r["created_at"]) >= cut else base
        bucket.update(r["hashtags"])
    days = max(1, len({r["created_at"][:10] for r in rows}) - 1)
    out = [{"tag": t, "recent": c, "baseline_per_day": round(base[t] / days, 1),
            "trend_score": round(c / (base[t] / days + 1), 2)} for t, c in recent.items()]
    return sorted(out, key=lambda x: -x["recent"])[:limit]


@app.get("/api/keywords")
def keywords(limit: int = 15):
    c = Counter(k for r in load_posts() for k in r["keywords"])
    return [{"word": w, "count": n} for w, n in c.most_common(limit)]


@app.get("/api/aspects")
def aspects():
    agg = defaultdict(Counter)
    for r in load_posts():
        for a, lab in r["aspects"].items():
            agg[a][lab] += 1
    return [{"aspect": a, "positive": c["positive"], "negative": c["negative"], "neutral": c["neutral"]}
            for a, c in sorted(agg.items())]


@app.get("/api/languages")
def languages():
    c = Counter(r["lang"] for r in load_posts())
    return [{"lang": k, "count": v} for k, v in c.most_common()]


@app.get("/api/search")
def search(q: str = "", source: str = "", sentiment: str = "", limit: int = Query(50, le=200)):
    ql = q.lower()
    hits = [r for r in load_posts()
            if (not ql or ql in r["text"].lower()) and (not source or r["source"] == source)
            and (not sentiment or r["label"] == sentiment)]
    hits.sort(key=lambda r: r["created_at"], reverse=True)
    return {"count": len(hits), "results": hits[:limit]}


@app.get("/api/topics")
def topics():
    """Written by the Spark batch job (LDA)."""
    c = conn()
    try:
        out = []
        for k, d in c.table("topics").scan():
            out.append({"topic": k.decode(), "words": json.loads(d.get(b"t:words", b"[]")),
                        "docs": int(d.get(b"t:count", b"0"))})
        return out
    finally:
        c.close()


@app.get("/api/history")
def history():
    """Daily sentiment aggregates written by the Spark batch job."""
    c = conn()
    try:
        return [{"key": k.decode(), "positive": int(d.get(b"a:pos", 0)), "negative": int(d.get(b"a:neg", 0)),
                 "neutral": int(d.get(b"a:neu", 0))} for k, d in c.table("analytics").scan()]
    finally:
        c.close()


@app.websocket("/ws/live")
async def live(ws: WebSocket):
    await ws.accept()
    SRC.set(ws.query_params.get("source", ""))
    try:
        while True:
            rows = await asyncio.to_thread(load_posts)
            latest = sorted(rows, key=lambda r: r["created_at"], reverse=True)[:8]
            await ws.send_json({"summary": summary_of(rows), "latest": latest})
            await asyncio.sleep(3)
    except Exception:  # noqa: BLE001
        pass
