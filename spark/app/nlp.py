"""Lightweight English NLP: sentiment (VADER), hashtags, keywords, aspect-based sentiment."""
import re
from collections import Counter

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_an = None

STOP = set("""the a an and or but if then than so of to in on at by for with from as is are was were be been being
this that these those it its i you he she we they them his her our your their my me not no yes do does did have has had
will would can could should just very really also about into over under more most some any all out up down how what
when where who why which there here get got like one two new use used using been than because while after before""".split())

ASPECTS = {
    "price": ["price", "cost", "expensive", "cheap", "pricing", "value", "subscription", "fee"],
    "quality": ["quality", "build", "reliable", "reliability", "broken", "durable", "bug", "bugs"],
    "service": ["service", "staff", "customer", "delivery", "shipping", "refund"],
    "performance": ["performance", "fast", "slow", "speed", "lag", "latency", "crash", "crashes"],
    "battery": ["battery", "charge", "charging"],
    "design": ["design", "interface", "ui", "look", "screen", "display"],
    "support": ["support", "help", "documentation", "docs", "community"],
    "privacy": ["privacy", "security", "tracking", "data", "leak"],
}


def analyzer():
    global _an
    if _an is None:
        _an = SentimentIntensityAnalyzer()
    return _an


def label_of(compound):
    if compound >= 0.05:
        return "positive"
    if compound <= -0.05:
        return "negative"
    return "neutral"


def sentiment(text):
    c = analyzer().polarity_scores(text)["compound"]
    return label_of(c), round(c, 4)


def hashtags(text):
    return sorted({h.lower() for h in re.findall(r"#\w+", text)})


def keywords(text, n=5):
    toks = [t for t in re.findall(r"[a-z][a-z\-']{3,}", re.sub(r"#\w+", " ", text.lower())) if t not in STOP]
    return [w for w, _ in Counter(toks).most_common(n)]


def aspects(text):
    """Aspect-based sentiment: score each sentence that mentions an aspect keyword."""
    out = {}
    for sent in re.split(r"[.!?;\n]+", text):
        low = sent.lower()
        if not low.strip():
            continue
        words = set(re.findall(r"[a-z]+", low))
        for asp, keys in ASPECTS.items():
            if words & set(keys):
                out[asp] = label_of(analyzer().polarity_scores(sent)["compound"])
    return out


def language(text):
    try:
        from langdetect import DetectorFactory, detect
        DetectorFactory.seed = 0
        return detect(text)
    except Exception:  # noqa: BLE001
        return "unknown"
