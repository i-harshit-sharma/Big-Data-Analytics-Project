"""Spark BATCH job: reads historical processed posts from HBase, computes daily sentiment aggregates
and LDA topic modelling (Spark MLlib), and writes results back to HBase (analytics, topics)."""
import json

from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession, functions as F

import hb
import nlp

K = 4


def main():
    hb.ensure_tables()
    spark = SparkSession.builder.appName("social-batch").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Load historical records from HBase (thousands of rows -> scanned on the driver, then distributed)
    c = hb.connect(retries=10)
    rows = [(d[b"d:source"].decode(), d[b"d:created_at"].decode(), d[b"s:label"].decode(), d[b"d:clean"].decode())
            for _, d in c.table("processed_posts").scan(
                columns=[b"d:source", b"d:created_at", b"d:clean", b"s:label"], batch_size=200)]
    c.close()
    print("loaded", len(rows), "records", flush=True)
    df = spark.createDataFrame(rows, ["source", "created_at", "label", "clean"]).withColumn("day", F.substring("created_at", 1, 10))

    # 1) time-based aggregates
    agg = df.groupBy("day", "source").pivot("label", ["positive", "negative", "neutral"]).count().na.fill(0).collect()
    c = hb.connect(retries=10)
    b = c.table("analytics").batch()
    for r in agg:
        b.put(f"daily|{r['day']}|{r['source']}".encode(),
              {b"a:pos": str(r["positive"]).encode(), b"a:neg": str(r["negative"]).encode(), b"a:neu": str(r["neutral"]).encode()})
    b.send()
    print("wrote", len(agg), "daily aggregates", flush=True)

    # 2) topic modelling with LDA
    extra = list(nlp.STOP) + ["amazing", "terribly"]
    pipe = Pipeline(stages=[
        RegexTokenizer(inputCol="clean", outputCol="tok", pattern="[^a-zA-Z]+", minTokenLength=4, toLowercase=True),
        StopWordsRemover(inputCol="tok", outputCol="words", stopWords=StopWordsRemover.loadDefaultStopWords("english") + extra),
        CountVectorizer(inputCol="words", outputCol="features", vocabSize=800, minDF=3),
    ])
    model = pipe.fit(df)
    feats = model.transform(df)
    lda = LDA(k=K, maxIter=25, seed=1).fit(feats)
    vocab = model.stages[-1].vocabulary
    topics = lda.describeTopics(6).collect()
    assign = lda.transform(feats).select(F.udf(lambda v: int(max(range(len(v)), key=lambda i: v[i])), "int")("topicDistribution").alias("t"))
    counts = {r["t"]: r["count"] for r in assign.groupBy("t").count().collect()}
    c.close()
    c = hb.connect(retries=10)  # the connection above sat idle during LDA training; open a fresh one
    tb = c.table("topics").batch()
    for t in topics:
        words = [vocab[i] for i in t["termIndices"]]
        tb.put(f"topic_{t['topic']:02d}".encode(), {b"t:words": json.dumps(words).encode(), b"t:count": str(counts.get(t["topic"], 0)).encode()})
        print("topic", t["topic"], words, flush=True)
    tb.send()
    c.close()
    print("batch job done", flush=True)


if __name__ == "__main__":
    main()
