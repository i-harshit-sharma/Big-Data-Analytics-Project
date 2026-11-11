"""Spark Structured Streaming: Kafka (posts.raw) -> validate/dedupe/clean -> NLP -> HBase.
Invalid records go to Kafka topic posts.dlq."""
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, types as T

import hb

KAFKA = "kafka:9092"

SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("author", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("created_at", T.StringType()),
    T.StructField("url", T.StringType()),
])


def write_partition(rows):
    """Runs on a Spark executor: NLP + HBase writes for one partition."""
    import hb as _hb
    import nlp
    conn = _hb.connect(retries=5)
    raw = conn.table("raw_posts").batch(batch_size=200)
    proc = conn.table("processed_posts").batch(batch_size=200)
    now = datetime.now(timezone.utc).isoformat().encode()
    for r in rows:
        key = _hb.rowkey(r["source"], r["id"]).encode()
        raw.put(key, {b"r:json": r["json"].encode(), b"r:ingested": now})
        clean = r["clean"]
        label, score = nlp.sentiment(clean)
        proc.put(key, {
            b"d:id": r["id"].encode(),
            b"d:source": r["source"].encode(),
            b"d:author": (r["author"] or "").encode(),
            b"d:text": r["text"].encode(),
            b"d:clean": clean.encode(),
            b"d:created_at": r["event_time"].encode(),
            b"d:ingested_at": now,
            b"d:lang": nlp.language(clean).encode(),
            b"s:label": label.encode(),
            b"s:score": str(score).encode(),
            b"n:hashtags": json.dumps(nlp.hashtags(r["text"])).encode(),
            b"n:keywords": json.dumps(nlp.keywords(clean)).encode(),
            b"n:aspects": json.dumps(nlp.aspects(clean)).encode(),
        })
    raw.send()
    proc.send()
    conn.close()


def process_batch(df, batch_id):
    n = df.count()
    df.foreachPartition(write_partition)
    print(f"[batch {batch_id}] wrote {n} records to HBase", flush=True)


def main():
    hb.ensure_tables()
    spark = SparkSession.builder.appName("social-sentiment-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", "posts.raw")
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 500)
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw.select(F.col("value").cast("string").alias("json"))
              .withColumn("p", F.from_json("json", SCHEMA)))
    is_valid = (F.col("p.id").isNotNull() & F.col("p.source").isNotNull()
                & F.col("p.text").isNotNull() & (F.length(F.trim("p.text")) > 0))

    # Normalise + clean (Spark SQL functions)
    valid = (parsed.filter(is_valid)
             .select("json", "p.id", "p.source", "p.author", "p.text",
                     F.coalesce(F.to_timestamp("p.created_at"), F.current_timestamp()).alias("event_ts"))
             .withColumn("event_time", F.date_format("event_ts", "yyyy-MM-dd'T'HH:mm:ss"))
             .withColumn("clean", F.trim(F.regexp_replace(
                 F.regexp_replace("text", r"https?://\S+", ""), r"\s+", " ")))
             # Deduplicate with a watermark so state stays bounded; late events > 7 days are dropped from state.
             .withWatermark("event_ts", "7 days")
             .dropDuplicates(["id", "event_ts"]))

    q1 = (valid.writeStream.foreachBatch(process_batch)
          .option("checkpointLocation", "/checkpoints/valid")
          .trigger(processingTime="5 seconds").start())

    q2 = (parsed.filter(~is_valid).select(F.col("json").alias("value"))
          .writeStream.format("kafka")
          .option("kafka.bootstrap.servers", KAFKA).option("topic", "posts.dlq")
          .option("checkpointLocation", "/checkpoints/dlq").start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
