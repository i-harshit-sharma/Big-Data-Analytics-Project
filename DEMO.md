# Demo script (about 5 minutes)

Run from the `social-trends` folder in PowerShell.

## 0. Before the demo
```
docker compose up -d
docker compose ps
```
Wait about 1 minute after start, then confirm data flows: `curl http://localhost:8000/api/summary`

## 1. Show the three-node cluster (30 s)
- Open http://localhost:8080: Spark master with 2 workers ALIVE (spark-worker-1, spark-worker-2) and the running streaming application.
- Say: master + 2 workers = 3 containers on one Docker network; everything is on one laptop.

## 2. Kafka (60 s)
```
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic posts.raw
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic posts.raw --max-messages 3 --from-beginning
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic posts.dlq --from-beginning --timeout-ms 5000
```
- Point out: 3 partitions, JSON events keyed by source, and the invalid messages in `posts.dlq`.

## 3. Spark streaming (60 s)
```
docker compose logs --tail 10 spark-stream
```
- Show lines like `[batch N] wrote 498 records to HBase`.
- Explain: validate, clean, dedupe with watermark, NLP (sentiment, hashtags, keywords, aspects), write to HBase.

## 4. HBase (45 s)
- Open http://localhost:16010 and show tables `raw_posts`, `processed_posts`, `topics`, `analytics`.
- Optional shell check:
```
docker compose exec hbase bash -c "echo \"count 'processed_posts'\" | hbase shell -n"
```

## 5. Batch job (60 s)
Run the batch command from the README, then refresh the dashboard: Topics and Historical analytics fill in.

## 6. Dashboard (90 s)
Open http://localhost:3000 and show, in order: overview counts, sentiment distribution, platform comparison,
sentiment over time, trending hashtags, keyword analysis, aspect-based sentiment, topics, live feed
(new posts appear every few seconds), then search (e.g. "battery" + negative).

## 7. Be upfront
Mention the limitations in README.md: only two real sources (Hacker News, Sentiment140), English only, VADER baseline (0.533 accuracy on Sentiment140), single broker, no performance testing.
