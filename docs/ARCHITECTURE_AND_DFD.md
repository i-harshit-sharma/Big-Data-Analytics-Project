# System Architecture and Data Flow Diagrams (DFD)

This document provides a comprehensive technical reference for the **Social Media Trend and Sentiment Analysis** platform. It details the system architecture, Lambda architecture pattern, container topology, database schema, and Data Flow Diagrams (DFD Level 0, Level 1, and Level 2) reflecting the codebase implementation.

---

## Table of Contents
1. [End-to-End System Architecture Diagram](#1-end-to-end-system-architecture-diagram)
2. [Docker Container & Deployment Topology](#2-docker-container--deployment-topology)
3. [HBase NoSQL Storage Schema](#3-hbase-nosql-storage-schema)
4. [Data Flow Diagrams (DFD)](#4-data-flow-diagrams-dfd)
   - [DFD Level 0: Context Diagram](#dfd-level-0-context-diagram)
   - [DFD Level 1: System Decomposition](#dfd-level-1-system-decomposition)
   - [DFD Level 2: Stream & Batch Processing Pipelines](#dfd-level-2-stream--batch-processing-pipelines)
   - [DFD Level 2.1: Streaming Ingestion & Real-Time NLP Pipeline](#dfd-level-21-streaming-ingestion--real-time-nlp-pipeline)
   - [DFD Level 2.2: Offline Batch Processing & Topic Modeling (LDA)](#dfd-level-22-offline-batch-processing--topic-modeling-lda)
   - [DFD Level 2.3: API Serving & WebSocket Delivery](#dfd-level-23-api-serving--websocket-delivery)
5. [Component Reference & Source Code Map](#5-component-reference--source-code-map)

---
      

## 1. End-to-End System Architecture Diagram

![System Architecture](diagrams/architecture-system.png)

> Direct Vector Image: [architecture-system.svg](diagrams/architecture-system.svg) | High-Resolution Raster: [architecture-system.png](diagrams/architecture-system.png)

<details>
<summary><b>Click to expand Mermaid Source Code</b></summary>

```mermaid
flowchart TD
    subgraph SOURCELAYER ["Data Sources & Ingestion"]
        HN["Hacker News API\n(Firebase REST API)"]
        S140["Sentiment140 Dataset\n(4,000 Sample Tweets)"]
        BS["Bluesky Firehose\n(ATProto Jetstream / Optional)"]
        
        P_LIVE["producer.py\n(Polls Top Stories & Comments)"]
        P_LOAD["load_hackernews.py /\nload_dataset.py"]
    end

    subgraph BROKERLAYER ["Message Broker (Kafka Cluster)"]
        K_RAW[("Kafka Topic: posts.raw\n(3 Partitions, Key=Source)")]
        K_DLQ[("Kafka Topic: posts.dlq\n(1 Partition, Dead-Letter Queue)")]
    end

    subgraph STREAMLAYER ["Speed Layer (Spark Structured Streaming)"]
        SS_IN["Spark Kafka Consumer\n(maxOffsetsPerTrigger=500)"]
        VAL{"Schema Validation\n(id, source, text)"}
        CLEAN["Clean & Normalize\n(Strip URLs, Regex Whitespace)"]
        DEDUPE["Watermark Deduplication\n(event_ts, 7-day window)"]
        
        subgraph EXECUTOR_NLP ["Partition Worker: NLP Extraction"]
            VADER["VADER Sentiment\n(Polarity Compound Score)"]
            ASPECT["Aspect-Based Sentiment\n(8 Lexicon Categories)"]
            KW_HT["Keywords & Hashtags\n(Stopwords & Regex Extractor)"]
            LANG["Language Detection\n(langdetect)"]
        end
    end

    subgraph STORAGELAYER ["Distributed Storage (Apache HBase 2.1)"]
        THRIFT["HBase Thrift Gateway\n(Port 9090 / HappyBase)"]
        T_RAW[("HBase Table: raw_posts\nCF 'r': json, ingested")]
        T_PROC[("HBase Table: processed_posts\nCF 'd': post data\nCF 's': sentiment\nCF 'n': nlp features\nSalt: CRC32 % 8")]
        T_ANALYTICS[("HBase Table: analytics\nCF 'a': daily pos/neg/neu")]
        T_TOPICS[("HBase Table: topics\nCF 't': topic words, doc counts")]
    end

    subgraph BATCHLAYER ["Batch Processing Layer (Spark MLlib)"]
        BATCH_RUN["batch_job.py\n(Triggered Spark Job)"]
        SCAN_HIST["Driver HBase Scan\n(processed_posts)"]
        AGG_TIME["Daily Pivot Aggregation\n(Group by Day + Source)"]
        ML_PIPE["MLlib Text Pipeline\n(RegexTokenizer -> StopWords -> CountVectorizer)"]
        LDA_MODEL["Spark MLlib LDA Model\n(k=4 Topics, 25 Iterations)"]
    end

    subgraph SERVINGLAYER ["Serving & Backend API (FastAPI)"]
        API["FastAPI Backend Server\n(Port 8000)"]
        CACHE["In-Memory Cache\n(TTL: 3 seconds)"]
        WS_SRV["WebSocket Broadcaster\n(/ws/live @ 3s interval)"]
        ENDPOINTS["REST API Endpoints\n(/api/summary, /sentiment, /platforms,\n/timeseries, /hashtags, /keywords, /aspects,\n/languages, /search, /topics, /history)"]
    end

    subgraph UILAYER ["Presentation Layer (React Frontend)"]
        UI["React Dashboard\n(Vite + Recharts)"]
        NGINX["NGINX Web Server\n(Port 3000)"]
        DASH_CHARTS["Dashboard Components:\n- KPI Summary Cards\n- Sentiment Pie & Platforms Bar Chart\n- Daily Sentiment Timeline\n- Trending Hashtags & Top Keywords\n- Aspect Sentiment Stacked Bars\n- Discovered LDA Topics\n- Real-Time Live Feed & Search"]
    end

    %% Ingestion flows
    HN -->|HTTP GET /topstories| P_LIVE
    S140 -->|CSV Replay| P_LOAD
    BS -.->|JSONL Ingest| P_LOAD
    P_LIVE -->|Publish Valid JSON| K_RAW
    P_LIVE -->|Deliberate Malformed Payloads| K_RAW
    P_LOAD -->|Bulk Publish| K_RAW

    %% Streaming flows
    K_RAW -->|Read Stream| SS_IN
    SS_IN --> VAL
    VAL -->|Invalid Records| K_DLQ
    VAL -->|Valid Records| CLEAN
    CLEAN --> DEDUPE
    DEDUPE --> EXECUTOR_NLP
    EXECUTOR_NLP -->|Salted Writes via Thrift| THRIFT
    THRIFT --> T_RAW
    THRIFT --> T_PROC

    %% Batch flows
    T_PROC -.->|Batch Scan| SCAN_HIST
    SCAN_HIST --> BATCH_RUN
    BATCH_RUN --> AGG_TIME
    BATCH_RUN --> ML_PIPE
    ML_PIPE --> LDA_MODEL
    AGG_TIME -->|Write Aggregates| THRIFT
    LDA_MODEL -->|Write Topics & Counts| THRIFT
    THRIFT --> T_ANALYTICS
    THRIFT --> T_TOPICS

    %% Serving flows
    T_PROC -.->|Scan with 3s Cache| CACHE
    T_ANALYTICS -.->|Scan| API
    T_TOPICS -.->|Scan| API
    CACHE --> API
    API --> ENDPOINTS
    API --> WS_SRV

    %% Presentation flows
    ENDPOINTS -->|HTTP GET| UI
    WS_SRV -->|WebSocket Push| UI
    NGINX -->|Hosts SPA Assets| UI
    UI --> DASH_CHARTS
```

</details>

---

## 2. Docker Container & Deployment Topology

![Docker Container Topology](diagrams/deployment-topology.png)

> Direct Vector Image: [deployment-topology.svg](diagrams/deployment-topology.svg) | High-Resolution Raster: [deployment-topology.png](diagrams/deployment-topology.png)

All 9 services execute within a private Docker bridge network (`social-trends_lab`). The multi-node Spark cluster consists of a standalone master and two worker nodes sharing local host resources.

<details>
<summary><b>Click to expand Mermaid Source Code</b></summary>

```mermaid
graph TB
    subgraph HOST ["Local Host / Laptop (Docker Desktop with WSL2)"]
        subgraph DOCKER_NET ["Docker Network: lab (Bridge: social-trends_lab)"]
            
            subgraph KAFKA_CONTAINER ["Kafka Service"]
                C_KAFKA["kafka (apache/kafka:3.8.0)\nKRaft Mode (Node 1)\nPort: 9092 (Internal)"]
            end

            subgraph HBASE_CONTAINER ["HBase Service"]
                C_HBASE["hbase (harisekhon/hbase:2.1)\nStandalone Master & RegionServer\nPorts: 9090 (Thrift), 16010 (Web UI)"]
            end

            subgraph SPARK_CLUSTER ["Distributed Spark Cluster (3.5.3)"]
                C_SPARK_M["spark-master\nMaster Process\nPorts: 7077 (Cluster), 8080 (Web UI)"]
                C_SPARK_W1["spark-worker-1\n2 Cores, 1 GB RAM\nConnects to spark://spark-master:7077"]
                C_SPARK_W2["spark-worker-2\n2 Cores, 1 GB RAM\nConnects to spark://spark-master:7077"]
                C_SPARK_STR["spark-stream\nStreaming Driver Application\nstream_job.py"]
            end

            subgraph APP_CONTAINERS ["Application & Client Services"]
                C_PROD["producer\nPython Kafka Producer\nPolls Hacker News API"]
                C_BACK["backend\nFastAPI Application (Uvicorn)\nPort: 8000"]
                C_FRONT["frontend\nReact SPA + NGINX\nPort: 80 (Container) -> 3000 (Host)"]
            end

            subgraph VOLUMES ["Persistent Volumes"]
                V_KAFKA[("kafka-data\n(/var/lib/kafka/data)")]
                V_HBASE[("hbase-data\n(/hbase-data)")]
                V_CHECK[("checkpoints\n(/checkpoints)")]
            end
        end

        %% Host Port Mappings
        HOST_3000["Host: http://localhost:3000"] -->|HTTP| C_FRONT
        HOST_8000["Host: http://localhost:8000"] -->|REST / WebSocket| C_BACK
        HOST_8080["Host: http://localhost:8080"] -->|Web UI| C_SPARK_M
        HOST_16010["Host: http://localhost:16010"] -->|Web UI| C_HBASE

        %% Inter-container Connections
        C_PROD -->|posts.raw| C_KAFKA
        C_SPARK_STR -->|Consume| C_KAFKA
        C_SPARK_STR -->|Schedule Tasks| C_SPARK_M
        C_SPARK_M --> C_SPARK_W1
        C_SPARK_M --> C_SPARK_W2
        C_SPARK_W1 -->|Thrift Puts| C_HBASE
        C_SPARK_W2 -->|Thrift Puts| C_HBASE
        C_BACK -->|Thrift Scans (9090)| C_HBASE
        C_FRONT -->|API Proxy / Direct Calls| C_BACK

        %% Volume Attachments
        C_KAFKA --- V_KAFKA
        C_HBASE --- V_HBASE
        C_SPARK_STR --- V_CHECK
    end
```

</details>

---

## 3. HBase NoSQL Storage Schema

![HBase NoSQL Storage Schema](diagrams/hbase-nosql-schema.png)

> Direct Vector Image: [hbase-nosql-schema.svg](diagrams/hbase-nosql-schema.svg) | High-Resolution Raster: [hbase-nosql-schema.png](diagrams/hbase-nosql-schema.png)

HBase acts as the central NoSQL datastore for both streaming writes and batch analytical workloads. Salted keys prevent write hotspots across HBase region boundaries.

<details>
<summary><b>Click to expand Mermaid Source Code</b></summary>

```mermaid
classDiagram
    class raw_posts {
        +RowKey: [salt:02d]|[source]|[id]
        +Family r: json (Raw JSON string)
        +Family r: ingested (UTC timestamp)
    }

    class processed_posts {
        +RowKey: [salt:02d]|[source]|[id]
        +Family d: id (Post ID)
        +Family d: source (hackernews, sentiment140, etc.)
        +Family d: author (Author handle)
        +Family d: text (Original content)
        +Family d: clean (URL-stripped & trimmed text)
        +Family d: created_at (ISO timestamp)
        +Family d: ingested_at (ISO timestamp)
        +Family d: lang (Detected ISO language code)
        +Family s: label (positive | negative | neutral)
        +Family s: score (VADER compound float [-1.0, 1.0])
        +Family n: hashtags (JSON array of tags)
        +Family n: keywords (JSON array of top-5 keywords)
        +Family n: aspects (JSON object of aspect sentiments)
    }

    class analytics {
        +RowKey: daily|[YYYY-MM-DD]|[source]
        +Family a: pos (Count of positive posts)
        +Family a: neg (Count of negative posts)
        +Family a: neu (Count of neutral posts)
    }

    class topics {
        +RowKey: topic_[NN]
        +Family t: words (JSON array of top-6 keywords)
        +Family t: count (Document assignment count)
    }

    raw_posts <.. processed_posts : Written concurrently
    processed_posts --> analytics : Aggregated by batch_job.py
    processed_posts --> topics : Modeled by MLlib LDA
```

</details>

### Key Salting Formula
$$\text{salt} = \text{CRC32}(\text{source} \parallel "|" \parallel \text{post\_id}) \pmod 8$$
Row keys are formatted as `"{salt:02d}|{source}|{post_id}"` (e.g. `03|hackernews|hn-38491204`). This uniformly balances write traffic across 8 virtual key buckets.

---

## 4. Data Flow Diagrams (DFD)

### DFD Level 0: Context Diagram

![DFD Level 0 Context Diagram](diagrams/dfd-level-0.png)

> Direct Vector Image: [dfd-level-0.svg](diagrams/dfd-level-0.svg) | High-Resolution Raster: [dfd-level-0.png](diagrams/dfd-level-0.png)

The Level 0 Context Diagram establishes the system boundary, showing all external actors and high-level inbound/outbound data flows.

<details>
<summary><b>Click to expand Mermaid Source Code</b></summary>

```mermaid
flowchart LR
    E_HN["External Entity:\nHacker News API"]
    E_S140["External Entity:\nSentiment140 Data File"]
    E_BS["External Entity:\nBluesky ATProto / Network"]
    
    SYS(("0.0\nSocial Media Trend\n& Sentiment Analytics\nPlatform"))
    
    E_USER["External Entity:\nData Analyst / End User"]

    E_HN -->|"1. Live story items & comments JSON"| SYS
    E_S140 -->|"2. Historical tweet CSV records"| SYS
    E_BS -.->|"3. Firehose event JSONL"| SYS

    E_USER -->|"4. Filter by Source (?source=), Search Query (q), Time Range"| SYS
    SYS -->|"5. Real-time Dashboard Visualizations & WebSocket Live Stream"| E_USER
    SYS -->|"6. Trending Hashtags, LDA Topics & Sentiment Analytics"| E_USER
```

</details>

---

### DFD Level 1: System Decomposition

![DFD Level 1 System Decomposition](diagrams/dfd-level-1.png)

> Direct Vector Image: [dfd-level-1.svg](diagrams/dfd-level-1.svg) | High-Resolution Raster: [dfd-level-1.png](diagrams/dfd-level-1.png)

The Level 1 DFD decomposes the system into six primary functional processes and five physical data stores.

<details>
<summary><b>Click to expand Mermaid Source Code</b></summary>

```mermaid
flowchart TD
    E1["External Source: Hacker News API"]
    E2["External Dataset: Sentiment140 CSV"]
    USER["User / Analyst Browser"]

    P1["1.0 Data Ingestion and Messaging\n(producer.py / loaders)"]
    P2["2.0 Stream Validation and Filtering\n(stream_job.py)"]
    P3["3.0 Real-Time NLP Enrichment\n(nlp.py)"]
    P4["4.0 Storage Management and Salting\n(hb.py / Thrift)"]
    P5["5.0 Offline Batch Analytics\n(batch_job.py)"]
    P6["6.0 API Serving and Web Delivery\n(main.py / React)"]

    D1[("D1: Kafka (posts.raw / posts.dlq)")]
    D2[("D2: HBase raw_posts")]
    D3[("D3: HBase processed_posts")]
    D4[("D4: HBase analytics")]
    D5[("D5: HBase topics")]

    E1 -->|"Raw stories and comments"| P1
    E2 -->|"Sampled tweet records"| P1
    P1 -->|"Keyed JSON events"| D1

    D1 -->|"Micro-batch posts"| P2
    P2 -->|"Invalid records to DLQ"| D1
    P2 -->|"Cleaned stream"| P3

    P3 -->|"NLP enriched attributes"| P4
    P4 -->|"Raw payload"| D2
    P4 -->|"Salted features"| D3

    D3 -.->|"Historical batch scan"| P5
    P5 -->|"Daily sentiment aggregates"| D4
    P5 -->|"LDA topic clusters"| D5

    D3 -.->|"Recent scan with 3s cache"| P6
    D4 -.->|"Historical trends"| P6
    D5 -.->|"Topic distribution"| P6
    USER -->|"REST queries and search"| P6
    P6 -->|"Dashboard metrics and live feed"| USER
```

</details>

---

### DFD Level 2: Stream & Batch Processing Pipelines

![DFD Level 2 Pipelines](diagrams/dfd-level-2.png)

> Direct Vector Image: [dfd-level-2.svg](diagrams/dfd-level-2.svg) | High-Resolution Raster: [dfd-level-2.png](diagrams/dfd-level-2.png)

---

### DFD Level 2.1: Streaming Ingestion & Real-Time NLP Pipeline

![DFD Level 2.1](diagrams/dfd-level-2-1.png)

> Direct Vector Image: [dfd-level-2-1.svg](diagrams/dfd-level-2-1.svg) | High-Resolution Raster: [dfd-level-2-1.png](diagrams/dfd-level-2-1.png)

Decomposition of **Process 2.0 (Stream Validation & Filtering)** and **Process 3.0 (Real-Time NLP Enrichment)**.

<details>
<summary><b>Click to expand Mermaid Source Code</b></summary>

```mermaid
flowchart TD
    D1_IN[("D1: Kafka (posts.raw)")]
    
    P2_1["2.1 Parse JSON Schema\n(id, source, author, text, created_at, url)"]
    P2_2{"2.2 Validate Fields\n(id != null and text != '')"}
    P2_3["2.3 Normalise and Clean Text\n(Strip URLs and normalize whitespace)"]
    P2_4["2.4 Watermarked Deduplication\n(7-day watermark on event_ts, dropDuplicates)"]
    
    D1_DLQ[("D1: Kafka (posts.dlq)\nDead-Letter Queue")]

    P3_1["3.1 VADER Sentiment Analyzer\n(Compound score -> positive / negative / neutral)"]
    P3_2["3.2 Aspect Sentiment Mining\n(Sentence matching on 8 aspect lexicons)"]
    P3_3["3.3 Keyword and Hashtag Extractor\n(Stopwords filter and regex #hashtag extraction)"]
    P3_4["3.4 Language Detector\n(langdetect seed=0)"]
    
    P4_1["4.1 Salt Key Generator\n(crc32(source|id) % 8)"]
    P4_2["4.2 HBase Batch Writer\n(happybase batch size=200)"]

    D2_OUT[("D2: HBase raw_posts")]
    D3_OUT[("D3: HBase processed_posts")]

    D1_IN -->|"Stream read"| P2_1
    P2_1 --> P2_2
    P2_2 -->|"Invalid record"| D1_DLQ
    P2_2 -->|"Valid record"| P2_3
    P2_3 --> P2_4
    P2_4 --> P3_1
    P2_4 --> P3_2
    P2_4 --> P3_3
    P2_4 --> P3_4

    P3_1 --> P4_1
    P3_2 --> P4_1
    P3_3 --> P4_1
    P3_4 --> P4_1
    P4_1 --> P4_2
    P4_2 -->|"Put r:json, r:ingested"| D2_OUT
    P4_2 -->|"Put d:*, s:*, n:*"| D3_OUT
```

</details>

---

### DFD Level 2.2: Offline Batch Processing & Topic Modeling (LDA)

![DFD Level 2.2](diagrams/dfd-level-2-2.png)

> Direct Vector Image: [dfd-level-2-2.svg](diagrams/dfd-level-2-2.svg) | High-Resolution Raster: [dfd-level-2-2.png](diagrams/dfd-level-2-2.png)

Decomposition of **Process 5.0 (Offline Batch Processing & Topic Modeling)**.

<details>
<summary><b>Click to expand Mermaid Source Code</b></summary>

```mermaid
flowchart TD
    D3_IN[("D3: HBase processed_posts")]

    P5_1["5.1 Scan Historical Records\n(Fetch source, created_at, label, clean)"]
    P5_2["5.2 Time-Series Aggregator\n(Pivot label counts by day and source)"]
    
    subgraph SPARK_ML ["Spark MLlib Pipeline"]
        P5_3["5.3 RegexTokenizer\n(pattern='[^a-zA-Z]+', minTokenLength=4)"]
        P5_4["5.4 StopWordsRemover\n(Default English + Custom Domain Stopwords)"]
        P5_5["5.5 CountVectorizer\n(vocabSize=800, minDF=3)"]
        P5_6["5.6 Latent Dirichlet Allocation\n(k=4, maxIter=25, seed=1)"]
    end

    P5_7["5.7 Extract Topic Vocabularies\n(Top 6 representative terms per topic)"]
    P5_8["5.8 Document Topic Assignment\n(argmax topicDistribution count)"]

    P5_9["5.9 Thrift Write to analytics\n(Row: daily|day|source)"]
    P5_10["5.10 Thrift Write to topics\n(Row: topic_NN)"]

    D4_OUT[("D4: HBase analytics")]
    D5_OUT[("D5: HBase topics")]

    D3_IN -->|"Thrift Table Scan"| P5_1
    P5_1 --> P5_2
    P5_1 --> P5_3
    P5_3 --> P5_4
    P5_4 --> P5_5
    P5_5 --> P5_6
    P5_6 --> P5_7
    P5_6 --> P5_8

    P5_2 --> P5_9
    P5_9 --> D4_OUT

    P5_7 --> P5_10
    P5_8 --> P5_10
    P5_10 --> D5_OUT
```

</details>

---

### DFD Level 2.3: API Serving & WebSocket Delivery

![DFD Level 2.3](diagrams/dfd-level-2-3.png)

> Direct Vector Image: [dfd-level-2-3.svg](diagrams/dfd-level-2-3.svg) | High-Resolution Raster: [dfd-level-2-3.png](diagrams/dfd-level-2-3.png)

Decomposition of **Process 6.0 (API Serving & Web Delivery)**.

<details>
<summary><b>Click to expand Mermaid Source Code</b></summary>

```mermaid
flowchart TD
    D3_HBASE[("D3: HBase processed_posts")]
    D4_HBASE[("D4: HBase analytics")]
    D5_HBASE[("D5: HBase topics")]

    subgraph BACKEND ["FastAPI Service (backend/main.py)"]
        P6_1{"6.1 Check Memory Cache\n(TTL <= 3 seconds)"}
        P6_2["6.2 Scan HBase processed_posts\n(Scan limit=50,000, Thrift 9090)"]
        P6_3[("In-Memory Cache Dict\n(Timestamp and Deserialized Rows)")]
        P6_4["6.4 Apply Request Filter\n(ContextVar SRC from ?source=)"]
        P6_5["6.5 Compute Aggregates and Metrics\n(Summary, Timeseries, Hashtags, Keywords, Aspects, Langs)"]
        P6_6["6.6 Compute Trending Hashtags\n(Recent 24h volume vs baseline ratio)"]
        P6_7["6.7 Real-Time Push Loop\n(WebSocket /ws/live every 3s)"]
    end

    CLIENT["React Client Dashboard\n(App.jsx)"]

    CLIENT -->|"GET /api/* ?source=..."| P6_4
    CLIENT -->|"WS Connect /ws/live?source=..."| P6_7

    P6_4 --> P6_1
    P6_1 -->|"Cache Miss (> 3s)"| P6_2
    D3_HBASE -->|"Thrift Scan"| P6_2
    P6_2 -->|"Update cache"| P6_3
    P6_1 -->|"Cache Hit (<= 3s)"| P6_3
    P6_3 --> P6_5
    P6_3 --> P6_6
    P6_3 --> P6_7

    D4_HBASE -->|"GET /api/history"| CLIENT
    D5_HBASE -->|"GET /api/topics"| CLIENT
    P6_5 -->|"JSON Responses"| CLIENT
    P6_6 -->|"JSON Responses"| CLIENT
    P6_7 -->|"Live JSON Feed"| CLIENT
```

</details>

---

## 5. Component Reference & Source Code Map

| Layer | Component | File Path | Responsibilities & Technologies |
|---|---|---|---|
| **Ingestion** | Live Producer | [producer.py](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/producer/producer.py) | Polls Hacker News Firebase API; injects DLQ test records; publishes to Kafka. |
| **Ingestion** | Dataset Loaders | [load_dataset.py](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/dataset/load_dataset.py), [load_hackernews.py](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/dataset/load_hackernews.py) | Ingests Sentiment140 CSV and bulk HN archives into `posts.raw`. |
| **Messaging** | Kafka Broker | [docker-compose.yml](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/docker-compose.yml#L13-L30) | KRaft broker; manages partitioned `posts.raw` and `posts.dlq` topics. |
| **Speed** | Streaming Driver | [stream_job.py](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/spark/app/stream_job.py) | Structured Streaming, schema verification, 7-day watermark, HBase partition writes. |
| **Speed** | NLP Utilities | [nlp.py](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/spark/app/nlp.py) | VADER sentiment scoring, aspect extraction, hashtag/keyword parsing, `langdetect`. |
| **Storage** | HBase Helpers | [hb.py](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/spark/app/hb.py) | HappyBase connection management, table initialization, CRC32 rowkey salting. |
| **Batch** | Batch Aggregator | [batch_job.py](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/spark/app/batch_job.py) | Historical HBase scan, daily sentiment pivot aggregation, MLlib LDA topic clustering. |
| **Serving** | FastAPI Server | [main.py](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/backend/main.py) | 3s caching layer, context-based filtering, REST endpoints, WebSocket `/ws/live`. |
| **UI** | React Dashboard | [App.jsx](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/frontend/src/App.jsx) | Recharts data visualizations, platform filtering, search, and live event display. |
| **Evaluation**| Benchmark Script| [evaluate.py](file:///c:/Users/abhis/Downloads/bda/Big-Data-Analytics-Project/dataset/evaluate.py) | Validates VADER accuracy and precision/recall against Sentiment140 gold labels. |

