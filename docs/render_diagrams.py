"""
Renders Mermaid diagrams into high-resolution PNG and SVG image files using Chrome and PIL.
"""
import http.server
import os
import re
import socketserver
import subprocess
import threading
import time
from PIL import Image

DIAGRAMS_DIR = os.path.abspath("docs/diagrams")
DOCS_DIR = os.path.abspath("docs")
os.makedirs(DIAGRAMS_DIR, exist_ok=True)

CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
PORT = 9876

# Diagrams definition
DIAGRAMS = {
    "architecture-system": {
        "title": "System Architecture (Lambda Pattern)",
        "width": 1400,
        "height": 1100,
        "code": """flowchart TD
    subgraph INGESTION ["1. Ingestion Layer"]
        HN["Hacker News API\\n(Live Producer: 60s poll)"]
        S140["Sentiment140 Dataset\\n(CSV Sample Loader)"]
        BS["Bluesky Firehose\\n(ATProto Jetstream / Optional)"]
        KAFKA[("Apache Kafka 3.8\\nTopic: posts.raw (3 partitions)\\nTopic: posts.dlq (1 partition)")]
        HN -->|HTTP GET /topstories| KAFKA
        S140 -->|Replay 4,000 tweets| KAFKA
        BS -.->|JSONL Stream| KAFKA
    end

    subgraph SPEED ["2. Speed Layer (Spark Structured Streaming)"]
        SS["Spark Streaming Engine (5s trigger)\\n- Schema Validation (id, source, text)\\n- Dead Letter Queue routing (posts.dlq)\\n- Text Cleansing and URL Stripping\\n- 7-day Watermark Deduplication"]
        NLP["Partition NLP Workers\\n- VADER Sentiment Polarity Scoring\\n- Aspect Mining (8 Lexicons)\\n- Keyword and Hashtag Extractor\\n- Language Detection (langdetect)"]
        KAFKA -->|Consume micro-batch| SS
        SS --> NLP
    end

    subgraph STORAGE ["3. Storage Layer (Apache HBase 2.1)"]
        THRIFT["HBase Thrift Gateway (Port 9090)"]
        T_RAW[("HBase: raw_posts\\ncf 'r': json, ingested")]
        T_PROC[("HBase: processed_posts\\ncf 'd': data | cf 's': sentiment\\ncf 'n': nlp features\\nSalt: CRC32 % 8")]
        T_ANALYTICS[("HBase: analytics\\ncf 'a': pos, neg, neu")]
        T_TOPICS[("HBase: topics\\ncf 't': words, doc counts")]
        NLP -->|Salted batch writes| THRIFT
        THRIFT --> T_RAW
        THRIFT --> T_PROC
    end

    subgraph BATCH ["4. Batch Layer (Spark MLlib)"]
        BATCH_JOB["Spark Batch Analytical Job\\n- Daily Sentiment Pivot Aggregation\\n- Text NLP Pipeline (Tokenizer, Stopwords, Vectorizer)\\n- Spark MLlib LDA Topic Modeling (k=4)"]
        T_PROC -.->|Historical Table Scan| BATCH_JOB
        BATCH_JOB -->|Daily Aggregates| THRIFT
        BATCH_JOB -->|Topic Words and Weights| THRIFT
        THRIFT --> T_ANALYTICS
        THRIFT --> T_TOPICS
    end

    subgraph SERVING ["5. Serving and API Layer (FastAPI)"]
        API["FastAPI Backend (Port 8000)\\n- In-Memory Cache (3s TTL)\\n- ContextVar source filter (?source=)\\n- REST Endpoints (/api/summary, /timeseries, etc.)\\n- WebSocket Stream (/ws/live @ 3s interval)"]
        T_PROC -.->|Cached Reads| API
        T_ANALYTICS -.->|Batch History| API
        T_TOPICS -.->|LDA Topics| API
    end

    subgraph PRESENTATION ["6. Presentation Layer (React and NGINX)"]
        DASH["React 18 SPA (Port 3000)\\n- KPI Metric Cards\\n- Recharts Pie, Bar, and Line Charts\\n- Trending Hashtags and Top Keywords\\n- Aspect Sentiment Stacked Distribution\\n- Real-time Feed and Interactive Post Search"]
        API -->|JSON REST and WebSocket| DASH
    end
"""
    },
    "dfd-level-0": {
        "title": "DFD Level 0: Context Diagram",
        "width": 1100,
        "height": 550,
        "code": """flowchart LR
    E_HN["External Entity:\\nHacker News API"]
    E_S140["External Entity:\\nSentiment140 File"]
    E_BS["External Entity:\\nBluesky Network"]
    
    SYS(("0.0\\nSocial Media Trend\\nand Sentiment Analytics\\nPlatform"))
    
    E_USER["External Entity:\\nData Analyst / User"]

    E_HN -->|"1. Live story items and comments JSON"| SYS
    E_S140 -->|"2. Historical tweet CSV records"| SYS
    E_BS -.->|"3. Firehose event JSONL"| SYS

    E_USER -->|"4. Filter by Source (?source=), Search Query (q)"| SYS
    SYS -->|"5. Real-time Dashboard Visualizations and WebSocket Live Stream"| E_USER
    SYS -->|"6. Trending Hashtags, LDA Topics and Sentiment Analytics"| E_USER
"""
    },
    "dfd-level-1": {
        "title": "DFD Level 1: System Decomposition",
        "width": 1300,
        "height": 950,
        "code": """flowchart TD
    E1["External Source: Hacker News API"]
    E2["External Dataset: Sentiment140 CSV"]
    USER["User / Analyst Browser"]

    P1["1.0 Data Ingestion and Messaging\\n(producer.py / loaders)"]
    P2["2.0 Stream Validation and Filtering\\n(stream_job.py)"]
    P3["3.0 Real-Time NLP Enrichment\\n(nlp.py)"]
    P4["4.0 Storage Management and Salting\\n(hb.py / Thrift)"]
    P5["5.0 Offline Batch Analytics\\n(batch_job.py)"]
    P6["6.0 API Serving and Web Delivery\\n(main.py / React)"]

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
"""
    },
    "dfd-level-2": {
        "title": "DFD Level 2: Stream & Batch Processing Pipelines",
        "width": 1400,
        "height": 1150,
        "code": """flowchart TD
    subgraph STREAMING_PIPELINE ["2.0 and 3.0 Real-Time Streaming Pipeline"]
        D1_RAW[("D1: Kafka (posts.raw)")]
        P2_1("2.1 JSON Schema Parsing\\n(id, source, text, created_at)")
        P2_2{"2.2 Schema Validation\\n(id != null and text != '')"}
        D1_DLQ[("D1: Kafka (posts.dlq)\\nDead-Letter Queue")]
        P2_3("2.3 Text Cleansing\\n(Strip URLs and normalize spaces)")
        P2_4("2.4 Watermark Deduplication\\n(7-day window on event_ts)")
        
        P3_1("3.1 VADER Sentiment\\n(Polarity score -> pos/neg/neu)")
        P3_2("3.2 Aspect Extraction\\n(8 Keyword Lexicons)")
        P3_3("3.3 Keywords and Hashtags\\n(Stopwords Counter and Regex)")
        P3_4("3.4 Language Detector\\n(langdetect)")
        
        P4_1("4.1 CRC32 Hash Salter\\n(00..07 prefix)")
        P4_2("4.2 HBase Batch Writer\\n(HappyBase batch 200)")

        D1_RAW --> P2_1
        P2_1 --> P2_2
        P2_2 -->|Invalid| D1_DLQ
        P2_2 -->|Valid| P2_3
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
    end

    subgraph STORAGE_PIPELINE ["Storage Tables"]
        D2_RAW[("D2: HBase raw_posts")]
        D3_PROC[("D3: HBase processed_posts")]
        D4_ANALYTICS[("D4: HBase analytics")]
        D5_TOPICS[("D5: HBase topics")]
        P4_2 --> D2_RAW
        P4_2 --> D3_PROC
    end

    subgraph BATCH_PIPELINE ["5.0 Offline Batch Analytics Pipeline"]
        P5_1("5.1 Historical Records Scan\\n(processed_posts via Thrift)")
        P5_2("5.2 Daily Sentiment Aggregator\\n(Pivot pos/neg/neu by day)")
        P5_3("5.3 MLlib Text Pipeline\\n(RegexTokenizer -> StopWords -> CountVectorizer)")
        P5_4("5.4 LDA Topic Model\\n(Spark MLlib k=4, maxIter=25)")
        P5_5("5.5 Write Analytics and Topics")

        D3_PROC -.->|Batch Scan| P5_1
        P5_1 --> P5_2
        P5_1 --> P5_3
        P5_3 --> P5_4
        P5_2 --> P5_5
        P5_4 --> P5_5
        P5_5 --> D4_ANALYTICS
        P5_5 --> D5_TOPICS
    end
"""
    },
    "dfd-level-2-1": {
        "title": "DFD Level 2.1: Streaming Ingestion & Real-Time NLP Pipeline",
        "width": 1300,
        "height": 950,
        "code": """flowchart TD
    D1_IN[("D1: Kafka (posts.raw)")]
    
    P2_1["2.1 Parse JSON Schema\\n(id, source, author, text, created_at, url)"]
    P2_2{"2.2 Validate Fields\\n(id != null and text != '')"}
    P2_3["2.3 Normalise and Clean Text\\n(Strip URLs and normalize whitespace)"]
    P2_4["2.4 Watermarked Deduplication\\n(7-day watermark on event_ts, dropDuplicates)"]
    
    D1_DLQ[("D1: Kafka (posts.dlq)\\nDead-Letter Queue")]

    P3_1["3.1 VADER Sentiment Analyzer\\n(Compound score -> positive / negative / neutral)"]
    P3_2["3.2 Aspect Sentiment Mining\\n(Sentence matching on 8 aspect lexicons)"]
    P3_3["3.3 Keyword and Hashtag Extractor\\n(Stopwords filter and regex #hashtag extraction)"]
    P3_4["3.4 Language Detector\\n(langdetect seed=0)"]
    
    P4_1["4.1 Salt Key Generator\\n(crc32(source|id) % 8)"]
    P4_2["4.2 HBase Batch Writer\\n(happybase batch size=200)"]

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
"""
    },
    "dfd-level-2-2": {
        "title": "DFD Level 2.2: Offline Batch Processing & Topic Modeling (LDA)",
        "width": 1200,
        "height": 950,
        "code": """flowchart TD
    D3_IN[("D3: HBase processed_posts")]

    P5_1["5.1 Scan Historical Records\\n(Fetch source, created_at, label, clean)"]
    P5_2["5.2 Time-Series Aggregator\\n(Pivot label counts by day and source)"]
    
    subgraph SPARK_ML ["Spark MLlib Pipeline"]
        P5_3["5.3 RegexTokenizer\\n(pattern='[^a-zA-Z]+', minTokenLength=4)"]
        P5_4["5.4 StopWordsRemover\\n(Default English + Custom Domain Stopwords)"]
        P5_5["5.5 CountVectorizer\\n(vocabSize=800, minDF=3)"]
        P5_6["5.6 Latent Dirichlet Allocation\\n(k=4, maxIter=25, seed=1)"]
    end

    P5_7["5.7 Extract Topic Vocabularies\\n(Top 6 representative terms per topic)"]
    P5_8["5.8 Document Topic Assignment\\n(argmax topicDistribution count)"]

    P5_9["5.9 Thrift Write to analytics\\n(Row: daily|day|source)"]
    P5_10["5.10 Thrift Write to topics\\n(Row: topic_NN)"]

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
"""
    },
    "dfd-level-2-3": {
        "title": "DFD Level 2.3: API Serving & WebSocket Delivery",
        "width": 1250,
        "height": 950,
        "code": """flowchart TD
    D3_HBASE[("D3: HBase processed_posts")]
    D4_HBASE[("D4: HBase analytics")]
    D5_HBASE[("D5: HBase topics")]

    subgraph BACKEND ["FastAPI Service (backend/main.py)"]
        P6_1{"6.1 Check Memory Cache\\n(TTL <= 3 seconds)"}
        P6_2["6.2 Scan HBase processed_posts\\n(Scan limit=50,000, Thrift 9090)"]
        P6_3[("In-Memory Cache Dict\\n(Timestamp and Deserialized Rows)")]
        P6_4["6.4 Apply Request Filter\\n(ContextVar SRC from ?source=)"]
        P6_5["6.5 Compute Aggregates and Metrics\\n(Summary, Timeseries, Hashtags, Keywords, Aspects, Langs)"]
        P6_6["6.6 Compute Trending Hashtags\\n(Recent 24h volume vs baseline ratio)"]
        P6_7["6.7 Real-Time Push Loop\\n(WebSocket /ws/live every 3s)"]
    end

    CLIENT["React Client Dashboard\\n(App.jsx)"]

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
"""
    },
    "deployment-topology": {
        "title": "Docker Container & Deployment Topology",
        "width": 1250,
        "height": 900,
        "code": """flowchart TB
    subgraph DOCKER_HOST ["Docker Host Machine (Docker Desktop / WSL2)"]
        subgraph NET ["Bridge Network: social-trends_lab"]
            
            subgraph KAFKA_SVC ["Messaging"]
                KAFKA["kafka (apache/kafka:3.8.0)\\nKRaft Controller and Broker\\nPort: 9092"]
            end

            subgraph HBASE_SVC ["Storage"]
                HBASE["hbase (harisekhon/hbase:2.1)\\nMaster and RegionServer\\nPorts: 9090 (Thrift), 16010 (Web UI)"]
            end

            subgraph SPARK_CLUSTER ["Distributed Spark Cluster 3.5.3"]
                MASTER["spark-master\\nCluster Coordinator\\nPorts: 7077, 8080 (Web UI)"]
                W1["spark-worker-1\\n2 Cores, 1 GB RAM"]
                W2["spark-worker-2\\n2 Cores, 1 GB RAM"]
                STR["spark-stream\\nDriver: stream_job.py"]
            end

            subgraph APP_SVCS ["Application Services"]
                PROD["producer\\nPython HN Poller"]
                BACK["backend\\nFastAPI (Uvicorn)\\nPort: 8000"]
                FRONT["frontend\\nReact SPA and NGINX\\nPort: 80 -> Host 3000"]
            end

            subgraph VOLUMES ["Docker Volumes"]
                V1[("kafka-data")]
                V2[("hbase-data")]
                V3[("checkpoints")]
            end
        end

        HOST_UI["Browser Dashboard\\nhttp://localhost:3000"] --> FRONT
        HOST_API["API and Docs\\nhttp://localhost:8000/docs"] --> BACK
        HOST_SPARK["Spark Master UI\\nhttp://localhost:8080"] --> MASTER
        HOST_HBASE["HBase Master UI\\nhttp://localhost:16010"] --> HBASE

        PROD --> KAFKA
        STR --> KAFKA
        STR --> MASTER
        MASTER --> W1
        MASTER --> W2
        W1 --> HBASE
        W2 --> HBASE
        BACK --> HBASE
        FRONT --> BACK

        KAFKA --- V1
        HBASE --- V2
        STR --- V3
    end
"""
    },
    "hbase-nosql-schema": {
        "title": "HBase NoSQL Storage Schema & Data Model",
        "width": 1400,
        "height": 1100,
        "code": """flowchart TD
    subgraph SALTING ["Salting Hash Strategy"]
        SALT_RULE["Rowkey Salting Formula:\\nsalt = CRC32(source | post_id) % 8 (Buckets: 00..07)\\nPrevents region hotspotting on high-velocity writes"]
    end

    subgraph RAW_TABLE ["HBase Table: raw_posts"]
        R_KEY["RowKey: [salt:02d]|[source]|[post_id]"]
        R_CF["Column Family 'r':\\n• r:json (Original raw JSON payload)\\n• r:ingested (UTC ingestion timestamp)"]
        R_WRITER["Writer: Spark Streaming (stream_job.py)"]
        R_KEY --- R_CF --- R_WRITER
    end

    subgraph PROC_TABLE ["HBase Table: processed_posts"]
        P_KEY["RowKey: [salt:02d]|[source]|[post_id]"]
        P_CF_D["Column Family 'd' (Data):\\n• d:id (Unique ID), d:source (Platform name)\\n• d:author (Handle), d:text (Original text)\\n• d:clean (Cleaned text), d:lang (ISO language)\\n• d:created_at (Event time), d:ingested_at (Ingest time)"]
        P_CF_S["Column Family 's' (Sentiment):\\n• s:label ('positive' / 'negative' / 'neutral')\\n• s:score (VADER compound score [-1.0, 1.0])"]
        P_CF_N["Column Family 'n' (NLP Features):\\n• n:hashtags (JSON array of #tags)\\n• n:keywords (JSON top 5 keywords)\\n• n:aspects (JSON aspect sentiments)"]
        P_KEY --- P_CF_D --- P_CF_S --- P_CF_N
    end

    subgraph ANALYTICS_TABLE ["HBase Table: analytics"]
        A_KEY["RowKey: daily|[YYYY-MM-DD]|[source]"]
        A_CF["Column Family 'a' (Daily Aggregates):\\n• a:pos (Positive post count)\\n• a:neg (Negative post count)\\n• a:neu (Neutral post count)"]
        A_ROLE["Writer: Spark Batch Job (batch_job.py)\\nReader: FastAPI /api/history"]
        A_KEY --- A_CF --- A_ROLE
    end

    subgraph TOPICS_TABLE ["HBase Table: topics"]
        T_KEY["RowKey: topic_[00..03]"]
        T_CF["Column Family 't' (LDA Clusters):\\n• t:words (Top 6 topic terms JSON)\\n• t:count (Assigned document count)"]
        T_ROLE["Writer: Spark MLlib LDA (batch_job.py)\\nReader: FastAPI /api/topics"]
        T_KEY --- T_CF --- T_ROLE
    end

    SALT_RULE --> R_KEY
    SALT_RULE --> P_KEY
    PROC_TABLE -.->|"Daily Aggregate Scan"| ANALYTICS_TABLE
    PROC_TABLE -.->|"Unsupervised LDA Scan"| TOPICS_TABLE
"""
    }
}

class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DOCS_DIR, **kwargs)
    def log_message(self, format, *args):
        pass

def crop_image(img_path):
    try:
        im = Image.open(img_path)
        bg = Image.new(im.mode, im.size, (255, 255, 255))
        from PIL import ImageChops
        diff = ImageChops.difference(im, bg)
        bbox = diff.getbbox()
        if bbox:
            pad = 25
            padded_bbox = (
                max(0, bbox[0] - pad),
                max(0, bbox[1] - pad),
                min(im.size[0], bbox[2] + pad),
                min(im.size[1], bbox[3] + pad)
            )
            cropped = im.crop(padded_bbox)
            cropped.save(img_path, optimize=True)
            print(f"  Cropped PNG to {cropped.size}")
    except Exception as e:
        print(f"  Crop error: {e}")

def main():
    server = socketserver.TCPServer(("127.0.0.1", PORT), QuietHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    print(f"Server started on 127.0.0.1:{PORT}")

    for name, item in DIAGRAMS.items():
        print(f"\nProcessing: {name} ({item['title']})...")
        html_content = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{item['title']}</title>
  <script src="mermaid.min.js"></script>
  <style>
    body {{
      background-color: #ffffff;
      margin: 0;
      padding: 30px;
      display: inline-block;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }}
  </style>
</head>
<body>
  <div id="diagram" class="mermaid">
{item['code']}
  </div>
  <script>
    mermaid.initialize({{
      startOnLoad: true,
      theme: 'default',
      flowchart: {{ useMaxWidth: false, htmlLabels: true }},
      themeVariables: {{
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        fontSize: '14px',
        primaryColor: '#e1f5fe',
        primaryTextColor: '#01579b',
        primaryBorderColor: '#0288d1',
        lineColor: '#546e7a',
        secondaryColor: '#f3e5f5',
        tertiaryColor: '#e8f5e9'
      }}
    }});
  </script>
</body>
</html>"""
        html_file = os.path.join(DOCS_DIR, f"{name}.html")
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        png_path = os.path.join(DIAGRAMS_DIR, f"{name}.png")
        svg_path = os.path.join(DIAGRAMS_DIR, f"{name}.svg")
        url = f"http://127.0.0.1:{PORT}/{name}.html"

        # 1. Take PNG screenshot
        cmd_png = [
            CHROME_PATH,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            f"--window-size={item['width']},{item['height']}",
            f"--screenshot={png_path}",
            url
        ]
        subprocess.run(cmd_png, check=True)
        if os.path.exists(png_path):
            crop_image(png_path)
            print(f"  PNG: {png_path} ({os.path.getsize(png_path)} bytes)")

        # 2. Extract rendered SVG via Chrome dump-dom
        cmd_svg = [
            CHROME_PATH,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            "--virtual-time-budget=2000",
            "--dump-dom",
            url
        ]
        res = subprocess.run(cmd_svg, capture_output=True, text=True, check=True)
        dom = res.stdout
        svg_match = re.search(r"(<svg[^>]*id=\"mermaid-[^\"]*\"[^>]*>.*?</svg>)", dom, re.DOTALL)
        if not svg_match:
            svg_match = re.search(r"(<svg[^>]*>.*?</svg>)", dom, re.DOTALL)
        if svg_match:
            with open(svg_path, "w", encoding="utf-8") as f:
                f.write(svg_match.group(1))
            print(f"  SVG: {svg_path} ({os.path.getsize(svg_path)} bytes)")
        else:
            print(f"  Warning: SVG tag not found in DOM for {name}")

        # Clean temporary HTML
        if os.path.exists(html_file):
            os.remove(html_file)

    server.shutdown()
    print("\nAll diagram assets (PNG and SVG) generated successfully!")

if __name__ == "__main__":
    main()
