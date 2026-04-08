# Real-Time Stock Market Analytics System

A production-grade Big Data pipeline that fetches live stock data for 47 symbols across 6 sectors, streams through Apache Kafka with load-balanced parallel producers, processes with Apache Spark Structured Streaming (event-time windowing, RSI, VWAP, crash detection), stores in Parquet, and visualizes on a 6-tab auto-refreshing Streamlit dashboard with 3D plots and real-time alerts.

## Architecture

```
┌─────────────────┐   ┌────────────────┐   ┌──────────────────────┐   ┌────────────┐   ┌────────────────┐
│  Yahoo Finance  │──▶│ Kafka Producer │──▶│  Spark Structured    │──▶│  Parquet   │──▶│   Streamlit    │
│  (yfinance)     │   │ 4 workers      │   │  Streaming           │   │  Storage   │   │  Dashboard     │
│  + GBM sim      │   │ Manager.Queue  │   │  • Event-time windows│   │  /processed│   │  6 Tabs        │
│  47 stocks      │   │ topic: 4 parts │   │  • MA5/MA20/RSI14    │   │  /raw_ticks│   │  3D plots      │
│  6 sectors      │   │ port: 9092     │   │  • VWAP, crash score │   │  /state/   │   │  port: 8501    │
└─────────────────┘   └────────────────┘   └──────────────────────┘   └────────────┘   └────────────────┘
      Mac                   Docker               Docker (Spark)          Shared Vol          Mac
   (multiprocess)          (Kafka)                                      (./data/)
```

## Project Structure

```
BDAProject/
├── producer/
│   └── producer.py          # 4-worker multiprocessing producer, GBM simulation
├── spark/
│   └── spark_stream.py      # Spark Streaming: event-time, state store, analytics
├── dashboard/
│   └── app.py               # 6-tab Streamlit dashboard with 3D/scatter/heatmap
├── data/
│   ├── processed/           # Enriched Parquet (partitioned by symbol+date)
│   ├── raw_ticks/           # Raw tick Parquet (Volume V evidence)
│   ├── checkpoint/          # Spark streaming checkpoints
│   ├── state/               # Per-symbol rolling indicator history (JSON)
│   ├── logs/                # lb_metrics.jsonl, spark_metrics.jsonl
│   └── verify_parquet.py    # Pipeline data verification script
├── .streamlit/
│   └── config.toml          # Dark theme configuration
├── docker-compose.yml       # Kafka + Zookeeper + Spark (4040 exposed)
├── requirements.txt         # Python dependencies
├── start_all.sh             # One-command pipeline launcher
├── verify_pipeline.sh       # Health check script
└── README.md                # This file
```

## How to Run

### Prerequisites
- Docker Desktop installed and running
- Python 3.10+ with a virtual environment

### One-Command Launch

```bash
cd /Users/akshatsingh/Desktop/BDAProject
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

chmod +x start_all.sh verify_pipeline.sh
./start_all.sh
```

After ~30 seconds, open **http://localhost:8501**

### Manual Launch (4 terminals)

```bash
# Terminal 1: Docker services
docker-compose up -d
sleep 20
docker exec kafka kafka-topics --create --topic stock-data \
  --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1 --if-not-exists

# Terminal 2: Producer (Mac, 4 parallel workers)
source venv/bin/activate
python producer/producer.py

# Terminal 3: Spark Streaming (inside container)
docker exec -it spark bash -c "
  export PATH=\$PATH:/opt/spark/bin && cd /opt/spark/work &&
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
    spark/spark_stream.py"

# Terminal 4: Dashboard
source venv/bin/activate
streamlit run dashboard/app.py
```

### Verify Pipeline Health

```bash
./verify_pipeline.sh
```

### Stopping Everything

```bash
kill $(cat /tmp/producer.pid 2>/dev/null)   # stop producer
docker-compose down                           # stop containers
# Ctrl+C in dashboard terminal
```

---

## 3Vs Evidence

### Volume — Demonstrating large-scale data ingestion and storage

| Evidence | File | Details |
|---|---|---|
| 47 stocks × every 5s = ~560 msgs/min | [producer/producer.py](producer/producer.py) | `ALL_SYMBOLS` list (lines 60–83) |
| Raw tick archive (unprocessed) | [spark/spark_stream.py](spark/spark_stream.py) | `RAW_TICKS_PATH` write in `process_batch()` |
| Cumulative rows chart | [dashboard/app.py](dashboard/app.py) | Tab 5 — Volume panel, `fig_growth` |
| Total Parquet size metric | [dashboard/app.py](dashboard/app.py) | Tab 1 metric card + Tab 5 |
| Data archived separately | `data/raw_ticks/` + `data/processed/` | Two independent Parquet trees |

### Velocity — Demonstrating real-time, low-latency streaming

| Evidence | File | Details |
|---|---|---|
| Kafka produce timestamp in every msg | [producer/producer.py](producer/producer.py) | `"kafka_produce_time": int(time.time()*1000)` |
| End-to-end latency computed per row | [spark/spark_stream.py](spark/spark_stream.py) | `latency_ms = spark_process_time - kafka_produce_time` |
| Latency histogram on dashboard | [dashboard/app.py](dashboard/app.py) | Tab 5 — Velocity panel, `fig_lat` |
| Throughput gauge (rows/sec) | [dashboard/app.py](dashboard/app.py) | Tab 5 — `go.Indicator` gauge |
| Spark batch duration logged | [spark/spark_stream.py](spark/spark_stream.py) | `log_batch_metrics()` → `data/logs/spark_metrics.jsonl` |
| 10-second micro-batches | [spark/spark_stream.py](spark/spark_stream.py) | `trigger(processingTime="10 seconds")` |
| Event-time watermarking | [spark/spark_stream.py](spark/spark_stream.py) | `.withWatermark("event_time", "5 minutes")` |

### Variety — Demonstrating diverse data types and analytical dimensions

| Evidence | File | Details |
|---|---|---|
| Real vs simulated data field | [producer/producer.py](producer/producer.py) | `"data_type": "real"` or `"simulated"` |
| GBM simulation when market closed | [producer/producer.py](producer/producer.py) | `gbm_next_price()`, `simulate_ohlcv()` |
| 6 data sectors (Tech/Finance/Health…) | [producer/producer.py](producer/producer.py) | `STOCKS` dict, `SECTOR_MAP` |
| 10+ derived analytics columns | [spark/spark_stream.py](spark/spark_stream.py) | MA5, MA20, RSI14, VWAP, volatility, crash_score, latency_ms… |
| Real vs simulated pie chart | [dashboard/app.py](dashboard/app.py) | Tab 5 — Variety panel |
| Schema explorer panel | [dashboard/app.py](dashboard/app.py) | Tab 5 — schema DataFrame |
| 3D multi-dimensional plots | [dashboard/app.py](dashboard/app.py) | Tab 3 — Price×Volume×Time, Risk Cube |
| Sector correlation heatmap | [dashboard/app.py](dashboard/app.py) | Tab 4 — `go.Heatmap` |
| Crash detection with composite score | [spark/spark_stream.py](spark/spark_stream.py) | `crash_score` = price < -2% AND vol surge AND volatility |

---

## Load Balancing

The producer uses `multiprocessing.Manager().Queue()` with 4 parallel worker processes:

```
Worker-0  (AAPL, MSFT, GOOGL, ...)  ─┐
Worker-1  (JPM, BAC, GS, ...)        ─┤──▶  Manager().Queue()  ──▶  KafkaSender  ──▶  Kafka
Worker-2  (JNJ, PFE, XOM, ...)       ─┤
Worker-3  (AMZN, SPY, QQQ, ...)      ─┘
```

- Metrics logged to `data/logs/lb_metrics.jsonl` every cycle
- Queue depth monitored with 1000-message warning threshold
- Dashboard Tab 5 shows per-worker throughput bars and queue depth chart

## Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Apache Spark | 4.0.1 | Structured Streaming, analytics |
| Apache Kafka | 7.5.0 (Confluent) | Message queue, 4 partitions |
| Python | 3.10+ | Producer, dashboard |
| yfinance | ≥0.2.18 | Yahoo Finance live/historical data |
| Streamlit | ≥1.32.0 | Real-time 6-tab dashboard |
| Plotly | ≥5.18.0 | 3D plots, candlestick, heatmaps |
| pandas | ≥2.0.0 | Parquet I/O, analytics |
| psutil | ≥5.9.0 | CPU/memory monitoring |
| Docker | — | Kafka, Zookeeper, Spark containers |
| Parquet | — | Columnar storage, partitioned |

## Author

Akshat Singh — BDA Lab Project
