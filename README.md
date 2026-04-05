# 📈 Real-Time Stock Market Analytics System

A real-time stock market analytics pipeline that fetches live stock data using Yahoo Finance, streams it through Apache Kafka, processes it with Apache Spark Structured Streaming (computing moving averages, price changes, volatility, and volume trends), stores results in Parquet format, and visualizes everything on an auto-refreshing Streamlit dashboard with crash detection alerts.

## 🏗️ Architecture

```
┌──────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌─────────────┐     ┌────────────────┐
│ Yahoo Finance│────▶│   Kafka     │────▶│ Spark Structured │────▶│   Parquet   │────▶│   Streamlit    │
│  (yfinance)  │     │  Producer   │     │   Streaming      │     │   Storage   │     │  Dashboard     │
│              │     │             │     │                  │     │             │     │                │
│ AAPL, GOOGL  │     │ topic:      │     │ • Moving Avg     │     │ Partitioned │     │ • Live Charts  │
│ MSFT, TSLA   │     │ stock-data  │     │ • Price Change % │     │ by symbol   │     │ • Crash Alerts │
│ AMZN         │     │ port: 9092  │     │ • Volatility     │     │ and date    │     │ • Metric Cards │
│              │     │             │     │ • Volume Trend   │     │             │     │ port: 8501     │
└──────────────┘     └─────────────┘     └──────────────────┘     └─────────────┘     └────────────────┘
     Mac                  Docker              Docker                  Shared               Mac
   Terminal             (Kafka)             (Spark)                  Volume              Terminal
```

## 📂 Project Structure

```
BDAProject/
├── producer/
│   └── producer.py          # Kafka producer — fetches stock data
├── spark/
│   └── spark_stream.py      # Spark Structured Streaming job
├── dashboard/
│   └── app.py               # Streamlit dashboard
├── data/
│   ├── processed/           # Parquet output (auto-created)
│   ├── checkpoint/          # Spark checkpoints (auto-created)
│   └── verify_parquet.py    # Parquet verification script
├── docker-compose.yml       # Kafka + Zookeeper + Spark
├── requirements.txt         # Python dependencies
├── start_all.sh             # One-command pipeline launcher
└── README.md                # This file
```

## 🚀 How to Run

### Prerequisites
- Docker Desktop installed and running
- Python 3.x with a virtual environment

### Step-by-Step

```bash
# 1. Navigate to project folder
cd /Users/akshatsingh/Desktop/BDAProject

# 2. Create and activate virtual environment (first time only)
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Option A — Run everything with one command
chmod +x start_all.sh
./start_all.sh

# 3. Option B — Run each component manually

# Terminal 1: Start Docker services
docker-compose up -d
sleep 15

# Create Kafka topic
docker exec kafka kafka-topics --create \
  --topic stock-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# Terminal 2: Start producer (Mac terminal)
source venv/bin/activate
python producer/producer.py

# Terminal 3: Start Spark (inside container)
docker exec -it spark bash
export PATH=$PATH:/opt/spark/bin
cd /opt/spark/work
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  spark/spark_stream.py

# Terminal 4: Start dashboard (Mac terminal)
source venv/bin/activate
streamlit run dashboard/app.py

# 4. Open browser: http://localhost:8501
```

### Stopping Everything

```bash
# Stop dashboard: Ctrl+C in Terminal 4
# Stop producer:  Ctrl+C in Terminal 2
# Stop Spark:     Ctrl+C in Terminal 3, then exit
# Stop Docker:
docker-compose down
```

## 🛠️ Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Apache Spark | 4.0.1 | Structured Streaming engine |
| Apache Kafka | 7.5.0 (Confluent) | Message queue / data pipeline |
| Python | 3.x | Producer, dashboard |
| yfinance | latest | Yahoo Finance data source |
| Streamlit | latest | Real-time dashboard |
| Parquet | — | Columnar storage format |
| Docker | — | Containerization |

## 👤 Author

Akshat Singh — BDA Lab Project
