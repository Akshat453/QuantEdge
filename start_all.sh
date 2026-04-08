#!/bin/bash
# =================================================================
# start_all.sh — Launch the entire Real-Time Stock Analytics Pipeline
# =================================================================
# Usage: chmod +x start_all.sh && ./start_all.sh
# Run from: /Users/akshatsingh/Desktop/BDAProject
# =================================================================

set -e  # Exit on any error

PROJECT_DIR="/Users/akshatsingh/Desktop/BDAProject"
VENV_DIR="$PROJECT_DIR/venv"

echo "============================================================"
echo " Real-Time Stock Market Analytics — Pipeline Launcher"
echo "============================================================"
echo ""

# ----- Create required data directories -----
echo "[0/6]  Creating required directories..."
mkdir -p "$PROJECT_DIR/data/logs"
mkdir -p "$PROJECT_DIR/data/raw_ticks"
mkdir -p "$PROJECT_DIR/data/archive"
mkdir -p "$PROJECT_DIR/data/state"
mkdir -p "$PROJECT_DIR/data/processed"
mkdir -p "$PROJECT_DIR/data/checkpoint"
echo "  Done"

# ----- Step 1: Start Docker Compose (Kafka + Zookeeper + Spark) -----
echo ""
echo "[1/6]  Starting Docker containers (Kafka + Zookeeper + Spark)..."
cd "$PROJECT_DIR"
docker-compose up -d

if [ $? -eq 0 ]; then
    echo "  Docker containers started"
else
    echo "  ERROR: Failed to start Docker containers"
    exit 1
fi

# ----- Step 2: Wait for Kafka to be ready -----
echo ""
echo "[2/6]  Waiting for Kafka + Zookeeper to initialize..."
sleep 20

echo "  Polling Kafka until it accepts connections (up to 60s)..."
for i in {1..30}; do
    if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        echo "  Kafka is ready! (after ${i} polls)"
        break
    fi
    echo "  Kafka not ready yet... ($i/30)"
    sleep 2
done

# ----- Step 3: Create Kafka topic with 4 partitions -----
echo ""
echo "[3/6]  Configuring Kafka topic 'stock-data' (4 partitions)..."

# Create topic if it doesn't exist (--if-not-exists makes this safe to re-run)
docker exec kafka kafka-topics --create \
    --topic stock-data \
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    --replication-factor 1 \
    --if-not-exists \
    2>/dev/null && echo "  Topic created (or already exists)" || true

# If topic existed with fewer partitions, increase to 4
# (Kafka only allows increasing partition count, never decreasing)
docker exec kafka kafka-topics --alter \
    --topic stock-data \
    --bootstrap-server localhost:9092 \
    --partitions 4 \
    2>/dev/null || echo "  Topic already has 4+ partitions — skipping alter"

# Confirm final partition count
echo "  Topic status:"
docker exec kafka kafka-topics --describe \
    --topic stock-data \
    --bootstrap-server localhost:9092 \
    2>/dev/null | grep -E "PartitionCount|Topic:" || echo "  (topic not yet visible)"

echo "  Kafka topic ready"

# ----- Step 4: Start Producer (Mac terminal, background) -----
echo ""
echo "[4/6]  Starting Kafka producer (background, 4 worker processes)..."
source "$VENV_DIR/bin/activate"

echo "  Installing/verifying Python dependencies..."
pip install psutil pyarrow kafka-python-ng yfinance streamlit plotly pandas numpy multiprocessing-logging --quiet
echo "  Dependencies verified"

python "$PROJECT_DIR/producer/producer.py" > /tmp/producer.log 2>&1 &
PRODUCER_PID=$!
echo "  Producer started (PID: $PRODUCER_PID)"
echo "     Logs: tail -f /tmp/producer.log"

# ----- Step 5: Start Spark Structured Streaming (inside Docker) -----
echo ""
echo "[5/6]  Starting Spark Structured Streaming (inside container)..."
# IMPORTANT: This change is ADDITIVE ONLY — same --packages string is preserved exactly.
# Only --conf flags have been added. The script path and working dir are unchanged.
docker exec -d spark bash -c "
    export PATH=\$PATH:/opt/spark/bin && \
    cd /opt/spark/work && \
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
        --conf spark.sql.shuffle.partitions=4 \
        --conf spark.streaming.backpressure.enabled=true \
        --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
        spark/spark_stream.py \
        > /opt/spark/work/data/spark.log 2>&1
"
echo "  Spark streaming started (inside 'spark' container)"
echo "     Logs: tail -f data/spark.log"

# ----- Step 6: Start Streamlit Dashboard (Mac terminal, foreground) -----
echo ""
echo "[6/6]  Starting Streamlit dashboard..."
echo ""
echo "============================================================"
echo " ALL SERVICES RUNNING"
echo "============================================================"
echo ""
echo "  Producer PID:    $PRODUCER_PID"
echo "  Spark:           Running inside 'spark' container"
echo "  Dashboard:       http://localhost:8501"
echo "  Spark UI:        http://localhost:4040"
echo ""
echo "  Producer logs:   tail -f /tmp/producer.log"
echo "  Spark logs:      tail -f data/spark.log"
echo "  LB metrics:      tail -f data/logs/lb_metrics.jsonl"
echo "  Spark metrics:   tail -f data/logs/spark_metrics.jsonl"
echo ""
echo "  To stop everything:"
echo "     kill $PRODUCER_PID          # stop producer"
echo "     docker-compose down         # stop all containers"
echo "     Ctrl+C                      # stop dashboard"
echo ""
echo "  To verify pipeline health:"
echo "     ./verify_pipeline.sh"
echo ""
echo "============================================================"
echo " Launching Streamlit dashboard now..."
echo "============================================================"
echo ""

# Runs in foreground — Ctrl+C to stop
streamlit run "$PROJECT_DIR/dashboard/app.py"
