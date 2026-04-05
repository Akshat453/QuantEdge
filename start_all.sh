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
echo " 🚀 Real-Time Stock Market Analytics — Pipeline Launcher"
echo "============================================================"
echo ""

# ----- Step 1: Start Docker Compose (Kafka + Zookeeper + Spark) -----
echo "[1/6] 🐳 Starting Docker containers (Kafka + Zookeeper + Spark)..."
cd "$PROJECT_DIR"
docker-compose up -d

if [ $? -eq 0 ]; then
    echo "  ✅ Docker containers started"
else
    echo "  ❌ Failed to start Docker containers"
    exit 1
fi

# ----- Step 2: Wait for Kafka to be ready -----
echo ""
echo "[2/6] ⏳ Waiting 15 seconds for Kafka to initialize..."
sleep 15
echo "  ✅ Wait complete"

# ----- Step 3: Create Kafka topic -----
echo ""
echo "[3/6] 📝 Creating Kafka topic 'stock-data'..."
docker exec kafka kafka-topics --create \
    --topic stock-data \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    2>/dev/null || echo "  ℹ️  Topic already exists (this is fine)"
echo "  ✅ Kafka topic ready"

# ----- Step 4: Start Producer (Mac terminal, background) -----
echo ""
echo "[4/6] 📡 Starting Kafka producer (background)..."
source "$VENV_DIR/bin/activate"
python "$PROJECT_DIR/producer/producer.py" > /tmp/producer.log 2>&1 &
PRODUCER_PID=$!
echo "  ✅ Producer started (PID: $PRODUCER_PID)"
echo "     Logs: tail -f /tmp/producer.log"

# ----- Step 5: Start Spark Streaming (inside Docker container) -----
echo ""
echo "[5/6] ⚡ Starting Spark Structured Streaming (inside container)..."
docker exec -d spark bash -c "
    export PATH=\$PATH:/opt/spark/bin && \
    cd /opt/spark/work && \
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
        spark/spark_stream.py \
        > /opt/spark/work/data/spark.log 2>&1
"
echo "  ✅ Spark streaming started (inside container)"
echo "     Logs: tail -f data/spark.log"

# ----- Step 6: Start Streamlit Dashboard (Mac terminal) -----
echo ""
echo "[6/6] 📊 Starting Streamlit dashboard..."
echo ""
echo "============================================================"
echo " ✅ ALL SERVICES RUNNING"
echo "============================================================"
echo ""
echo "  📡 Producer PID:    $PRODUCER_PID"
echo "  ⚡ Spark:           Running inside 'spark' container"
echo "  📊 Dashboard:       http://localhost:8501"
echo "  🔍 Spark UI:        http://localhost:4040"
echo ""
echo "  📝 Producer logs:   tail -f /tmp/producer.log"
echo "  📝 Spark logs:      tail -f data/spark.log"
echo ""
echo "  🛑 To stop everything:"
echo "     kill $PRODUCER_PID          # stop producer"
echo "     docker-compose down         # stop all containers"
echo "     Ctrl+C                      # stop dashboard"
echo ""
echo "============================================================"
echo " 🌐 Launching Streamlit dashboard now..."
echo "============================================================"
echo ""

# This runs in the foreground (Ctrl+C to stop)
streamlit run "$PROJECT_DIR/dashboard/app.py"
