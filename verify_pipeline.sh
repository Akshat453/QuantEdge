#!/bin/bash
# =================================================================
# verify_pipeline.sh — Health check for the Stock Analytics Pipeline
# =================================================================
# Checks: Docker containers, Kafka topic, Parquet files, dashboard
# Run from project root: ./verify_pipeline.sh
# =================================================================

PROJECT_DIR="/Users/akshatsingh/Desktop/BDAProject"
PASS=0
FAIL=0

green() { echo "  [OK]  $1"; }
red()   { echo "  [!!]  $1"; }
info()  { echo "  [--]  $1"; }

check() {
    # check <description> <condition_command>
    local desc="$1"
    shift
    if "$@" &>/dev/null; then
        green "$desc"
        PASS=$((PASS + 1))
    else
        red "$desc"
        FAIL=$((FAIL + 1))
    fi
}

echo "============================================================"
echo "  Pipeline Health Check — Real-Time Stock Analytics"
echo "  $(date)"
echo "============================================================"

# ── Docker containers ──────────────────────────────────────────
echo ""
echo "[ Docker ]"
check "Zookeeper container running" docker ps --filter "name=zookeeper" --filter "status=running" -q
check "Kafka container running"     docker ps --filter "name=kafka"     --filter "status=running" -q
check "Spark container running"     docker ps --filter "name=spark"     --filter "status=running" -q

# ── Kafka topic ────────────────────────────────────────────────
echo ""
echo "[ Kafka ]"
check "Topic 'stock-data' exists" \
    docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Show partition count
PARTITIONS=$(docker exec kafka kafka-topics --describe \
    --topic stock-data --bootstrap-server localhost:9092 2>/dev/null \
    | grep PartitionCount | awk -F'PartitionCount:' '{print $2}' | awk '{print $1}')
if [ -n "$PARTITIONS" ]; then
    info "Partition count: $PARTITIONS (target: 4)"
else
    info "Topic details not yet available"
fi

# ── Producer ───────────────────────────────────────────────────
echo ""
echo "[ Producer ]"
if [ -f /tmp/producer.log ]; then
    LAST_LINE=$(tail -1 /tmp/producer.log 2>/dev/null)
    if [ -n "$LAST_LINE" ]; then
        green "Producer log exists ($(wc -l < /tmp/producer.log) lines)"
        info  "Last line: $LAST_LINE"
    else
        red "Producer log is empty"
        FAIL=$((FAIL + 1))
    fi
else
    red "Producer log not found at /tmp/producer.log — is producer running?"
    FAIL=$((FAIL + 1))
fi

# Check lb_metrics.jsonl
if [ -f "$PROJECT_DIR/data/logs/lb_metrics.jsonl" ]; then
    LB_LINES=$(wc -l < "$PROJECT_DIR/data/logs/lb_metrics.jsonl")
    green "Load-balancing metrics: $LB_LINES entries"
else
    info "lb_metrics.jsonl not yet created"
fi

# ── Spark ──────────────────────────────────────────────────────
echo ""
echo "[ Spark ]"
if [ -f "$PROJECT_DIR/data/spark.log" ]; then
    SPARK_LINES=$(wc -l < "$PROJECT_DIR/data/spark.log")
    green "Spark log exists ($SPARK_LINES lines)"
    # Check for recent batch activity
    if grep -q "Batch" "$PROJECT_DIR/data/spark.log" 2>/dev/null; then
        LAST_BATCH=$(grep "Batch" "$PROJECT_DIR/data/spark.log" | tail -1)
        info "Last batch: $LAST_BATCH"
    else
        info "No batch processed yet (Spark may still be initializing)"
    fi
else
    red "Spark log not found — is Spark running inside container?"
    FAIL=$((FAIL + 1))
fi

# ── Parquet files ──────────────────────────────────────────────
echo ""
echo "[ Parquet Data ]"
if [ -d "$PROJECT_DIR/data/processed" ] && [ "$(find "$PROJECT_DIR/data/processed" -name '*.parquet' 2>/dev/null | head -1)" ]; then
    PROC_FILES=$(find "$PROJECT_DIR/data/processed" -name '*.parquet' | wc -l)
    green "Processed Parquet files: $PROC_FILES file(s)"
else
    red "No processed Parquet files yet — Spark hasn't written output"
    FAIL=$((FAIL + 1))
fi

if [ -d "$PROJECT_DIR/data/raw_ticks" ] && [ "$(find "$PROJECT_DIR/data/raw_ticks" -name '*.parquet' 2>/dev/null | head -1)" ]; then
    RAW_FILES=$(find "$PROJECT_DIR/data/raw_ticks" -name '*.parquet' | wc -l)
    green "Raw tick Parquet files: $RAW_FILES file(s)"
else
    info "No raw tick Parquet files yet (written alongside processed)"
fi

# State files
STATE_COUNT=$(find "$PROJECT_DIR/data/state" -name '*_history.json' 2>/dev/null | wc -l)
if [ "$STATE_COUNT" -gt 0 ]; then
    green "Symbol state files: $STATE_COUNT (rolling indicator history)"
else
    info "No state files yet (created after first Spark batch)"
fi

# ── Dashboard ──────────────────────────────────────────────────
echo ""
echo "[ Dashboard ]"
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8501 2>/dev/null)
if [ "$HTTP_STATUS" = "200" ]; then
    green "Streamlit dashboard: reachable (HTTP $HTTP_STATUS)"
else
    red "Streamlit dashboard: NOT reachable (HTTP $HTTP_STATUS — is it running?)"
    FAIL=$((FAIL + 1))
fi

# Spark UI
SPARK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:4040 2>/dev/null)
if [ "$SPARK_STATUS" = "200" ] || [ "$SPARK_STATUS" = "302" ]; then
    green "Spark UI: reachable at http://localhost:4040"
else
    info "Spark UI: not yet available (HTTP $SPARK_STATUS — Spark may be initializing)"
fi

# ── Summary ────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  Results: $PASS passed, $FAIL failed"
if [ "$FAIL" -eq 0 ]; then
    echo "  ALL CHECKS PASSED — pipeline is healthy"
else
    echo "  $FAIL CHECK(S) FAILED — see details above"
    echo ""
    echo "  Troubleshooting:"
    echo "    docker-compose up -d         # restart containers"
    echo "    tail -f /tmp/producer.log    # check producer"
    echo "    tail -f data/spark.log       # check Spark"
fi
echo "============================================================"

exit $FAIL
