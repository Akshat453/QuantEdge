"""
=================================================================
Spark Structured Streaming — Real-Time Stock Analytics (Upgraded)
=================================================================
Key upgrades:
  1. Extended schema: sector, kafka_produce_time, data_type
  2. Event-time processing with watermarking (not processing time)
  3. State persistence: per-symbol history JSON for correct rolling indicators
  4. Full analytics: MA5, MA20, RSI14, VWAP, volume_trend, crash_score, latency
  5. Dual output: raw_ticks (Volume V) + processed (enriched analytics)
  6. Per-batch metrics logged to data/logs/spark_metrics.jsonl

Run INSIDE the Spark Docker container via start_all.sh:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
    spark/spark_stream.py
=================================================================
"""

import json
import os
import time
from datetime import datetime, date

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType, BooleanType
)

# =================================================================
# PATHS — all relative to /opt/spark/work (Docker mount point)
# =================================================================
BASE = "/opt/spark/work"
PROCESSED_PATH = f"{BASE}/data/processed"
RAW_TICKS_PATH = f"{BASE}/data/raw_ticks"
CHECKPOINT_PATH = f"{BASE}/data/checkpoint"
STATE_DIR = f"{BASE}/data/state"
LOG_DIR = f"{BASE}/data/logs"
SPARK_LOG = f"{BASE}/data/spark_metrics.jsonl"

# Create required directories at startup
for d in [PROCESSED_PATH, RAW_TICKS_PATH, STATE_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

# History length to keep per symbol for rolling indicator accuracy
HISTORY_LEN = 50

# =================================================================
# 1. CREATE SPARK SESSION
# =================================================================
spark = SparkSession.builder \
    .appName("StockMarketStreaming") \
    .master("local[*]") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print(" Spark Session created — Stock Market Streaming (Upgraded)")
print("=" * 60)

# =================================================================
# 2. EXTENDED SCHEMA — matches producer.py message format
# =================================================================
# Now includes: sector, kafka_produce_time (for latency), data_type
stock_schema = StructType([
    StructField("symbol",             StringType(),    True),
    StructField("sector",             StringType(),    True),
    StructField("open",               DoubleType(),    True),
    StructField("high",               DoubleType(),    True),
    StructField("low",                DoubleType(),    True),
    StructField("close",              DoubleType(),    True),
    StructField("volume",             LongType(),      True),
    StructField("timestamp",          StringType(),    True),
    StructField("kafka_produce_time", LongType(),      True),
    StructField("data_type",          StringType(),    True),
])

# =================================================================
# 3. READ STREAMING DATA FROM KAFKA
# =================================================================
# kafka:29092 is the internal Docker network address — unchanged from original
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "stock-data") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

print("[✓] Connected to Kafka topic 'stock-data'")

# =================================================================
# 4. PARSE JSON AND APPLY EVENT-TIME WATERMARK
# =================================================================
# Cast timestamp string to TimestampType (event time = actual market time)
# withWatermark: allow up to 5-minute late arrivals before closing windows
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), stock_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType())) \
    .withWatermark("event_time", "5 minutes") \
    .withColumn("date", to_date(col("event_time")))

print("[✓] JSON schema applied | Event-time watermark set to 5 minutes")


# =================================================================
# 5. ANALYTICS HELPERS
# =================================================================

def load_symbol_history(symbol: str) -> pd.DataFrame:
    """
    Load last HISTORY_LEN rows for a symbol from the state JSON file.
    Returns empty DataFrame with correct columns if file not found.
    """
    path = os.path.join(STATE_DIR, f"{symbol}_history.json")
    cols = ["symbol", "sector", "open", "high", "low", "close",
            "volume", "event_time", "kafka_produce_time", "data_type", "date"]
    if os.path.exists(path):
        try:
            df = pd.read_json(path, orient="records")
            if not df.empty and "event_time" in df.columns:
                df["event_time"] = pd.to_datetime(df["event_time"])
                return df[cols] if all(c in df.columns for c in cols) else df
        except Exception:
            pass
    return pd.DataFrame(columns=cols)


def save_symbol_history(symbol: str, df: pd.DataFrame):
    """
    Persist the last HISTORY_LEN rows to the state JSON file.
    Stores event_time as ISO string for JSON compatibility.
    """
    path = os.path.join(STATE_DIR, f"{symbol}_history.json")
    try:
        save_df = df.tail(HISTORY_LEN).copy()
        save_df["event_time"] = save_df["event_time"].astype(str)
        # date column: convert date to str if present
        if "date" in save_df.columns:
            save_df["date"] = save_df["date"].astype(str)
        save_df.to_json(path, orient="records")
    except Exception as e:
        print(f"[!] Could not save history for {symbol}: {e}")


def compute_rsi(close_series: pd.Series, periods: int = 14) -> pd.Series:
    """
    Compute Wilder's RSI for a close price series.
    RSI = 100 - (100 / (1 + avg_gain / avg_loss))
    Returns NaN for rows with insufficient history.
    """
    delta = close_series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # Wilder's smoothed moving average
    avg_gain = gain.ewm(com=periods - 1, min_periods=periods).mean()
    avg_loss = loss.ewm(com=periods - 1, min_periods=periods).mean()

    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.round(2)


def enrich_symbol(symbol_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    Given a DataFrame of rows for one symbol (may be just this batch),
    load historical state, prepend it, compute rolling indicators on the
    combined series, save updated state, then return ONLY the new rows
    with all analytics columns filled in.

    Args:
        symbol_df: New rows from the current micro-batch for this symbol
        symbol: The stock ticker string

    Returns:
        symbol_df rows enriched with all analytics columns
    """
    history = load_symbol_history(symbol)

    # Tag new rows so we can filter them back out after computing on combined data
    symbol_df = symbol_df.copy()
    symbol_df["_is_new"] = True

    # Combine history with new rows, sort by event time
    if not history.empty:
        history = history.copy()
        history["_is_new"] = False
        combined = pd.concat([history, symbol_df], ignore_index=True)
    else:
        combined = symbol_df.copy()

    combined["event_time"] = pd.to_datetime(combined["event_time"])
    combined = combined.sort_values("event_time").reset_index(drop=True)

    # ---- Rolling indicators on the full combined series ----
    close = combined["close"].astype(float)
    volume = combined["volume"].astype(float)

    combined["moving_avg_5"] = close.rolling(5, min_periods=1).mean().round(2)
    combined["moving_avg_20"] = close.rolling(20, min_periods=1).mean().round(2)
    combined["volatility"] = close.rolling(5, min_periods=2).std().round(4)
    combined["rsi_14"] = compute_rsi(close, 14)

    # VWAP: cumulative sum(price * volume) / cumulative sum(volume)
    cum_pv = (close * volume).cumsum()
    cum_v = volume.cumsum()
    combined["vwap"] = (cum_pv / cum_v).round(2)

    # Volume trend: current volume vs 5-period rolling average
    vol_avg = volume.rolling(5, min_periods=1).mean()
    combined["volume_trend"] = (volume / vol_avg.replace(0, float("nan"))).round(3)

    # Price change % within each row (open→close)
    combined["price_change_pct"] = (
        (combined["close"] - combined["open"]) / combined["open"] * 100
    ).round(2)

    # Crash score: price dropped >2%, volume surge >2x avg, volatility > 1.5%
    combined["crash_score"] = (
        (combined["price_change_pct"] < -2.0) &
        (combined["volume_trend"] > 2.0) &
        (combined["volatility"].fillna(0) > 1.5)
    )

    # Latency: time from Kafka produce to Spark processing (ms)
    spark_now_ms = int(time.time() * 1000)
    combined["spark_process_time"] = spark_now_ms
    combined["latency_ms"] = (
        spark_now_ms - combined["kafka_produce_time"].fillna(spark_now_ms)
    ).astype(int)

    # Save full combined history for next batch
    save_symbol_history(symbol, combined)

    # Return ONLY the new rows (those from this batch)
    new_rows = combined[combined["_is_new"] == True].copy()
    new_rows = new_rows.drop(columns=["_is_new"])

    return new_rows


# =================================================================
# 6. LOG BATCH METRICS
# =================================================================

def log_batch_metrics(batch_id: int, batch_df: pd.DataFrame, elapsed_ms: float):
    """
    Log per-batch processing stats to spark_metrics.jsonl.
    Includes batch_id, row count per symbol, and processing duration.
    """
    per_symbol = batch_df.groupby("symbol").size().to_dict()
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "batch_id": batch_id,
        "total_rows": len(batch_df),
        "per_symbol": per_symbol,
        "elapsed_ms": round(elapsed_ms, 1),
    }
    try:
        with open(SPARK_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


# =================================================================
# 7. FOREACHBATCH PROCESSING
# =================================================================

def process_batch(batch_spark_df, batch_id: int):
    """
    Called once per micro-batch. Handles:
      1. Write raw ticks to /data/raw_ticks/ (Volume V demonstration)
      2. Enrich each symbol with rolling analytics using state store
      3. Write enriched data to /data/processed/ (partitioned by symbol+date)
      4. Log batch metrics to spark_metrics.jsonl

    Uses event time from message, not processing time.
    """
    batch_start = time.time()

    if batch_spark_df.isEmpty():
        print(f"[Batch {batch_id}] Empty — no new data")
        return

    # ---- Write raw ticks (unprocessed) for Volume V demonstration ----
    # These are the unmodified records straight from Kafka
    raw_df = batch_spark_df.select(
        "symbol", "sector", "open", "high", "low", "close",
        "volume", "event_time", "kafka_produce_time", "data_type", "date"
    )
    try:
        raw_df.write \
            .mode("append") \
            .partitionBy("date") \
            .parquet(RAW_TICKS_PATH)
    except Exception as e:
        print(f"[!] Raw tick write error (batch {batch_id}): {e}")

    # ---- Convert to pandas for rich analytics ----
    try:
        pdf = batch_spark_df.toPandas()
    except Exception as e:
        print(f"[!] toPandas() failed (batch {batch_id}): {e}")
        return

    if pdf.empty:
        return

    pdf["event_time"] = pd.to_datetime(pdf["event_time"])

    # ---- Enrich each symbol using its state history ----
    enriched_parts = []
    for symbol, sym_df in pdf.groupby("symbol"):
        try:
            enriched = enrich_symbol(sym_df.copy(), str(symbol))
            enriched_parts.append(enriched)
        except Exception as e:
            print(f"[!] Error enriching {symbol} (batch {batch_id}): {e}")

    if not enriched_parts:
        return

    enriched_pdf = pd.concat(enriched_parts, ignore_index=True)

    # Ensure date column is present and correct type for Parquet partitioning
    if "date" not in enriched_pdf.columns or enriched_pdf["date"].isna().all():
        enriched_pdf["date"] = enriched_pdf["event_time"].dt.date

    # ---- Log per-symbol row counts (demonstrates partitioned processing) ----
    elapsed_ms = (time.time() - batch_start) * 1000
    log_batch_metrics(batch_id, enriched_pdf, elapsed_ms)

    count = len(enriched_pdf)
    print(f"\n{'=' * 60}")
    print(f" Batch {batch_id} — {count} records | {elapsed_ms:.0f}ms processing time")
    print(f"{'=' * 60}")

    # Quick sample to console
    sample_cols = [
        "symbol", "close", "price_change_pct", "volatility",
        "moving_avg_5", "rsi_14", "vwap", "crash_score", "data_type"
    ]
    available = [c for c in sample_cols if c in enriched_pdf.columns]
    print(enriched_pdf[available].tail(10).to_string(index=False))

    # ---- Write enriched analytics to Parquet ----
    # Partitioned by symbol + date for efficient dashboard queries
    try:
        enriched_spark = spark.createDataFrame(enriched_pdf)
        enriched_spark.write \
            .mode("append") \
            .partitionBy("symbol", "date") \
            .parquet(PROCESSED_PATH)
        print(f"[✓] Batch {batch_id} → data/processed/ ({count} rows)")
    except Exception as e:
        print(f"[!] Parquet write error (batch {batch_id}): {e}")


# =================================================================
# 8. START STREAMING QUERY
# =================================================================
print("\n[▶] Starting streaming query...")
print(f"[▶] Raw ticks  → {RAW_TICKS_PATH}")
print(f"[▶] Processed  → {PROCESSED_PATH}")
print(f"[▶] State      → {STATE_DIR}")
print(f"[▶] Checkpoint → {CHECKPOINT_PATH}")
print("[▶] Trigger: 10 seconds | Watermark: 5 minutes | Press Ctrl+C to stop\n")

query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
