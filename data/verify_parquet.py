"""
=================================================================
Parquet Verification Utility — Real-Time Stock Analytics
=================================================================
Checks both data/processed/ and data/raw_ticks/ for:
  - Record counts and schema
  - Presence of all analytics columns (rsi_14, vwap, crash_score, etc.)
  - File sizes on disk
  - Sample rows per stock symbol
  - Partition structure (symbol= and date= directories)

Run on Mac (outside Docker):
  python data/verify_parquet.py

Run inside Spark container (uses PySpark):
  cd /opt/spark/work && python data/verify_parquet.py
=================================================================
"""

import os
import sys

# Detect environment and set paths
INSIDE_DOCKER = os.path.exists("/opt/spark/work")
if INSIDE_DOCKER:
    BASE = "/opt/spark/work/data"
else:
    # Running on Mac — resolve relative to this file's parent directory
    BASE = os.path.dirname(os.path.abspath(__file__))

PROCESSED_PATH = os.path.join(BASE, "processed")
RAW_TICKS_PATH = os.path.join(BASE, "raw_ticks")
STATE_PATH = os.path.join(BASE, "state")
LOGS_PATH = os.path.join(BASE, "logs")

# Expected analytics columns from the upgraded Spark job
EXPECTED_ANALYTICS_COLS = [
    "symbol", "sector", "open", "high", "low", "close", "volume",
    "event_time", "data_type", "kafka_produce_time",
    "moving_avg_5", "moving_avg_20", "price_change_pct", "volatility",
    "vwap", "rsi_14", "volume_trend", "crash_score",
    "spark_process_time", "latency_ms",
]


def get_dir_size_mb(path: str) -> float:
    """Recursively sum all .parquet file sizes in a directory."""
    total: int = 0
    if os.path.exists(path):
        for root, _, files in os.walk(path):
            for f in files:
                if f.endswith(".parquet"):
                    try:
                        total = total + int(os.path.getsize(os.path.join(root, f)))
                    except OSError:
                        pass
    return float(total) / (1024.0 * 1024.0)


def list_partitions(path: str) -> list:
    """List first-level partition directories (e.g. symbol=AAPL or date=2026-04-07)."""
    if not os.path.exists(path):
        return []
    return sorted([
        d for d in os.listdir(path)
        if os.path.isdir(os.path.join(path, d))
    ])


def verify_with_pandas(path: str, label: str, expected_cols: list = None):
    """
    Verify a Parquet directory using pandas (no Spark needed).
    Prints schema, record count, unique symbols, file size, and sample.
    """
    print(f"\n{'─' * 60}")
    print(f"  {label}")
    print(f"  Path: {path}")
    print(f"{'─' * 60}")

    if not os.path.exists(path):
        print("  [!] Path does not exist — Spark has not written data here yet.")
        return

    size_mb = get_dir_size_mb(path)
    if size_mb == 0:
        print("  [!] No .parquet files found. Spark streaming may still be starting up.")
        return

    print(f"  Disk size:  {size_mb} MB")

    try:
        import pandas as pd
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"  [x] Could not read Parquet: {e}")
        return

    print(f"  Total rows: {len(df):,}")
    print(f"  Columns:    {len(df.columns)}")
    print(f"\n  Schema:")
    for col_name, dtype in df.dtypes.items():
        print(f"    {col_name:<30} {dtype}")

    if "symbol" in df.columns:
        syms = sorted(df["symbol"].unique())
        print(f"\n  Unique symbols ({len(syms)}): {syms}")

    if "data_type" in df.columns:
        breakdown = df["data_type"].value_counts().to_dict()
        print(f"  Data type breakdown: {breakdown}")

    if expected_cols:
        missing = [c for c in expected_cols if c not in df.columns]
        present = [c for c in expected_cols if c in df.columns]
        print(f"\n  Analytics columns present ({len(present)}/{len(expected_cols)}):")
        for c in expected_cols:
            status = "[ok]" if c in df.columns else "[--]"
            print(f"    {status} {c}")
        if missing:
            print(f"\n  [!] Missing (may appear after more batches): {missing}")

    print(f"\n  Partition structure (top-level dirs):")
    parts = list_partitions(path)
    for p in parts[:10]:
        print(f"    {p}/")
    if len(parts) > 10:
        print(f"    ... and {len(parts) - 10} more")

    if "symbol" in df.columns:
        print(f"\n  Sample rows (last 3 per symbol):")
        sample_cols = [c for c in ["symbol", "close", "price_change_pct", "rsi_14",
                                   "vwap", "crash_score", "data_type", "event_time"]
                       if c in df.columns]
        sort_col = "event_time" if "event_time" in df.columns else df.columns[0]
        sample = df.sort_values(sort_col)
        sample = sample.groupby("symbol", observed=True).tail(3)
        import pandas as pd
        with pd.option_context("display.max_columns", None, "display.width", 140,
                               "display.float_format", "{:.2f}".format):
            print(sample[sample_cols].to_string(index=False))

    if "crash_score" in df.columns:
        crash_count = int(df["crash_score"].sum())
        print(f"\n  Crash signals detected: {crash_count}")

    if "latency_ms" in df.columns:
        avg_lat = df["latency_ms"].mean()
        print(f"  Avg end-to-end latency: {avg_lat:.0f} ms")


def verify_with_spark(path: str, label: str):
    """
    Verify using PySpark — available inside the Docker container.
    Falls back to pandas if PySpark session cannot be created.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("VerifyParquet") \
            .master("local[2]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        print(f"\n{'─' * 60}")
        print(f"  {label} (PySpark mode)")
        print(f"  Path: {path}")
        print(f"{'─' * 60}")

        df = spark.read.parquet(path)
        count = df.count()
        print(f"  Total rows:  {count:,}")
        print(f"  Schema:")
        df.printSchema()
        if "symbol" in [f.name for f in df.schema.fields]:
            print("  Per-symbol counts:")
            df.groupBy("symbol").count().orderBy("symbol").show(50, truncate=False)
        df.show(5, truncate=False)
        spark.stop()
    except Exception:
        verify_with_pandas(path, label, EXPECTED_ANALYTICS_COLS)


def check_logs():
    """Print a summary of the log files."""
    print(f"\n{'─' * 60}")
    print("  Log Files")
    print(f"{'─' * 60}")

    for fname in ["lb_metrics.jsonl", "spark_metrics.jsonl", "producer.log"]:
        fpath = os.path.join(LOGS_PATH, fname)
        if os.path.exists(fpath):
            size = os.path.getsize(fpath)
            try:
                with open(fpath) as f:
                    lines = f.readlines()
                print(f"  {fname}: {len(lines)} entries ({size} bytes)")
            except Exception:
                print(f"  {fname}: {size} bytes")
        else:
            print(f"  {fname}: not found yet")

    # Check per-symbol state files
    if os.path.exists(STATE_PATH):
        state_files = [f for f in os.listdir(STATE_PATH) if f.endswith("_history.json")]
        print(f"\n  Symbol state files: {len(state_files)}")
        if state_files:
            syms = [f.replace("_history.json", "") for f in sorted(state_files)]
            print(f"  Symbols with history: {syms}")
    else:
        print("\n  State directory not found (Spark not yet running).")


# =================================================================
# MAIN
# =================================================================
print("=" * 60)
print("  Parquet Verification — Real-Time Stock Analytics (Upgraded)")
print("=" * 60)
print(f"  Environment: {'Docker (PySpark)' if INSIDE_DOCKER else 'Mac (pandas)'}")

if INSIDE_DOCKER:
    verify_with_spark(PROCESSED_PATH, "PROCESSED DATA (enriched analytics)")
    verify_with_pandas(RAW_TICKS_PATH, "RAW TICKS (Volume V evidence)")
else:
    verify_with_pandas(PROCESSED_PATH, "PROCESSED DATA (enriched analytics)", EXPECTED_ANALYTICS_COLS)
    verify_with_pandas(RAW_TICKS_PATH, "RAW TICKS (Volume V evidence)", None)

check_logs()

print(f"\n{'=' * 60}")
print("  Verification complete")
print(f"{'=' * 60}")
