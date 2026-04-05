"""
=================================================================
Parquet Verification Script — Check Saved Data
=================================================================
What this does:
  1. Reads the Parquet files saved by Spark Streaming
  2. Shows total row count
  3. Shows sample rows for each stock symbol
  4. Shows min/max close price per symbol
  5. Shows the partition structure

Run INSIDE the Spark Docker container:
  spark-submit data/verify_parquet.py

OR on your Mac terminal (with venv activated):
  python data/verify_parquet.py
=================================================================
"""

import os
import sys

# Detect if running inside Docker or on Mac
if os.path.exists("/opt/spark/work/data/processed"):
    PARQUET_PATH = "/opt/spark/work/data/processed"
else:
    PARQUET_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "processed"
    )

# Try using PySpark (runs inside container or if pyspark is installed on Mac)
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, min as spark_min, max as spark_max, count

    spark = SparkSession.builder \
        .appName("ParquetVerification") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print(" PARQUET DATA VERIFICATION")
    print("=" * 60)
    print(f"\n📁 Reading from: {PARQUET_PATH}\n")

    # -------------------------------------------------------
    # Check if Parquet files exist
    # -------------------------------------------------------
    if not os.path.exists(PARQUET_PATH):
        print("[✗] ERROR: No Parquet files found!")
        print("    Make sure spark_stream.py has been running")
        print("    and has processed at least one batch.")
        sys.exit(1)

    # -------------------------------------------------------
    # Read the Parquet files
    # -------------------------------------------------------
    df = spark.read.parquet(PARQUET_PATH)

    # -------------------------------------------------------
    # 1. Total row count
    # -------------------------------------------------------
    total_rows = df.count()
    print(f"📊 Total records: {total_rows}")

    if total_rows == 0:
        print("[!] No data rows found. Let the streaming job run longer.")
        sys.exit(0)

    # -------------------------------------------------------
    # 2. Show schema
    # -------------------------------------------------------
    print("\n📋 Schema:")
    df.printSchema()

    # -------------------------------------------------------
    # 3. Sample rows for each stock symbol
    # -------------------------------------------------------
    symbols = [row["symbol"] for row in df.select("symbol").distinct().collect()]
    print(f"📈 Symbols found: {symbols}\n")

    for symbol in sorted(symbols):
        print(f"--- {symbol} (sample rows) ---")
        df.filter(col("symbol") == symbol) \
          .select("symbol", "open", "close", "high", "low",
                  "volume", "price_change_pct", "volatility",
                  "moving_avg_5", "timestamp") \
          .orderBy(col("timestamp").desc()) \
          .show(5, truncate=False)

    # -------------------------------------------------------
    # 4. Min/Max close price per symbol
    # -------------------------------------------------------
    print("\n📉 Min/Max Close Price per Symbol:")
    df.groupBy("symbol") \
      .agg(
          spark_min("close").alias("min_close"),
          spark_max("close").alias("max_close"),
          count("*").alias("total_records")
      ) \
      .orderBy("symbol") \
      .show(truncate=False)

    # -------------------------------------------------------
    # 5. Show partition structure
    # -------------------------------------------------------
    print("📂 Partition structure (folders on disk):")
    for entry in sorted(os.listdir(PARQUET_PATH)):
        if entry.startswith("symbol="):
            symbol_dir = os.path.join(PARQUET_PATH, entry)
            if os.path.isdir(symbol_dir):
                dates = [d for d in os.listdir(symbol_dir) if d.startswith("date=")]
                print(f"  {entry}/")
                for date_dir in sorted(dates):
                    parquet_count = len([
                        f for f in os.listdir(os.path.join(symbol_dir, date_dir))
                        if f.endswith(".parquet")
                    ])
                    print(f"    {date_dir}/ ({parquet_count} parquet files)")

    print("\n" + "=" * 60)
    print(" ✅ Verification complete!")
    print("=" * 60)

    spark.stop()

except ImportError:
    # Fallback: use pandas if pyspark is not available
    import pandas as pd

    print("=" * 60)
    print(" PARQUET VERIFICATION (using Pandas)")
    print("=" * 60)
    print(f"\n📁 Reading from: {PARQUET_PATH}\n")

    if not os.path.exists(PARQUET_PATH):
        print("[✗] ERROR: No Parquet files found!")
        sys.exit(1)

    df = pd.read_parquet(PARQUET_PATH)
    print(f"📊 Total records: {len(df)}")
    print(f"\n📋 Columns: {list(df.columns)}")
    print(f"\n📈 Symbols: {df['symbol'].unique().tolist()}")

    print("\n📉 Min/Max Close per Symbol:")
    print(df.groupby("symbol")["close"].agg(["min", "max", "count"]))

    print("\n--- Sample rows ---")
    print(df.head(10).to_string())

    print("\n✅ Verification complete!")
